#pragma once

#include "../storage/garbage_collector.hpp"
#include "SPSCQueue.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cpp/when.h>
#include <cstdint>
#include "flushscheduler.hpp"
#include "gcscheduler.hpp"
#include <latch>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace verona::cpp;

#ifndef CHECKPOINT_BATCH_SIZE
#  error "You must define CHECKPOINT_BATCH_SIZE"
#endif

#ifndef CHECKPOINT_THRESHOLD
#  error "You must define CHECKPOINT_THRESHOLD"
#endif

#ifndef CHECKPOINT_DB_PATH
#  error "You must define CHECKPOINT_DB_PATH"
#endif

constexpr size_t BatchSize = CHECKPOINT_BATCH_SIZE;
constexpr const char* DefaultDBPath = CHECKPOINT_DB_PATH;

namespace batch_helpers
{
  template<typename T, typename Tuple, typename F, size_t... Is>
  void apply_when_impl(Tuple&& t, F&& f, std::index_sequence<Is...>)
  {
    when(std::get<Is>(t)...)
      << [func = std::forward<F>(f)](auto&&... acquired) {
           T* arr[] = {&static_cast<T&>(acquired)...};
           func(arr, sizeof...(acquired));
         };
  }

  template<typename T, typename Tuple, typename F>
  void apply_when(Tuple&& t, F&& f)
  {
    constexpr size_t N = std::tuple_size_v<std::remove_reference_t<Tuple>>;
    apply_when_impl<T>(
      std::forward<Tuple>(t),
      std::forward<F>(f),
      std::make_index_sequence<N>());
  }

  template<size_t N, typename T>
  auto cowns_to_tuple(const std::vector<cown_ptr<T>>& vc, size_t start)
  {
    std::array<cown_ptr<T>, N> arr{};
    for (size_t i = 0; i < N && start + i < vc.size(); ++i)
      arr[i] = vc[start + i];
    return std::apply(
      [](auto&&... elems) { return std::make_tuple(elems...); }, arr);
  }

  template<size_t N, typename T, typename F>
  void process_n_cowns(
    const std::vector<cown_ptr<T>>& cowns,
    const std::vector<uint64_t>& keys,
    size_t start,
    F&& f)
  {
    size_t remain = std::min(N, cowns.size() - start);
    if (remain == N)
    {
      auto tup = cowns_to_tuple<N>(cowns, start);
      auto lam = [&, idx = start, func = std::forward<F>(f)](T** objs, size_t) {
        func(&keys[idx], objs, N);
      };
      apply_when<T>(std::move(tup), std::move(lam));
    }
    else
    {
      for (size_t i = 0; i < remain; ++i)
      {
        when(cowns[start + i])
          << [&, idx = start + i, func = std::forward<F>(f)](auto&& a) {
               T* obj = &static_cast<T&>(a);
               func(&keys[idx], &obj, 1);
             };
      }
    }
  }
} // namespace batch_helpers

template<typename T>
struct Index;

template<typename StorageType, typename TxnType, typename RowType = TxnType>
class Checkpointer
{
public:
  static constexpr int CHECKPOINT_MARKER = -1;

  Checkpointer(const std::string& path = DefaultDBPath)
  {
    if (!storage.open(path))
      throw std::runtime_error("Failed to open DB");

    // Initialize garbage collector
    gc = std::make_unique<GarbageCollector<StorageType>>(storage);
  }

  ~Checkpointer()
  {
    std::lock_guard<std::mutex> lg(completion_mu);
    if (completion_thread.joinable())
      completion_thread.join();
  }

  void set_index(Index<RowType>* idx)
  {
    if (!index)
      index = idx;
  }

  void increment_tx_count(int count)
  {
    tx_count_since_last_checkpoint.fetch_add(count, std::memory_order_relaxed);
    total_transactions.fetch_add(count, std::memory_order_relaxed);
  }

  [[nodiscard]] bool should_checkpoint() const
  {
    return tx_count_since_last_checkpoint.load(std::memory_order_relaxed) >=
      tx_count_threshold;
  }

  void schedule_checkpoint(
    rigtorp::SPSCQueue<int>* ring, std::vector<uint64_t>&& dirty_keys)
  {
    if (!checkpoint_in_flight.exchange(true, std::memory_order_acq_rel))
    {
      ring->push(CHECKPOINT_MARKER);
      int prev = current_diff_idx.exchange(
        1 - current_diff_idx.load(std::memory_order_relaxed),
        std::memory_order_relaxed);
      diffs[prev].swap(dirty_keys);

      tx_count_since_last_checkpoint.store(0, std::memory_order_relaxed);
    }
  }

  void process_checkpoint_request(rigtorp::SPSCQueue<int>* ring)
  {
    std::cout << "Processing checkpoint request" << std::endl;

    // Record checkpoint start time
    auto checkpoint_start = std::chrono::steady_clock::now();

    // 1) Wait for any previous checkpoint to finish
    {
      std::lock_guard<std::mutex> lg(completion_mu);
      if (completion_thread.joinable())
        completion_thread.join();
    }

    // 2) Pop the marker and clear the in‐flight flag
    ring->pop();
    checkpoint_in_flight.store(false, std::memory_order_relaxed);

    // 3) Grab the current dirty‐keys list and reset it
    int idx = 1 - current_diff_idx.load(std::memory_order_relaxed);
    auto keys_ptr =
      std::make_shared<std::vector<uint64_t>>(std::move(diffs[idx]));
    diffs[idx].clear();

    // Record number of objects to checkpoint
    size_t num_objects = keys_ptr->size();

    // 4) Collect the corresponding cowns
    std::vector<cown_ptr<RowType>> cows;
    cows.reserve(keys_ptr->size());
    for (uint64_t k : *keys_ptr)
    {
      if (auto p = index->get_row_addr(k))
        cows.push_back(*p);
    }

    // 5) Calculate number of batches and create a latch
    size_t num_batches = cows.size() / BatchSize + cows.size() % BatchSize;
    auto latch = std::make_shared<std::latch>(num_batches);

    // 6) Allocate a new snapshot ID for this checkpoint
    uint64_t snap =
      current_snapshot.fetch_add(1, std::memory_order_relaxed) + 1;

    // 7) Define the per‐batch write operation
    auto op = [this, latch, keys_ptr, snap](
                const uint64_t* key_ptr, RowType** items, size_t cnt) {
      std::vector<std::pair<std::string, std::string>> writes;
      writes.reserve(cnt);
      for (size_t i = 0; i < cnt; ++i)
      {
        RowType& obj = *items[i];
        // serialize the row
        std::string data(reinterpret_cast<const char*>(&obj), sizeof(RowType));
        // write under "<row_id>_v<version_id>"
        std::string versioned_key =
          std::to_string(key_ptr[i]) + "_v" + std::to_string(snap);
        writes.emplace_back(std::move(versioned_key), std::move(data));
      }

      // Schedule the flush work on the appropriate scheduler
      if (use_flush_scheduler)
      {
        verona::rt::schedule_flush_lambda(
          [this, latch, writes = std::move(writes)]() {
            auto batch = storage.create_batch();
            for (const auto& kv : writes)
            {
              storage.add_to_batch(batch, kv.first, kv.second);
            }
            storage.commit_batch(batch);
            latch->count_down();
          });
      }
      else
      {
        when() << [this, latch, writes = std::move(writes)]() {
          auto batch = storage.create_batch();
          for (const auto& kv : writes)
          {
            storage.add_to_batch(batch, kv.first, kv.second);
          }
          storage.commit_batch(batch);
          latch->count_down();
        };
      }
    };

    // 8) Dispatch all the batches
    for (size_t i = 0; i < cows.size(); i += BatchSize)
    {
      batch_helpers::process_n_cowns<BatchSize>(cows, *keys_ptr, i, op);
    }

    // 9) Once every batch has finished, write the global snapshot pointer and
    // total_txns
    {
      std::lock_guard<std::mutex> lg(completion_mu);
      completion_thread = std::thread(
        [this, snap, latch, keys_ptr, checkpoint_start, num_objects]() mutable {
          latch->wait();
          auto batch = storage.create_batch();
          // bump the global snapshot in the DB
          storage.add_to_batch(
            batch, GLOBAL_SNAPSHOT_KEY, std::to_string(snap));
          // persist the total‐transactions counter as before
          storage.add_to_batch(
            batch,
            "total_txns",
            std::to_string(total_transactions.load(std::memory_order_relaxed)));
          storage.commit_batch(batch);
          storage.flush();

          number_of_checkpoints_done.fetch_add(1, std::memory_order_relaxed);

          // Calculate checkpoint metrics
          auto checkpoint_end = std::chrono::steady_clock::now();
          std::chrono::duration<double> duration = checkpoint_end - checkpoint_start;
          double throughput = num_objects / duration.count();

          std::cout << "Checkpoint " << snap << " completed\n";
          std::cout << "Checkpoint Performance: "
                    << num_objects << " objects in "
                    << duration.count() << " seconds ("
                    << throughput << " objects/sec)\n";

          // Notify GC about the modified keys in this snapshot
          gc->add_snapshot_keys(snap, std::move(keys_ptr));

          // Schedule GC to run on the dedicated GC scheduler
          verona::rt::schedule_gc_lambda([this]() {
            gc->run_gc();
          });
        });
    }
  }

  size_t try_recovery()
  {
    // 1) Recover the total‐transactions counter
    std::string txs_str;
    if (storage.get("total_txns", txs_str))
    {
      uint64_t txs = std::stoull(txs_str);
      total_transactions.store(txs, std::memory_order_relaxed);
      std::cout << "Recovered total_transactions = " << txs << "\n";
    }
    else
    {
      std::cout << "No total_txns key found; starting from zero\n";
    }

    // 2) Load the last fully‐committed snapshot ID
    uint64_t valid_snap = 0;
    std::string snap_str;
    if (storage.get(GLOBAL_SNAPSHOT_KEY, snap_str))
    {
      valid_snap = std::stoull(snap_str);
    }
    std::cout << "Recovering using snapshot " << valid_snap << "\n";

    // 3) Scan all "<row_id>_v<version_id>" entries and pick, for each row_id,
    // the highest version_id ≤ valid_snap
    if (index)
    {
      std::unordered_map<uint64_t, std::pair<uint64_t, std::string>> best;
      for (auto& kv : storage.scan_prefix(""))
      {
        const std::string& key = kv.first;
        // skip our special keys
        if (key == "total_txns" || key == GLOBAL_SNAPSHOT_KEY)
          continue;
        auto pos = key.rfind("_v");
        if (pos == std::string::npos)
          continue;

        uint64_t row_id = std::stoull(key.substr(0, pos));
        uint64_t version_id = std::stoull(key.substr(pos + 2));
        if (version_id > valid_snap)
          continue;

        auto it = best.find(row_id);
        if (it == best.end() || it->second.first < version_id)
        {
          best[row_id] = {version_id, kv.second};
        }
      }

      // 4) Deserialize and reinsert into the in-memory index
      uint64_t max_seen = 0;
      for (auto& [id, entry] : best)
      {
        const std::string& data = entry.second;
        if (data.size() < sizeof(RowType))
        {
          std::cerr << "Corrupted row data for id=" << id << "\n";
          continue;
        }
        RowType obj;
        std::memcpy(&obj, data.data(), sizeof(RowType));
        auto cp = make_cown<RowType>(std::move(obj));
        *index->get_row_addr(id) = std::move(cp);
        max_seen = std::max(max_seen, id + 1);
      }
      index->set_count(max_seen);
      std::cout << "Rebuilt index; highest key = "
                << (max_seen ? max_seen - 1 : 0) << "\n";
    }

    // 5) Return how many transactions we recovered
    return total_transactions.load(std::memory_order_relaxed);
  }

  [[nodiscard]] size_t get_total_checkpoints() const
  {
    return number_of_checkpoints_done.load(std::memory_order_relaxed);
  }

  [[nodiscard]] size_t get_total_transactions() const
  {
    return total_transactions.load(std::memory_order_relaxed);
  }

  void parse_args(int argc, char* argv[])
  {
    for (int i = 1; i < argc; ++i)
    {
      if (std::string(argv[i]) == "--txn-threshold" && i + 1 < argc)
      {
        try
        {
          tx_count_threshold = std::stoul(argv[++i]);
        }
        catch (...)
        {
          fprintf(stderr, "Invalid threshold: %s\n", argv[i]);
        }
      }
      else if (std::string(argv[i]) == "--use-main-scheduler")
      {
        use_flush_scheduler = false;
      }
      else if (std::string(argv[i]) == "--use-flush-scheduler")
      {
        use_flush_scheduler = true;
      }
    }
  }

private:
  StorageType storage;
  Index<RowType>* index = nullptr;
  std::atomic<bool> checkpoint_in_flight{false};
  std::atomic<int> current_diff_idx{0};
  std::array<std::vector<uint64_t>, 2> diffs;
  std::thread completion_thread;
  std::mutex completion_mu;

  size_t tx_count_threshold{CHECKPOINT_THRESHOLD};
  std::atomic<size_t> tx_count_since_last_checkpoint{0};
  std::atomic<size_t> total_transactions{0};
  using clock = std::chrono::steady_clock;
  std::atomic<size_t> number_of_checkpoints_done{0};

  std::atomic<uint64_t> current_snapshot{0}; // Store the current snapshot ID
  static constexpr const char* GLOBAL_SNAPSHOT_KEY = "global_snapshot";
  std::unique_ptr<GarbageCollector<StorageType>> gc;

  // Flag to control whether to use flush scheduler (default: true)
  bool use_flush_scheduler = true;
};
