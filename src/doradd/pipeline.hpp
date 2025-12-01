#pragma once

#include "../storage/rocksdb.hpp"
#include "SPSCQueue.h"
#include "checkpoint_stats.hpp"
#include "checkpointer.hpp"
#include "config.hpp"
#include "dispatcher.hpp"
#include "pin-thread.hpp"
#include "rpc_handler.hpp"
#include "txcounter.hpp"

#include <flushscheduler.h>
#include <thread>
#include <unordered_map>

std::unordered_map<std::thread::id, uint64_t*>* counter_map;
std::unordered_map<std::thread::id, log_arr_type*>* log_map;
std::mutex* counter_map_mutex;

// Global benchmark start time
ts_type benchmark_start_time;

// Global flush scheduler state
std::thread* flush_scheduler_thread = nullptr;
std::atomic<bool> flush_scheduler_running{false};
int flush_worker_count = 2; // Configurable via command-line
std::vector<int> flush_affinity; // Configurable via command-line

template<typename T>
void build_pipelines(
  int worker_cnt,
  char* log_name,
  char* gen_type,
  int argc = 0,
  char** argv = nullptr)
{
  // Parse flush scheduler arguments
  if (argc > 0 && argv != nullptr)
  {
    for (int i = 1; i < argc; ++i)
    {
      if (std::string(argv[i]) == "--flush-workers" && i + 1 < argc)
      {
        flush_worker_count = std::stoi(argv[++i]);
      }
      else if (std::string(argv[i]) == "--flush-affinity" && i + 1 < argc)
      {
        std::string affinity_str = argv[++i];
        size_t pos = 0;
        while ((pos = affinity_str.find(',')) != std::string::npos)
        {
          flush_affinity.push_back(std::stoi(affinity_str.substr(0, pos)));
          affinity_str.erase(0, pos + 1);
        }
        flush_affinity.push_back(std::stoi(affinity_str));
      }
    }
  }

  // init verona-rt scheduler
  auto& sched = Scheduler::get();
  sched.init(worker_cnt + 1);
  when() << []() { std::cout << "Hello deterministic world!\n"; };

  // init flush scheduler for checkpoint I/O
  auto& flush_sched = verona::rt::FlushScheduler::get();
  flush_sched.init(flush_worker_count);

  // Set affinity for flush scheduler threads if specified
  if (!flush_affinity.empty())
  {
    auto* core = flush_sched.first_core();
    for (int i = 0; i < flush_worker_count; ++i)
    {
      // If we have fewer affinity values than workers, wrap around or reuse the
      // last one? Let's wrap around for now.
      core->affinity = flush_affinity[i % flush_affinity.size()];
      core = core->next;
      std::cout << "Flush scheduler affinity set to "
                << flush_affinity[i % flush_affinity.size()] << std::endl;
    }
  }

  // Schedule a task to add an external event source to keep the flush scheduler
  // alive
  verona::rt::schedule_flush_lambda(
    []() { verona::rt::FlushScheduler::add_external_event_source(); });

  // Start flush scheduler in a separate thread
  flush_scheduler_running.store(true, std::memory_order_relaxed);
  flush_scheduler_thread =
    new std::thread([&flush_sched]() { flush_sched.run(); });

  // init stats collectors for workers
  counter_map = new std::unordered_map<std::thread::id, uint64_t*>();
  counter_map->reserve(worker_cnt);
  log_map = new std::unordered_map<std::thread::id, log_arr_type*>();
  log_map->reserve(worker_cnt);
  counter_map_mutex = new std::mutex();

  // Create storage instance and checkpointer
  auto* checkpointer = new Checkpointer<RocksDBStore, T, typename T::RowType>();

  // Pass command line arguments to the checkpointer if available
  if (argc > 0 && argv != nullptr)
  {
    checkpointer->parse_args(argc, argv);
    CheckpointStats::parse_args(argc, argv);
  }

  // init and run dispatcher pipelines
  when() << [&]() {
    printf("Init and Run - Dispatcher Pipelines\n");

    // Initialize benchmark start time
    benchmark_start_time = std::chrono::system_clock::now();

    // sched.add_external_event_source();

    std::atomic<uint64_t> req_cnt(0);

    // Init RPC handler
#ifdef RPC_LATENCY
    uint8_t* log_arr = static_cast<uint8_t*>(
      aligned_alloc_hpage(RPC_LOG_SIZE * sizeof(ts_type)));

    uint64_t log_arr_addr = (uint64_t)log_arr;

    std::string res_log_dir = "./results/";
    std::string res_log_suffix = "-latency.log";
    std::string res_log_name = res_log_dir + gen_type + res_log_suffix;
    FILE* res_log_fd =
      fopen(reinterpret_cast<const char*>(res_log_name.c_str()), "w");

    RPCHandler rpc_handler(&req_cnt, gen_type, log_arr_addr);
#else
    RPCHandler rpc_handler(&req_cnt, gen_type);
#endif // RPC_LATENCY

    // Map txn logs into memory
    int fd = open(log_name, O_RDONLY);
    if (fd == -1)
    {
      printf("File not existed\n");
      exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);
    void* ret = reinterpret_cast<char*>(mmap(
      nullptr,
      sb.st_size,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_POPULATE,
      fd,
      0));

    // Init dispatcher, prefetcher, and spawner
#ifndef CORE_PIPE
    FileDispatcher<T> dispatcher(
      ret,
      worker_cnt,
      counter_map,
      counter_map_mutex,
      &req_cnt
#  ifdef RPC_LATENCY
      ,
      log_arr_addr
#  endif
    );

    std::thread extern_thrd([&]() mutable {
      pin_thread(2);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      dispatcher.run();
    });
#else

#  ifdef INDEXER
    rigtorp::SPSCQueue<int> ring_idx_pref(CHANNEL_SIZE_IDX_PREF);
    Indexer<T> indexer(ret, &ring_idx_pref, &req_cnt, checkpointer);
#  endif

    rigtorp::SPSCQueue<int> ring_pref_disp(CHANNEL_SIZE);

#  if defined(INDEXER)
    Prefetcher<T> prefetcher(ret, &ring_pref_disp, &ring_idx_pref);
#    ifdef RPC_LATENCY
    // give init_time_log_arr to spawner. Needed for capturing in when.
    Spawner<T> spawner(
      ret,
      worker_cnt,
      counter_map,
      counter_map_mutex,
      &ring_pref_disp,
      checkpointer,
      log_arr_addr,
      res_log_fd);
#    else
    Spawner<T> spawner(
      ret,
      worker_cnt,
      counter_map,
      counter_map_mutex,
      &ring_pref_disp,
      checkpointer);
#    endif // RPC_LATENCY
#  else
    Prefetcher<T> prefetcher(ret, &ring_pref_disp);
    Spawner<T> spawner(
      ret, worker_cnt, counter_map, counter_map_mutex, &ring_pref_disp);
#  endif // INDEXER

    std::thread spawner_thread([&]() mutable {
      pin_thread(0);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      spawner.run();
    });
    std::thread prefetcher_thread([&]() mutable {
      pin_thread(2);
      std::this_thread::sleep_for(std::chrono::seconds(2));
      prefetcher.run();
    });
#endif

#ifdef INDEXER
    std::thread indexer_thread([&]() mutable {
      pin_thread(4);
      std::this_thread::sleep_for(std::chrono::seconds(4));
      indexer.run();
    });
#endif

    std::thread rpc_handler_thread([&]() mutable {
      pin_thread(6);
      std::this_thread::sleep_for(std::chrono::seconds(6));
      rpc_handler.run();
    });

    // flush latency logs
    std::this_thread::sleep_for(std::chrono::seconds(300));

#ifdef CORE_PIPE
    pthread_cancel(spawner_thread.native_handle());
    pthread_cancel(prefetcher_thread.native_handle());
#  ifdef INDEXER
    pthread_cancel(indexer_thread.native_handle());
#  endif
#else
    pthread_cancel(extern_thrd.native_handle());
#endif // CORE_PIPE

    pthread_cancel(rpc_handler_thread.native_handle());

    // Print checkpoint statistics before continuing
    printf("Printing Checkpoint Statistics\n");
    CheckpointStats::print_stats();

#ifdef LOG_LATENCY
    printf("flush latency stats\n");

    // Create results directory if it doesn't exist
    std::filesystem::create_directories("results");

    // Write raw data to the specified file
    CheckpointStats::write_raw_data("results/checkpoint_latency.csv");

    for (const auto& entry : *log_map)
    {
      if (entry.second)
      {
#  ifdef LOG_SCHED_OHEAD
        for (std::tuple<uint32_t, uint32_t> value_tuple : *(entry.second))
          fprintf(
            res_log_fd,
            "%u %u\n",
            std::get<0>(value_tuple),
            std::get<1>(value_tuple));
#  else
        for (auto value : *(entry.second))
          fprintf(res_log_fd, "%lu\n", value);
#  endif // LOG_SCHED_OHEAD
      }
    }
#endif

    // sched.remove_external_event_source();
  };

  sched.run();

  // Shutdown flush scheduler after main scheduler completes
  if (flush_scheduler_thread != nullptr)
  {
    printf("Shutting down flush scheduler...\n");

    // Schedule a task to remove the external event source to allow shutdown
    verona::rt::schedule_flush_lambda(
      []() { verona::rt::FlushScheduler::remove_external_event_source(); });

    flush_scheduler_running.store(false, std::memory_order_relaxed);

    // Stop the flush scheduler threads
    // The threads will exit when they detect no more work

    // Wait for the flush scheduler thread to complete
    flush_scheduler_thread->join();
    delete flush_scheduler_thread;
    flush_scheduler_thread = nullptr;
    printf("Flush scheduler shut down\n");
  }
}
