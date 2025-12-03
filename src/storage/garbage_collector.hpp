#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <unordered_set>

template<typename StorageType>
class GarbageCollector
{
public:
  static constexpr int GC_INTERVAL_SECONDS = 5; // Run GC every 5 seconds
  static constexpr int KEEP_VERSIONS = 2; // Keep K-2 versions
  static constexpr const char* GLOBAL_SNAPSHOT_KEY = "global_snapshot";

  GarbageCollector(StorageType& store) : storage(store)
  {
  }

  ~GarbageCollector() = default;

  void add_snapshot_keys(
    uint64_t snapshot_id, std::shared_ptr<std::vector<uint64_t>> keys)
  {
    std::lock_guard<std::mutex> lock(history_mutex);
    snapshot_history.emplace_back(snapshot_id, std::move(keys));
  }

  void run_gc()
  {
    // Read the current global snapshot
    std::string snap_str;
    if (!storage.get(GLOBAL_SNAPSHOT_KEY, snap_str))
    {
      // It's possible global_snapshot isn't written yet
      return;
    }

    uint64_t current_snapshot = std::stoull(snap_str);
    if (current_snapshot <= KEEP_VERSIONS)
    {
      return;
    }
    uint64_t prune_threshold = current_snapshot - KEEP_VERSIONS;

    std::vector<std::pair<uint64_t, std::shared_ptr<std::vector<uint64_t>>>>
      snapshots_to_process;

    {
      std::lock_guard<std::mutex> lock(history_mutex);
      while (!snapshot_history.empty() &&
             snapshot_history.front().first <= prune_threshold)
      {
        snapshots_to_process.push_back(snapshot_history.front());
        snapshot_history.pop_front();
      }
    }

    if (snapshots_to_process.empty())
    {
      return;
    }

    auto batch = storage.create_batch();
    size_t deleted_count = 0;

    std::unordered_set<uint64_t> unique_keys;
    for (const auto& [snap_id, keys] : snapshots_to_process)
    {
      unique_keys.insert(keys->begin(), keys->end());
    }

    std::vector<uint64_t> sorted_keys(unique_keys.begin(), unique_keys.end());
    std::sort(sorted_keys.begin(), sorted_keys.end());

    for (uint64_t row_id : sorted_keys)
    {
      // For each dirty key in this old snapshot, we need to check if we have
      // too many versions We scan specifically for this row_id
      std::string prefix = std::to_string(row_id) + "_v";

      std::vector<std::pair<uint64_t, std::string>> versions;

      storage.scan_keys(prefix, [&](const std::string& key) {
        auto pos = key.rfind("_v");
        if (pos != std::string::npos)
        {
          uint64_t version_id = std::stoull(key.substr(pos + 2));
          versions.emplace_back(version_id, key);
        }
      });

      if (versions.empty())
        continue;

      // Sort versions ascending
      std::sort(versions.begin(), versions.end());

      // We want to keep the newest version that is <= prune_threshold.
      // Any version strictly older than that one can be deleted.
      // Versions > prune_threshold are kept (they are live or future relative
      // to prune point).

      // Find the version that is the "active" one at prune_threshold
      // It's the largest version_id <= prune_threshold
      int keep_idx = -1;
      for (size_t i = versions.size(); i-- > 0;)
      {
        if (versions[i].first <= prune_threshold)
        {
          keep_idx = static_cast<int>(i);
          break;
        }
      }

      // If we found a version to keep, delete everything older
      if (keep_idx != -1)
      {
        for (size_t i = 0; i < static_cast<size_t>(keep_idx); ++i)
        {
          storage.add_delete_to_batch(batch, versions[i].second);
          deleted_count++;
        }
      }
      else
      {
        // All versions are > prune_threshold, nothing to delete from older
        // history (or all versions are old but we didn't find one <=
        // threshold which shouldn't happen if we are processing a snapshot <=
        // threshold that wrote this key)
      }
    }

    if (deleted_count > 0)
    {
      storage.commit_batch(batch);
      std::cout << "GC completed: pruned " << deleted_count
                << " old versions up to snapshot " << prune_threshold
                << std::endl;
    }
  }

  StorageType& storage;

  std::mutex history_mutex;
  std::deque<std::pair<uint64_t, std::shared_ptr<std::vector<uint64_t>>>>
    snapshot_history;
};
