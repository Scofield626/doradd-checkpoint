#include "garbage_collector.hpp"

#include <algorithm>
#include <cassert>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

// Mock Storage for testing
class MockStore
{
public:
  std::map<std::string, std::string> data;
  std::vector<std::string> deleted_keys;

  bool get(const std::string& key, std::string& value)
  {
    if (data.count(key))
    {
      value = data[key];
      return true;
    }
    return false;
  }

  // Mock batch operations
  struct WriteBatch
  {
    std::map<std::string, std::string> puts;
    std::vector<std::string> deletes;

    void Put(const std::string& key, const std::string& value)
    {
      puts[key] = value;
    }
    void Delete(const std::string& key)
    {
      deletes.push_back(key);
    }
  };

  WriteBatch create_batch()
  {
    return WriteBatch();
  }

  void add_delete_to_batch(WriteBatch& batch, const std::string& key)
  {
    batch.Delete(key);
  }

  void commit_batch(WriteBatch& batch)
  {
    for (const auto& kv : batch.puts)
    {
      data[kv.first] = kv.second;
    }
    for (const auto& k : batch.deletes)
    {
      data.erase(k);
      deleted_keys.push_back(k);
    }
  }

  void scan_keys(
    const std::string& prefix,
    const std::function<void(const std::string& key)>&
      callback)
  {
    auto it = data.lower_bound(prefix);
    while (it != data.end() && it->first.substr(0, prefix.size()) == prefix)
    {
      callback(it->first);
      it++;
    }
  }
};

void test_gc_pruning()
{
  std::cout << "Running test_gc_pruning..." << std::endl;
  MockStore store;
  GarbageCollector<MockStore> gc(store);

  // Setup initial state:
  // Row 1 has versions 1, 2, 3, 4, 5
  store.data["1_v1"] = "val1";
  store.data["1_v2"] = "val2";
  store.data["1_v3"] = "val3";
  store.data["1_v4"] = "val4";
  store.data["1_v5"] = "val5";

  // Current global snapshot is 5.
  // KEEP_VERSIONS is 2.
  // Prune threshold = 5 - 2 = 3.
  // Versions <= 3 are candidates for pruning.
  // We should keep the newest version <= 3, which is v3.
  // So v1 and v2 should be deleted. v3, v4, v5 should remain.

  store.data["global_snapshot"] = "5";

  // Simulate Checkpointer notifying GC about modified keys in snapshots 1, 2, 3
  auto keys1 = std::make_shared<std::vector<uint64_t>>();
  keys1->push_back(1);
  auto keys2 = std::make_shared<std::vector<uint64_t>>();
  keys2->push_back(1);
  auto keys3 = std::make_shared<std::vector<uint64_t>>();
  keys3->push_back(1);

  gc.add_snapshot_keys(1, keys1);
  gc.add_snapshot_keys(2, keys2);
  gc.add_snapshot_keys(3, keys3);

  // Run GC manually
  gc.run_gc();

  // Verify results
  bool v1_deleted =
    std::find(store.deleted_keys.begin(), store.deleted_keys.end(), "1_v1") !=
    store.deleted_keys.end();
  bool v2_deleted =
    std::find(store.deleted_keys.begin(), store.deleted_keys.end(), "1_v2") !=
    store.deleted_keys.end();
  bool v3_deleted =
    std::find(store.deleted_keys.begin(), store.deleted_keys.end(), "1_v3") !=
    store.deleted_keys.end();

  assert(v1_deleted && "v1 should be deleted");
  assert(v2_deleted && "v2 should be deleted");
  assert(
    !v3_deleted && "v3 should NOT be deleted (it's the newest <= threshold)");
  assert(store.data.count("1_v3") && "v3 should still exist");
  assert(store.data.count("1_v4") && "v4 should still exist");
  assert(store.data.count("1_v5") && "v5 should still exist");

  std::cout << "test_gc_pruning PASSED" << std::endl;
}

void test_gc_no_pruning_needed()
{
  std::cout << "Running test_gc_no_pruning_needed..." << std::endl;
  MockStore store;
  GarbageCollector<MockStore> gc(store);

  // Row 2 has versions 4, 5
  store.data["2_v4"] = "val4";
  store.data["2_v5"] = "val5";

  // Global snapshot 5. Prune threshold 3.
  store.data["global_snapshot"] = "5";

  // Snapshot 4 modified key 2
  auto keys4 = std::make_shared<std::vector<uint64_t>>();
  keys4->push_back(2);
  gc.add_snapshot_keys(4, keys4);

  gc.run_gc();

  // Nothing should be deleted because all versions are > threshold (3)
  assert(store.deleted_keys.empty());
  assert(store.data.count("2_v4"));
  assert(store.data.count("2_v5"));

  std::cout << "test_gc_no_pruning_needed PASSED" << std::endl;
}

int main()
{
  test_gc_pruning();
  test_gc_no_pruning_needed();
  return 0;
}
