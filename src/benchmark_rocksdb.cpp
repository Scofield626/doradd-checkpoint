#include "storage/rocksdb.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace std;

void print_usage(const char* prog)
{
  cout << "Usage: " << prog << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  --threads <n>      Number of threads (default: 1)" << endl;
  cout << "  --batch-size <n>   Batch size (default: 100)" << endl;
  cout << "  --value-size <n>   Value size in bytes (default: 100)" << endl;
  cout << "  --num-keys <n>     Total number of keys to write (default: 100000)"
       << endl;
  cout << "  --db-path <path>   Path to RocksDB (default: ./benchmark_db)"
       << endl;
}

string random_string(size_t length)
{
  static const char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";
  string s;
  s.reserve(length);
  for (size_t i = 0; i < length; ++i)
  {
    s += alphanum[rand() % (sizeof(alphanum) - 1)];
  }
  return s;
}

int main(int argc, char** argv)
{
  int num_threads = 1;
  int batch_size = 100;
  int value_size = 1000;
  int num_keys = 1000000;
  string db_path = "./benchmark_db";

  for (int i = 1; i < argc; ++i)
  {
    string arg = argv[i];
    if (arg == "--threads")
    {
      if (i + 1 < argc)
        num_threads = stoi(argv[++i]);
    }
    else if (arg == "--batch-size")
    {
      if (i + 1 < argc)
        batch_size = stoi(argv[++i]);
    }
    else if (arg == "--value-size")
    {
      if (i + 1 < argc)
        value_size = stoi(argv[++i]);
    }
    else if (arg == "--num-keys")
    {
      if (i + 1 < argc)
        num_keys = stoi(argv[++i]);
    }
    else if (arg == "--db-path")
    {
      if (i + 1 < argc)
        db_path = argv[++i];
    }
    else
    {
      print_usage(argv[0]);
      return 1;
    }
  }

  cout << "Benchmark Configuration:" << endl;
  cout << "  Threads: " << num_threads << endl;
  cout << "  Batch Size: " << batch_size << endl;
  cout << "  Value Size: " << value_size << endl;
  cout << "  Num Keys: " << num_keys << endl;
  cout << "  DB Path: " << db_path << endl;

  // Clean up previous DB
  if (filesystem::exists(db_path))
  {
    filesystem::remove_all(db_path);
  }

  RocksDBStore store;
  if (!store.open(db_path))
  {
    cerr << "Failed to open DB" << endl;
    return 1;
  }

  atomic<int> keys_written{0};
  auto start_time = chrono::high_resolution_clock::now();

  vector<thread> threads;
  for (int t = 0; t < num_threads; ++t)
  {
    threads.emplace_back([&, t]() {
      string val = random_string(value_size);
      int keys_per_thread = num_keys / num_threads;

      for (int i = 0; i < keys_per_thread; i += batch_size)
      {
        auto batch = store.create_batch();
        int current_batch_size = min(batch_size, keys_per_thread - i);

        for (int j = 0; j < current_batch_size; ++j)
        {
          string key = to_string(t) + "_" + to_string(i + j);
          store.add_to_batch(batch, key, val);
        }
        store.commit_batch(batch);
        keys_written += current_batch_size;
      }
    });
  }

  for (auto& t : threads)
  {
    t.join();
  }

  auto end_time = chrono::high_resolution_clock::now();
  auto duration =
    chrono::duration_cast<chrono::milliseconds>(end_time - start_time).count();

  store.close();

  double seconds = duration / 1000.0;
  double throughput = keys_written / seconds;
  double mb_per_sec =
    (double(keys_written) * value_size) / (1024 * 1024 * seconds);

  cout << "Results:" << endl;
  cout << "  Time: " << seconds << " s" << endl;
  cout << "  Throughput: " << throughput << " ops/sec" << endl;
  cout << "  Bandwidth: " << mb_per_sec << " MB/sec" << endl;

  return 0;
}
