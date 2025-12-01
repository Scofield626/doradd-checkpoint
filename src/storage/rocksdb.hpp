#ifndef ROCKSDB_STORE_HPP
#define ROCKSDB_STORE_HPP

#include <iostream>
#include <memory>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>
#include <string>
#include <vector>

class RocksDBStore
{
private:
  rocksdb::DB* db_;
  rocksdb::Options options_;

public:
  RocksDBStore() : db_(nullptr)
  {
    options_.create_if_missing = true;
    options_.error_if_exists = false;
    options_.compression = rocksdb::kNoCompression;
    options_.max_background_jobs = 4;
  }

  ~RocksDBStore()
  {
    close();
  }

  bool open(const std::string& db_path)
  {
    rocksdb::Status status = rocksdb::DB::Open(options_, db_path, &db_);
    if (!status.ok())
    {
      std::cerr << "RocksDB: Failed to open DB: " << status.ToString()
                << std::endl;
      return false;
    }
    return true;
  }

  void close()
  {
    if (db_)
    {
      delete db_;
      db_ = nullptr;
    }
  }

  bool put(const std::string& key, const std::string& value)
  {
    if (!db_)
    {
      std::cerr << "RocksDB: DB not open." << std::endl;
      return false;
    }
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok())
    {
      std::cerr << "RocksDB: Failed to put: " << status.ToString() << std::endl;
      return false;
    }
    return true;
  }

  bool get(const std::string& key, std::string& value)
  {
    if (!db_)
    {
      std::cerr << "RocksDB: DB not open." << std::endl;
      return false;
    }
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
    if (!status.ok())
    {
      if (status.IsNotFound())
      {
        value.clear();
        return false;
      }
      std::cerr << "RocksDB: Failed to get: " << status.ToString() << std::endl;
      return false;
    }
    return true;
  }

  bool
  batch_put(const std::vector<std::pair<std::string, std::string>>& entries)
  {
    if (!db_)
    {
      std::cerr << "RocksDB: DB not open." << std::endl;
      return false;
    }
    rocksdb::WriteBatch batch;
    for (const auto& e : entries)
    {
      batch.Put(e.first, e.second);
    }
    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
    {
      std::cerr << "RocksDB: Failed to batch write: " << status.ToString()
                << std::endl;
      return false;
    }
    return true;
  }

  // Atomic batch API used by Checkpointer
  rocksdb::WriteBatch create_batch()
  {
    return rocksdb::WriteBatch();
  }

  void add_to_batch(
    rocksdb::WriteBatch& batch,
    const std::string& key,
    const std::string& value)
  {
    batch.Put(key, value);
  }

  void add_delete_to_batch(rocksdb::WriteBatch& batch, const std::string& key)
  {
    batch.Delete(key);
  }

  void commit_batch(rocksdb::WriteBatch& batch)
  {
    if (!db_)
      return;
    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
    {
      std::cerr << "RocksDB: Failed to write batch: " << status.ToString()
                << std::endl;
    }
  }

  bool write_batch(rocksdb::WriteBatch&& batch)
  {
    if (!db_)
      return false;
    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok())
    {
      std::cerr << "RocksDB: Failed to write batch: " << status.ToString()
                << std::endl;
      return false;
    }
    return true;
  }

  // Scan prefix for metadata keys
  std::vector<std::pair<std::string, std::string>>
  scan_prefix(const std::string& prefix)
  {
    std::vector<std::pair<std::string, std::string>> result;
    if (!db_)
      return result;
    rocksdb::ReadOptions ro;
    ro.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
         it->Next())
    {
      result.emplace_back(it->key().ToString(), it->value().ToString());
    }
    return result;
  }

  // Create an iterator for reuse
  std::shared_ptr<rocksdb::Iterator> create_iterator()
  {
    if (!db_)
      return nullptr;
    rocksdb::ReadOptions ro;
    ro.prefix_same_as_start = true;
    return std::shared_ptr<rocksdb::Iterator>(db_->NewIterator(ro));
  }

  // Scan keys using an existing iterator
  void scan_keys(
    std::shared_ptr<rocksdb::Iterator>& it,
    const std::string& prefix,
    const std::function<void(const std::string& key)>& callback)
  {
    if (!it)
      return;

    // Optimization disabled for debugging
    // if (it->Valid()) ...
    it->Seek(prefix);

    for (; it->Valid() && it->key().starts_with(prefix); it->Next())
    {
      callback(it->key().ToString());
    }
  }

  // Callback-based scan for efficient iteration
  void scan_keys(
    const std::string& prefix,
    const std::function<void(const std::string& key)>& callback)
  {
    if (!db_)
      return;
    rocksdb::ReadOptions ro;
    ro.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
         it->Next())
    {
      callback(it->key().ToString());
    }
  }

  void delete_key(const std::string& key)
  {
    if (!db_)
      return;
    rocksdb::Status status = db_->Delete(rocksdb::WriteOptions(), key);
    if (!status.ok())
    {
      std::cerr << "RocksDB: Failed to delete key: " << status.ToString()
                << std::endl;
    }
  }

  void delete_prefix(const std::string& prefix)
  {
    if (!db_)
      return;
    rocksdb::Status status = db_->DeleteRange(
      rocksdb::WriteOptions(),
      db_->DefaultColumnFamily(),
      prefix,
      prefix + "\xff");
    if (!status.ok())
    {
      std::cerr << "RocksDB: Failed to delete prefix: " << status.ToString()
                << std::endl;
    }
  }

  void flush()
  {
    if (!db_)
      return;
    rocksdb::Status status = db_->Flush(rocksdb::FlushOptions());
    if (!status.ok())
    {
      std::cerr << "RocksDB: Failed to flush: " << status.ToString()
                << std::endl;
    }
  }
};

#endif // ROCKSDB_STORE_HPP