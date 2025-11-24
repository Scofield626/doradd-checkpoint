#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include "checkpoint_stats.hpp"
#include "checkpointer.hpp"

template<typename StorageType, typename RowType>
class Recovery {
public:
    Recovery(const std::string& path = DefaultDBPath) {
        if (!storage.open(path)) {
            throw std::runtime_error("Failed to open recovery database");
        }
    }

    // Recover all rows from the database
    std::vector<std::pair<uint64_t, RowType>> recover_all() {
        std::vector<std::pair<uint64_t, RowType>> recovered_data;
        std::string meta_prefix = "_meta_bit";
        
        // First, read all meta bits to determine which version to use
        std::unordered_map<uint64_t, bool> meta_bits;
        for (auto& kv : storage.scan_prefix(meta_prefix)) {
            uint64_t k = std::stoull(kv.first.substr(0, kv.first.size() - meta_prefix.size()));
            meta_bits[k] = (kv.second.size() > 0 && kv.second[0] == '1');
        }

        // Then read the actual data using the correct version
        for (const auto& [key, bit] : meta_bits) {
            std::string versioned_key = std::to_string(key) + "_v" + (bit ? '1' : '0');
            auto data = storage.get(versioned_key);
            if (data) {
                RowType row;
                std::memcpy(&row, data->data(), sizeof(RowType));
                recovered_data.emplace_back(key, row);
            }
        }

        return recovered_data;
    }

    // Recover a specific row by key
    std::optional<RowType> recover_row(uint64_t key) {
        // First get the meta bit to determine which version to use
        std::string meta_key = std::to_string(key) + "_meta_bit";
        auto meta_data = storage.get(meta_key);
        if (!meta_data || meta_data->empty()) {
            return std::nullopt;
        }

        bool bit = (meta_data->at(0) == '1');
        std::string versioned_key = std::to_string(key) + "_v" + (bit ? '1' : '0');
        
        auto data = storage.get(versioned_key);
        if (!data) {
            return std::nullopt;
        }

        RowType row;
        std::memcpy(&row, data->data(), sizeof(RowType));
        return row;
    }

    // Get the total number of transactions from the database
    uint64_t get_total_transactions() {
        auto data = storage.get("total_transactions");
        if (!data) {
            return 0;
        }
        return std::stoull(*data);
    }

private:
    StorageType storage;
}; 