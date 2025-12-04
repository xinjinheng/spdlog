// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#include <spdlog/common.h>
#include <spdlog/details/file_helper.h>
#include <spdlog/details/os.h>
#include <spdlog/details/log_msg.h>
#include <spdlog/fmt/fmt.h>

// Forward declarations from udp_sink.h
namespace spdlog {
namespace sinks {

enum class retry_strategy;
enum class discard_policy;

struct udp_sink_config;

}} // namespace spdlog::sinks

#include <chrono>
#include <deque>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

namespace spdlog {
namespace details {

class log_cache {
private:
    struct cache_file {
        std::filesystem::path path;
        size_t size;
        std::chrono::system_clock::time_point created_time;
    };

public:
    log_cache(const sinks::udp_sink_config &config)
        : config_(config)
    {
        // Create cache directory if it doesn't exist
        std::filesystem::create_directories(config_.cache_dir);
        // Load existing cache files
        load_cache_files_();
    }

    ~log_cache() = default;

    // Add a log message to the cache
    void add_log(const std::string &log_message)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // Check if we need to create a new cache file
        if (!current_file_.is_open() || current_file_size_ >= config_.max_cache_file_size) {
            close_current_file_();
            if (!open_new_file_()) {
                // Failed to open new file, apply discard policy
                apply_discard_policy_();
                if (!open_new_file_()) {
                    // Still failed, call error callback
                    if (config_.error_callback) {
                        config_.error_callback("Failed to open cache file after discard");
                    }
                    return;
                }
            }
        }

        // Write log message to current file
        current_file_ << log_message << std::endl;
        current_file_size_ += log_message.size() + 1; // +1 for newline
    }

    // Get all cached log files
    std::vector<std::filesystem::path> get_cache_files() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::filesystem::path> files;
        for (const auto &cache_file : cache_files_) {
            files.push_back(cache_file.path);
        }
        return files;
    }

    // Read and remove a log file
    std::vector<std::string> read_and_remove_file(const std::filesystem::path &file_path)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> logs;

        // Read log file
        std::ifstream file(file_path);
        if (file.is_open()) {
            std::string line;
            while (std::getline(file, line)) {
                logs.push_back(line);
            }
            file.close();
        }

        // Remove file
        std::filesystem::remove(file_path);

        // Update cache files list
        load_cache_files_();

        return logs;
    }

    // Clear all cache files
    void clear()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        close_current_file_();
        for (const auto &cache_file : cache_files_) {
            std::filesystem::remove(cache_file.path);
        }
        cache_files_.clear();
    }

    // Get total cache size
    size_t get_total_cache_size() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        size_t total_size = 0;
        for (const auto &cache_file : cache_files_) {
            total_size += cache_file.size;
        }
        return total_size;
    }

    // Get number of cache files
    size_t get_cache_file_count() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return cache_files_.size();
    }

private:
    // Load existing cache files from directory
    void load_cache_files_()
    {
        cache_files_.clear();

        try {
            for (const auto &entry : std::filesystem::directory_iterator(config_.cache_dir)) {
                if (entry.is_regular_file() && entry.path().extension() == ".log") {
                    cache_file cf;
                    cf.path = entry.path();
                    cf.size = std::filesystem::file_size(entry.path());
                    cf.created_time = std::filesystem::last_write_time(entry.path());
                    cache_files_.push_back(cf);
                }
            }

            // Sort cache files by creation time (oldest first)
            std::sort(cache_files_.begin(), cache_files_.end(),
                [](const cache_file &a, const cache_file &b) {
                    return a.created_time < b.created_time;
                });
        }
        catch (const std::exception &e) {
            // Ignore errors when loading cache files
            SPDLOG_TRACE("Failed to load cache files: {}", e.what());
        }
    }

    // Open a new cache file
    bool open_new_file_()
    {
        try {
            // Generate unique file name
            auto timestamp = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            std::string file_name = fmt_lib::format("spdlog_cache_{}.log", timestamp);
            std::filesystem::path file_path = std::filesystem::path(config_.cache_dir) / file_name;

            // Open file in append mode
            current_file_.open(file_path, std::ios::out | std::ios::app);
            if (!current_file_.is_open()) {
                return false;
            }

            current_file_path_ = file_path;
            current_file_size_ = 0;

            // Add new file to cache files list
            cache_file cf;
            cf.path = file_path;
            cf.size = 0;
            cf.created_time = std::chrono::system_clock::now();
            cache_files_.push_back(cf);

            return true;
        }
        catch (const std::exception &e) {
            SPDLOG_TRACE("Failed to open new cache file: {}", e.what());
            return false;
        }
    }

    // Close current cache file
    void close_current_file_()
    {
        if (current_file_.is_open()) {
            current_file_.close();
            current_file_path_.clear();
            current_file_size_ = 0;
        }
    }

    // Apply discard policy when cache is full
    void apply_discard_policy_()
    {
        if (cache_files_.empty()) {
            return;
        }

        if (config_.cache_discard_policy == sinks::discard_policy::oldest) {
            // Discard oldest file
            std::filesystem::remove(cache_files_.front().path);
            cache_files_.erase(cache_files_.begin());
        }
        else {
            // Discard newest file
            std::filesystem::remove(cache_files_.back().path);
            cache_files_.pop_back();
        }
    }

private:
    const sinks::udp_sink_config &config_;
    mutable std::mutex mutex_;
    std::deque<cache_file> cache_files_;
    std::ofstream current_file_;
    std::filesystem::path current_file_path_;
    size_t current_file_size_ = 0;
};

} // namespace details
} // namespace spdlog
