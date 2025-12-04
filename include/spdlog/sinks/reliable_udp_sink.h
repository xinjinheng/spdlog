// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#include <spdlog/common.h>
#include <spdlog/details/null_mutex.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/details/file_helper.h>
#include <spdlog/details/udp_client.h>
#include <spdlog/details/os.h>
#include <spdlog/details/mpmc_blocking_q.h>
#include <spdlog/fmt/fmt.h>

#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <fstream>

namespace spdlog {
namespace sinks {

// Retry interval strategy
enum class retry_strategy {
    fixed,
    exponential_backoff
};

// Cache discard strategy
enum class cache_discard_strategy {
    discard_oldest,
    discard_newest
};

struct reliable_udp_sink_config {
    std::string server_host;
    uint16_t server_port;
    
    // Retry configuration
    int max_retry_count = 3;
    retry_strategy retry_strategy = retry_strategy::exponential_backoff;
    std::chrono::milliseconds retry_interval = std::chrono::milliseconds(100);
    std::chrono::milliseconds send_timeout = std::chrono::milliseconds(500);
    
    // Network monitoring configuration
    std::chrono::seconds network_check_interval = std::chrono::seconds(30);
    
    // Local cache configuration
    std::string cache_dir = "spdlog_cache";
    size_t max_cache_file_size = 100 * 1024 * 1024; // 100MB
    size_t max_cache_files = 5;
    cache_discard_strategy discard_strategy = cache_discard_strategy::discard_oldest;
    
    // Callback for severe errors
    std::function<void(const std::string &)> error_callback = nullptr;
    
    reliable_udp_sink_config(std::string host, uint16_t port)
        : server_host{std::move(host)},
          server_port{port} {}
    
    // Fluent configuration interface
    reliable_udp_sink_config &max_retry(int count) {
        max_retry_count = count;
        return *this;
    }
    
    reliable_udp_sink_config &with_retry_strategy(retry_strategy strategy) {
        retry_strategy = strategy;
        return *this;
    }
    
    reliable_udp_sink_config &retry_interval_ms(int ms) {
        retry_interval = std::chrono::milliseconds(ms);
        return *this;
    }
    
    reliable_udp_sink_config &send_timeout_ms(int ms) {
        send_timeout = std::chrono::milliseconds(ms);
        return *this;
    }
    
    reliable_udp_sink_config &network_check_interval_sec(int sec) {
        network_check_interval = std::chrono::seconds(sec);
        return *this;
    }
    
    reliable_udp_sink_config &cache_directory(const std::string &dir) {
        cache_dir = dir;
        return *this;
    }
    
    reliable_udp_sink_config &max_cache_size(size_t size) {
        max_cache_file_size = size;
        return *this;
    }
    
    reliable_udp_sink_config &max_cache_files_count(size_t count) {
        max_cache_files = count;
        return *this;
    }
    
    reliable_udp_sink_config &with_discard_strategy(cache_discard_strategy strategy) {
        discard_strategy = strategy;
        return *this;
    }
    
    reliable_udp_sink_config &on_error(std::function<void(const std::string &)> callback) {
        error_callback = std::move(callback);
        return *this;
    }
};

template <typename Mutex>
class reliable_udp_sink : public spdlog::sinks::base_sink<Mutex> {
public:
    explicit reliable_udp_sink(reliable_udp_sink_config sink_config)
        : config_(std::move(sink_config)),
          is_network_available_(true),
          stop_flag_(false),
          cache_queue_(10000) { // Initial queue size, will grow if needed
        initialize_cache_dir_();
        start_monitoring_thread_();
        start_sender_thread_();
    }
    
    ~reliable_udp_sink() override {
        stop_flag_ = true;
        cv_.notify_all();
        
        if (monitoring_thread_.joinable()) {
            monitoring_thread_.join();
        }
        
        if (sender_thread_.joinable()) {
            sender_thread_.join();
        }
        
        flush_cache_to_file_();
    }
    
    // Check if network is available
    bool is_network_available() const {
        return is_network_available_.load(std::memory_order_acquire);
    }
    
protected:
    void sink_it_(const spdlog::details::log_msg &msg) override {
        spdlog::memory_buf_t formatted;
        spdlog::sinks::base_sink<Mutex>::formatter_->format(msg, formatted);
        
        std::string log_str(formatted.begin(), formatted.end());
        cache_queue_.push(log_str);
        cv_.notify_all();
    }
    
    void flush_() override {
        // Flush is handled by the sender thread
    }
    
private:
    reliable_udp_sink_config config_;
    std::atomic<bool> is_network_available_;
    std::atomic<bool> stop_flag_;
    
    // Sender thread variables
    std::thread sender_thread_;
    std::condition_variable cv_;
    std::mutex cv_mutex_;
    details::mpmc_blocking_q<std::string> cache_queue_;
    
    // Monitoring thread variables
    std::thread monitoring_thread_;
    
    // Cache file variables
    std::mutex cache_mutex_;
    details::file_helper cache_file_;
    size_t current_cache_size_ = 0;
    size_t current_cache_file_index_ = 0;
    
    void initialize_cache_dir_() {
        // Create cache directory if it doesn't exist
        details::os::create_dir(config_.cache_dir);
    }
    
    void start_monitoring_thread_() {
        monitoring_thread_ = std::thread([this]() {
            while (!stop_flag_) {
                check_network_availability_();
                std::this_thread::sleep_for(config_.network_check_interval);
            }
        });
    }
    
    void start_sender_thread_() {
        sender_thread_ = std::thread([this]() {
            while (!stop_flag_) {
                std::string log_str;
                
                // Wait for a log message or stop signal
                {
                    std::unique_lock<std::mutex> lock(cv_mutex_);
                    cv_.wait(lock, [this]() {
                        return !cache_queue_.empty() || stop_flag_;
                    });
                }
                
                if (stop_flag_) {
                    break;
                }
                
                // Try to get the next log message
                if (cache_queue_.try_dequeue(log_str)) {
                    if (is_network_available_) {
                        if (!send_with_retry_(log_str)) {
                            cache_to_file_(log_str);
                        }
                    } else {
                        cache_to_file_(log_str);
                    }
                }
            }
            
            // Flush remaining cache to file before exiting
            flush_cache_to_file_();
        });
    }
    
    bool check_network_availability_() {
        try {
            // Create a temporary UDP client to test connectivity
            details::udp_client test_client(config_.server_host, config_.server_port);
            
            // Try to send a small test packet
            const char *test_data = "test";
            if (send_with_timeout_(test_client, test_data, strlen(test_data), std::chrono::milliseconds(100))) {
                if (!is_network_available_) {
                    is_network_available_ = true;
                    spdlog::info("Network connection restored");
                    
                    // Try to resend cached logs
                    resend_cached_logs_();
                }
                return true;
            }
        } catch (const spdlog::spdlog_ex &ex) {
            if (is_network_available_) {
                is_network_available_ = false;
                spdlog::warn("Network connection lost: {}", ex.what());
            }
        }
        
        return false;
    }
    
    bool send_with_retry_(const std::string &log_str) {
        int retry_count = 0;
        std::chrono::milliseconds current_interval = config_.retry_interval;
        
        while (retry_count <= config_.max_retry_count) {
            try {
                details::udp_client client(config_.server_host, config_.server_port);
                
                if (send_with_timeout_(client, log_str.data(), log_str.size(), config_.send_timeout)) {
                    return true;
                }
            } catch (const spdlog::spdlog_ex &ex) {
                spdlog::warn("Failed to send log (attempt {} of {}): {}", 
                            retry_count + 1, config_.max_retry_count + 1, ex.what());
            }
            
            retry_count++;
            
            if (retry_count <= config_.max_retry_count) {
                std::this_thread::sleep_for(current_interval);
                
                // Exponential backoff
                if (config_.retry_strategy == retry_strategy::exponential_backoff) {
                    current_interval *= 2;
                }
            }
        }
        
        spdlog::error("Failed to send log after {} attempts", config_.max_retry_count + 1);
        
        if (config_.error_callback) {
            config_.error_callback("Failed to send log after max retries: " + log_str);
        }
        
        return false;
    }
    
    bool send_with_timeout_(details::udp_client &client, const char *data, size_t size, std::chrono::milliseconds timeout) {
        // Set socket timeout
        struct timeval tv;
        tv.tv_sec = timeout.count() / 1000;
        tv.tv_usec = (timeout.count() % 1000) * 1000;
        
        if (::setsockopt(client.fd(), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0) {
            throw_spdlog_ex("setsockopt(SO_SNDTIMEO) failed", errno);
        }
        
        client.send(data, size);
        return true;
    }
    
    void cache_to_file_(const std::string &log_str) {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        
        try {
            if (!cache_file_.is_open()) {
                open_new_cache_file_();
            }
            
            // Check if we need to rotate cache file
            if (current_cache_size_ + log_str.size() > config_.max_cache_file_size) {
                rotate_cache_file_();
            }
            
            // Write to cache file
            cache_file_.write(log_str.data(), log_str.size());
            current_cache_size_ += log_str.size();
        } catch (const spdlog::spdlog_ex &ex) {
            spdlog::error("Failed to write to cache file: {}", ex.what());
            
            if (config_.error_callback) {
                config_.error_callback("Failed to write to cache file: " + std::string(ex.what()));
            }
        }
    }
    
    void open_new_cache_file_() {
        std::string filename = fmt::format("{}/cache_{:04d}.log", config_.cache_dir, current_cache_file_index_);
        cache_file_.open(filename, true); // Append mode
        current_cache_size_ = 0;
    }
    
    void rotate_cache_file_() {
        cache_file_.close();
        current_cache_file_index_++;
        
        // If we've exceeded max cache files, delete the oldest one
        if (current_cache_file_index_ >= config_.max_cache_files) {
            std::string oldest_file = fmt::format("{}/cache_{:04d}.log", config_.cache_dir, current_cache_file_index_ - config_.max_cache_files);
            details::os::remove(oldest_file);
        }
        
        open_new_cache_file_();
    }
    
    void flush_cache_to_file_() {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        
        if (cache_file_.is_open()) {
            cache_file_.flush();
            cache_file_.close();
        }
    }
    
    void resend_cached_logs_() {
        // Read all cache files and resend
        for (size_t i = 0; i <= current_cache_file_index_; i++) {
            std::string filename = fmt::format("{}/cache_{:04d}.log", config_.cache_dir, i);
            
            if (!details::os::exists(filename)) {
                continue;
            }
            
            try {
                std::ifstream cache_file(filename);
                std::string line;
                
                while (std::getline(cache_file, line)) {
                    if (!is_network_available_) {
                        // Network became unavailable again, stop resending
                        break;
                    }
                    
                    if (!send_with_retry_(line + "\n")) {
                        // Failed to send, put back into cache queue
                        cache_queue_.push(line + "\n");
                    }
                }
                
                // If we successfully sent all logs from this file, delete it
                if (is_network_available_) {
                    details::os::remove(filename);
                }
            } catch (const std::exception &ex) {
                spdlog::error("Failed to resend cached logs from file {}: {}", filename, ex.what());
            }
        }
        
        current_cache_file_index_ = 0;
    }
};

using reliable_udp_sink_mt = reliable_udp_sink<std::mutex>;
using reliable_udp_sink_st = reliable_udp_sink<spdlog::details::null_mutex>;

}  // namespace sinks

//
// factory functions
//
template <typename Factory = spdlog::synchronous_factory>
inline std::shared_ptr<logger> reliable_udp_logger_mt(const std::string &logger_name,
                                                     sinks::reliable_udp_sink_config config) {
    return Factory::template create<sinks::reliable_udp_sink_mt>(logger_name, std::move(config));
}

template <typename Factory = spdlog::synchronous_factory>
inline std::shared_ptr<logger> reliable_udp_logger_st(const std::string &logger_name,
                                                     sinks::reliable_udp_sink_config config) {
    return Factory::template create<sinks::reliable_udp_sink_st>(logger_name, std::move(config));
}

}  // namespace spdlog