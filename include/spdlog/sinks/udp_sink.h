// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#include <spdlog/common.h>
#include <spdlog/details/null_mutex.h>
#include <spdlog/sinks/base_sink.h>
#ifdef _WIN32
    #include <spdlog/details/udp_client-windows.h>
#else
    #include <spdlog/details/udp_client.h>
#endif

#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <atomic>
#include <memory>
#include <filesystem>
#include <spdlog/details/log_cache.h>

// Simple udp client sink
// Sends formatted log via udp

namespace spdlog {
namespace sinks {

enum class retry_strategy {
    fixed,
    exponential
};

enum class discard_policy {
    oldest,
    newest
};

struct udp_sink_config {
    std::string server_host;
    uint16_t server_port;

    // Retry configuration
    int max_retries = 3;
    retry_strategy retry_strategy = retry_strategy::exponential;
    std::chrono::milliseconds retry_interval = std::chrono::milliseconds(100);
    std::chrono::milliseconds send_timeout = std::chrono::milliseconds(500);

    // Network monitoring configuration
    std::chrono::seconds network_check_interval = std::chrono::seconds(30);

    // Local cache configuration
    std::string cache_dir = "./spdlog_cache";
    size_t max_cache_file_size = 100 * 1024 * 1024;  // 100MB
    int max_cache_files = 5;
    discard_policy cache_discard_policy = discard_policy::oldest;

    // Callback for severe errors
    std::function<void(const std::string &)> error_callback = nullptr;

    udp_sink_config(std::string host, uint16_t port)
        : server_host{std::move(host)},
          server_port{port} {}

    // Fluent configuration methods
    udp_sink_config &set_max_retries(int value) { max_retries = value; return *this; }
    udp_sink_config &set_retry_strategy(retry_strategy value) { retry_strategy = value; return *this; }
    udp_sink_config &set_retry_interval(std::chrono::milliseconds value) { retry_interval = value; return *this; }
    udp_sink_config &set_send_timeout(std::chrono::milliseconds value) { send_timeout = value; return *this; }
    udp_sink_config &set_network_check_interval(std::chrono::seconds value) { network_check_interval = value; return *this; }
    udp_sink_config &set_cache_dir(std::string value) { cache_dir = std::move(value); return *this; }
    udp_sink_config &set_max_cache_file_size(size_t value) { max_cache_file_size = value; return *this; }
    udp_sink_config &set_max_cache_files(int value) { max_cache_files = value; return *this; }
    udp_sink_config &set_cache_discard_policy(discard_policy value) { cache_discard_policy = value; return *this; }
    udp_sink_config &set_error_callback(std::function<void(const std::string &)> value) { error_callback = std::move(value); return *this; }
};

template <typename Mutex>
class udp_sink : public spdlog::sinks::base_sink<Mutex> {
public:
    // host can be hostname or ip address
    explicit udp_sink(udp_sink_config sink_config)
        : config_(std::move(sink_config))
        , client_(config_.server_host, config_.server_port)
        , cache_(config_)
        , is_network_up_(true)
        , network_check_thread_(nullptr)
        , stop_network_check_(false)
    {
        // Start network check thread
        start_network_check_thread_();
    }

    ~udp_sink() override
    {
        // Stop network check thread
        stop_network_check_ = true;
        if (network_check_thread_ != nullptr && network_check_thread_->joinable()) {
            network_check_thread_->join();
        }
    }

    // Check if network is up
    bool is_network_up() const
    {
        std::lock_guard<std::mutex> lock(network_mutex_);
        return is_network_up_;
    }

    // Flush cached logs
    void flush_cached_logs()
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        send_cached_logs_();
    }

    // Clear all cached logs
    void clear_cached_logs()
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        cache_.clear();
    }

    // Get total cache size
    size_t get_total_cache_size() const
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        return cache_.get_total_cache_size();
    }

protected:
    void sink_it_(const spdlog::details::log_msg &msg) override {
        spdlog::memory_buf_t formatted;
        spdlog::sinks::base_sink<Mutex>::formatter_->format(msg, formatted);
        std::string log_str = SPDLOG_BUF_TO_STRING(formatted);

        // Try to send log immediately
        if (send_log_(log_str)) {
            return;
        }

        // If sending failed, cache the log
        std::lock_guard<std::mutex> lock(cache_mutex_);
        cache_.add_log(log_str);
    }

    void flush_() override {}

private:
    // Send a single log message with retries
    bool send_log_(const std::string &log_str)
    {
        std::lock_guard<std::mutex> lock(network_mutex_);
        if (!is_network_up_) {
            return false;
        }

        int retries = 0;
        while (retries <= config_.max_retries) {
            try {
                client_.send(log_str.data(), log_str.size(), config_.send_timeout);
                return true;
            }
            catch (const spdlog::spdlog_ex &e) {
                SPDLOG_TRACE("Failed to send log (attempt {} of {}): {}", retries + 1, config_.max_retries + 1, e.what());

                // If this was the last retry, mark network as down
                if (retries == config_.max_retries) {
                    is_network_up_ = false;
                    SPDLOG_TRACE("Network marked as down after {} failed attempts", config_.max_retries + 1);
                    return false;
                }

                // Wait before retrying
                std::this_thread::sleep_for(get_retry_interval_(retries));
                retries++;
            }
        }

        return false;
    }

    // Get retry interval based on retry strategy
    std::chrono::milliseconds get_retry_interval_(int retry_count)
    {
        if (config_.retry_strategy == retry_strategy::fixed) {
            return config_.retry_interval;
        }
        else {
            // Exponential backoff with jitter
            auto interval = config_.retry_interval * (1 << retry_count);
            // Add jitter of up to 50% of the interval
            auto jitter = std::chrono::milliseconds(std::rand() % (interval.count() / 2));
            return interval + jitter;
        }
    }

    // Start network check thread
    void start_network_check_thread_()
    {
        network_check_thread_ = std::make_unique<std::thread>([this]() {
            while (!stop_network_check_) {
                std::this_thread::sleep_for(config_.network_check_interval);
                check_network_();
            }
        });
    }

    // Check network connectivity
    void check_network_()
    {
        bool was_up = is_network_up();
        bool is_up = false;

        // Try to send a small test message
        try {
            std::string test_msg = "spdlog_network_check";
            client_.send(test_msg.data(), test_msg.size(), std::chrono::milliseconds(1000));
            is_up = true;
        }
        catch (const spdlog::spdlog_ex &e) {
            is_up = false;
            SPDLOG_TRACE("Network check failed: {}", e.what());
        }

        // Update network status
        {
            std::lock_guard<std::mutex> lock(network_mutex_);
            is_network_up_ = is_up;
        }

        // If network just came up, send cached logs
        if (!was_up && is_up) {
            SPDLOG_TRACE("Network recovered, sending cached logs");
            std::lock_guard<std::mutex> lock(cache_mutex_);
            send_cached_logs_();
        }
    }

    // Send all cached logs
    void send_cached_logs_()
    {
        auto cache_files = cache_.get_cache_files();
        for (const auto &file_path : cache_files) {
            auto logs = cache_.read_and_remove_file(file_path);
            for (const auto &log : logs) {
                // Try to send log without retries (since we're in recovery mode)
                try {
                    client_.send(log.data(), log.size(), config_.send_timeout);
                }
                catch (const spdlog::spdlog_ex &e) {
                    SPDLOG_TRACE("Failed to send cached log: {}", e.what());
                    // If sending failed, put the log back to cache and stop
                    cache_.add_log(log);
                    // Put remaining logs back to cache
                    for (auto it = logs.begin() + 1; it != logs.end(); ++it) {
                        cache_.add_log(*it);
                    }
                    // Mark network as down again
                    {
                        std::lock_guard<std::mutex> lock(network_mutex_);
                        is_network_up_ = false;
                    }
                    return;
                }
            }
        }
    }

private:
    udp_sink_config config_;
    details::udp_client client_;
    details::log_cache cache_;
    mutable std::mutex network_mutex_;
    bool is_network_up_;
    std::unique_ptr<std::thread> network_check_thread_;
    std::atomic<bool> stop_network_check_;
    mutable std::mutex cache_mutex_;
};

using udp_sink_mt = udp_sink<std::mutex>;
using udp_sink_st = udp_sink<spdlog::details::null_mutex>;

}  // namespace sinks

//
// factory functions
//
template <typename Factory = spdlog::synchronous_factory>
inline std::shared_ptr<logger> udp_logger_mt(const std::string &logger_name,
                                             sinks::udp_sink_config sink_config) {
    return Factory::template create<sinks::udp_sink_mt>(logger_name, sink_config);
}

template <typename Factory = spdlog::synchronous_factory>
inline std::shared_ptr<logger> udp_logger_st(const std::string &logger_name,
                                             sinks::udp_sink_config sink_config) {
    return Factory::template create<sinks::udp_sink_st>(logger_name, sink_config);
}

}  // namespace spdlog
