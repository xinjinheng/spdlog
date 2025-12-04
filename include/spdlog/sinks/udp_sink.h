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
#include <vector>
#include <fstream>
#include <filesystem>
#include <random>
#include <fmt/core.h>

// Simple udp client sink
// Sends formatted log via udp

namespace spdlog {
namespace sinks {

struct udp_sink_config {
    std::string server_host;
    uint16_t server_port;

    // 重试机制配置
    int max_retries = 3;
    enum class retry_strategy { fixed, exponential };
    retry_strategy retry_strat = retry_strategy::exponential;
    int retry_interval_ms = 100;  // 固定间隔或指数退避的初始间隔
    int send_timeout_ms = 500;    // 单次发送超时时间

    // 网络状态监测配置
    int network_check_interval_sec = 30;  // 定期网络连通性检测间隔

    // 本地日志缓存配置
    std::string cache_dir = ".";  // 缓存文件目录
    size_t cache_file_max_size = 100 * 1024 * 1024;  // 单个缓存文件最大大小（默认100MB）
    int cache_file_max_count = 5;  // 缓存文件最大数量（默认5个）
    enum class cache_full_strategy { discard_oldest, discard_newest };
    cache_full_strategy cache_full_strat = cache_full_strategy::discard_oldest;

    // 回调接口
    std::function<void(const std::string &)> error_callback;  // 严重错误回调

    udp_sink_config(std::string host, uint16_t port)
        : server_host{std::move(host)},
          server_port{port} {}

    // 流式配置接口
    udp_sink_config &set_max_retries(int retries) {
        max_retries = retries;
        return *this;
    }

    udp_sink_config &set_retry_strategy(retry_strategy strat) {
        retry_strat = strat;
        return *this;
    }

    udp_sink_config &set_retry_interval_ms(int interval) {
        retry_interval_ms = interval;
        return *this;
    }

    udp_sink_config &set_send_timeout_ms(int timeout) {
        send_timeout_ms = timeout;
        return *this;
    }

    udp_sink_config &set_network_check_interval_sec(int interval) {
        network_check_interval_sec = interval;
        return *this;
    }

    udp_sink_config &set_cache_dir(std::string dir) {
        cache_dir = std::move(dir);
        return *this;
    }

    udp_sink_config &set_cache_file_max_size(size_t size) {
        cache_file_max_size = size;
        return *this;
    }

    udp_sink_config &set_cache_file_max_count(int count) {
        cache_file_max_count = count;
        return *this;
    }

    udp_sink_config &set_cache_full_strategy(cache_full_strategy strat) {
        cache_full_strat = strat;
        return *this;
    }

    udp_sink_config &set_error_callback(std::function<void(const std::string &)> callback) {
        error_callback = std::move(callback);
        return *this;
    }
};

template <typename Mutex>
class udp_sink : public spdlog::sinks::base_sink<Mutex> {
public:
    // host can be hostname or ip address
    explicit udp_sink(udp_sink_config sink_config)
        : config_{std::move(sink_config)},
          client_{config_.server_host, config_.server_port},
          is_network_available_{true},
          network_monitor_running_{false},
          cache_current_size_{0} {
        // 初始化随机数生成器
        std::srand(static_cast<unsigned int>(std::time(nullptr)));
        
        // 启动网络状态监测线程
        start_network_monitor_thread();
    }

    ~udp_sink() override {
        // 停止网络状态监测线程
        stop_network_monitor_thread();
        // 刷新缓存
        flush_cache();
    }

    // 供外部查询网络状态
    bool is_network_available() const {
        std::lock_guard<Mutex> lock(base_sink<Mutex>::mutex_);
        return is_network_available_;
    }

protected:
    void sink_it_(const spdlog::details::log_msg &msg) override {
        spdlog::memory_buf_t formatted;
        spdlog::sinks::base_sink<Mutex>::formatter_->format(msg, formatted);

        std::string log_str(formatted.data(), formatted.size());

        // 尝试发送日志
        if (try_send_log(log_str)) {
            // 发送成功
        } else {
            // 发送失败，将日志缓存到本地
            cache_log(log_str);
        }
    }

    void flush_() override {
        flush_cache();
    }

private:
    // 尝试发送日志，支持重试
    bool try_send_log(const std::string &log_str) {
        for (int retry = 0; retry <= config_.max_retries; ++retry) {
            try {
                // 设置发送超时
                set_send_timeout(config_.send_timeout_ms);

                // 发送日志
                client_.send(log_str.data(), log_str.size());

                // 发送成功，检查是否有缓存的日志需要发送
                if (has_cached_logs()) {
                    flush_cache();
                }

                // 更新网络状态为可用
                update_network_status(true);

                return true;
            } catch (const std::exception &e) {
                // 发送失败，记录错误
                log_error(fmt::format("Failed to send log (retry {}): {}", retry, e.what())); 

                // 如果不是最后一次重试，等待重试间隔
                if (retry < config_.max_retries) {
                    int interval = config_.retry_interval_ms;
                    if (config_.retry_strat == udp_sink_config::retry_strategy::exponential) {
                        interval *= (1 << retry);  // 指数退避
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(interval));
                }
            }
        }

        // 所有重试都失败了，更新网络状态为不可用
        update_network_status(false);

        return false;
    }

    // 设置发送超时
    void set_send_timeout(int timeout_ms) {
#ifdef _WIN32
        DWORD timeout = timeout_ms;
        setsockopt(client_.fd(), SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char *>(&timeout), sizeof(timeout));
#else
        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(client_.fd(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#endif
    }

    // 启动网络状态监测线程
    void start_network_monitor_thread() {
        network_monitor_running_ = true;
        network_monitor_thread_ = std::thread([this]() {
            while (network_monitor_running_) {
                // 检查网络连通性
                bool is_available = check_network_availability();

                // 如果网络状态从不可用变为可用，自动恢复日志发送
                if (!is_network_available_ && is_available) {
                    flush_cache();
                }

                // 更新网络状态
                update_network_status(is_available);

                // 等待检查间隔
                std::this_thread::sleep_for(std::chrono::seconds(config_.network_check_interval_sec));
            }
        });
    }

    // 停止网络状态监测线程
    void stop_network_monitor_thread() {
        network_monitor_running_ = false;
        if (network_monitor_thread_.joinable()) {
            network_monitor_thread_.join();
        }
    }

    // 检查网络连通性
    bool check_network_availability() {
        try {
            // 发送一个小的测试数据包来检查网络连通性
            std::string test_packet = "spdlog_network_test";
            set_send_timeout(config_.send_timeout_ms);
            client_.send(test_packet.data(), test_packet.size());
            return true;
        } catch (const std::exception &e) {
            return false;
        }
    }

    // 更新网络状态
    void update_network_status(bool is_available) {
        std::lock_guard<Mutex> lock(base_sink<Mutex>::mutex_);
        is_network_available_ = is_available;
    }

    // 缓存日志到本地文件
    void cache_log(const std::string &log_str) {
        try {
            // 检查缓存是否已满
            if (is_cache_full()) {
                // 缓存已满，根据配置的策略处理
                if (config_.cache_full_strat == udp_sink_config::cache_full_strategy::discard_oldest) {
                    // 丢弃最旧的缓存文件
                    delete_oldest_cache_file();
                } else {
                    // 丢弃最新的日志
                    return;
                }
            }

            // 打开当前缓存文件
            std::ofstream cache_file(get_current_cache_file_path(), std::ios::app);
            if (!cache_file) {
                throw std::runtime_error("Failed to open cache file");
            }

            // 写入日志
            cache_file << log_str;
            cache_file.flush();

            // 更新当前缓存文件大小
            cache_current_size_ += log_str.size();

            // 如果当前缓存文件大小超过最大值，切换到新的缓存文件
            if (cache_current_size_ >= config_.cache_file_max_size) {
                switch_to_next_cache_file();
            }
        } catch (const std::exception &e) {
            // 缓存失败，记录错误并调用回调接口
            std::string error_msg = fmt::format("Failed to cache log: {}", e.what()); 
            log_error(error_msg);
            if (config_.error_callback) {
                config_.error_callback(error_msg);
            }
        }
    }

    // 刷新缓存，发送所有缓存的日志
    void flush_cache() {
        try {
            // 获取所有缓存文件
            std::vector<std::string> cache_files = get_cache_files();

            // 按时间顺序发送所有缓存的日志
            for (const std::string &file_path : cache_files) {
                // 打开缓存文件
                std::ifstream cache_file(file_path);
                if (!cache_file) {
                    throw std::runtime_error(fmt::format("Failed to open cache file: {}", file_path));
                } 

                // 逐行读取日志并发送
                std::string line;
                while (std::getline(cache_file, line)) {
                    line += '\n';  // 恢复换行符
                    if (!try_send_log(line)) {
                        // 发送失败，停止刷新缓存
                        return;
                    }
                }

                // 发送成功，删除缓存文件
                std::remove(file_path.c_str());
            }

            // 重置缓存状态
            cache_current_size_ = 0;
        } catch (const std::exception &e) {
            // 刷新缓存失败，记录错误并调用回调接口
            std::string error_msg = fmt::format("Failed to flush cache: {}", e.what()); 
            log_error(error_msg);
            if (config_.error_callback) {
                config_.error_callback(error_msg);
            }
        }
    }

    // 检查缓存是否已满
    bool is_cache_full() {
        std::vector<std::string> cache_files = get_cache_files();
        return cache_files.size() >= static_cast<size_t>(config_.cache_file_max_count);
    }

    // 获取所有缓存文件，按时间顺序排列
    std::vector<std::string> get_cache_files() {
        std::vector<std::string> cache_files;

        // 检查缓存目录是否存在
        std::filesystem::path cache_dir_path(config_.cache_dir);
        if (!std::filesystem::exists(cache_dir_path)) {
            // 缓存目录不存在，创建它
            std::filesystem::create_directories(cache_dir_path);
            return cache_files;
        }

        // 遍历缓存目录，找到所有缓存文件
        std::filesystem::directory_iterator dir_iter(cache_dir_path);
        for (const auto &entry : dir_iter) {
            if (entry.is_regular_file()) {
                std::string file_name = entry.path().filename().string();
                // 检查文件名是否符合缓存文件的格式
                if (file_name.find("spdlog_cache_") == 0) {
                    cache_files.push_back(entry.path().string());
                }
            }
        }

        // 按时间顺序排序，最旧的文件排在前面
        std::sort(cache_files.begin(), cache_files.end(), [](const std::string &a, const std::string &b) {
            return std::filesystem::last_write_time(a) < std::filesystem::last_write_time(b);
        });

        return cache_files;
    }

    // 获取当前缓存文件的路径
    std::string get_current_cache_file_path() {
        std::vector<std::string> cache_files = get_cache_files();
        if (cache_files.empty()) {
            // 没有缓存文件，创建一个新的
            return create_new_cache_file();
        } else {
            // 返回最后一个缓存文件（最新的）
            return cache_files.back();
        }
    }

    // 创建一个新的缓存文件
    std::string create_new_cache_file() {
        // 生成唯一的文件名
        std::string file_name = fmt::format("spdlog_cache_{}_{}", std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()), std::rand()); 
        std::string file_path = std::filesystem::path(config_.cache_dir) / file_name;

        // 创建文件
        std::ofstream cache_file(file_path);
        if (!cache_file) {
            throw std::runtime_error(fmt::format("Failed to create cache file: {}", file_path));
        } 

        return file_path;
    }

    // 切换到下一个缓存文件
    void switch_to_next_cache_file() {
        create_new_cache_file();
        cache_current_size_ = 0;
    }

    // 删除最旧的缓存文件
    void delete_oldest_cache_file() {
        std::vector<std::string> cache_files = get_cache_files();
        if (!cache_files.empty()) {
            std::remove(cache_files.front().c_str());
        }
    }

    // 检查是否有缓存的日志
    bool has_cached_logs() {
        std::vector<std::string> cache_files = get_cache_files();
        return !cache_files.empty();
    }

    // 记录错误信息（使用内部日志）
    void log_error(const std::string &error_msg) {
        // 这里可以使用spdlog的内部日志记录错误信息
        // 为了避免循环依赖，我们可以直接将错误信息输出到标准错误流
        std::cerr << "spdlog udp_sink error: " << error_msg << std::endl;
        
        // 如果设置了错误回调，也调用回调
        if (config_.error_callback) {
            config_.error_callback(error_msg);
        }
    }

    udp_sink_config config_;
    details::udp_client client_;

    // 网络状态
    bool is_network_available_;

    // 网络状态监测线程
    std::thread network_monitor_thread_;
    std::atomic<bool> network_monitor_running_;

    // 缓存状态
    size_t cache_current_size_;
};

using udp_sink_mt = udp_sink<std::mutex>;
using udp_sink_st = udp_sink<spdlog::details::null_mutex>;

}  // namespace sinks

//
// factory functions
//
// 创建多线程udp logger
template <typename Factory = spdlog::synchronous_factory>
inline std::shared_ptr<logger> udp_logger_mt(const std::string &logger_name,
                                             sinks::udp_sink_config sink_config) {
    return Factory::template create<sinks::udp_sink_mt>(logger_name, sink_config);
}

// 创建单线程udp logger
template <typename Factory = spdlog::synchronous_factory>
inline std::shared_ptr<logger> udp_logger_st(const std::string &logger_name,
                                             sinks::udp_sink_config sink_config) {
    return Factory::template create<sinks::udp_sink_st>(logger_name, sink_config);
}

}  // namespace spdlog
