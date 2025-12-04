// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#include "includes.h"
#include <spdlog/sinks/udp_sink.h>
#include <thread>
#include <chrono>

class test_udp_server
{
private:
    int socket_;
    sockaddr_in addr_;
    std::thread server_thread_;
    std::atomic<bool> stop_;
    std::vector<std::string> received_messages_;
    std::mutex mutex_;

public:
    test_udp_server(uint16_t port)
        : stop_(false)
    {
        // Create socket
        socket_ = ::socket(PF_INET, SOCK_DGRAM, 0);
        if (socket_ < 0) {
            throw std::runtime_error("Failed to create socket");
        }

        // Set address
        addr_.sin_family = AF_INET;
        addr_.sin_port = htons(port);
        addr_.sin_addr.s_addr = INADDR_ANY;
        ::memset(addr_.sin_zero, 0x00, sizeof(addr_.sin_zero));

        // Bind socket
        if (::bind(socket_, (struct sockaddr *)&addr_, sizeof(addr_)) < 0) {
            ::close(socket_);
            throw std::runtime_error("Failed to bind socket");
        }

        // Start server thread
        server_thread_ = std::thread([this]() {
            run_();
        });
    }

    ~test_udp_server()
    {
        stop_ = true;
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
        ::close(socket_);
    }

    std::vector<std::string> get_received_messages() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return received_messages_;
    }

    void clear_received_messages()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        received_messages_.clear();
    }

private:
    void run_()
    {
        char buffer[1024];
        sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);

        while (!stop_) {
            // Set timeout for recvfrom
            struct timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100000;  // 100ms
            ::setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char *>(&tv), sizeof(tv));

            // Receive data
            ssize_t recv_len = ::recvfrom(socket_, buffer, sizeof(buffer), 0, (struct sockaddr *)&client_addr, &client_addr_len);
            if (recv_len > 0) {
                std::string message(buffer, recv_len);
                std::lock_guard<std::mutex> lock(mutex_);
                received_messages_.push_back(message);
            }
        }
    }
};

TEST_CASE("udp_sink_reliable_basic", "[udp_sink]")
{
    uint16_t port = 11092;
    test_udp_server server(port);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Create UDP sink with default configuration
    spdlog::sinks::udp_sink_config config("127.0.0.1", port);
    auto sink = std::make_shared<spdlog::sinks::udp_sink_mt>(config);
    auto logger = std::make_shared<spdlog::logger>("udp_test", sink);
    logger->set_level(spdlog::level::info);

    // Log some messages
    std::string test_message = "Hello, UDP!";
    for (int i = 0; i < 10; ++i) {
        logger->info("{}", test_message);
    }

    // Wait for messages to be received
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Check received messages
    auto received = server.get_received_messages();
    REQUIRE(received.size() >= 10);
    for (const auto &msg : received) {
        REQUIRE(msg.find(test_message) != std::string::npos);
    }
}

TEST_CASE("udp_sink_reliable_config", "[udp_sink]")
{
    // Test fluent configuration interface
    spdlog::sinks::udp_sink_config config("127.0.0.1", 11093);
    config.set_max_retries(5)
          .set_retry_strategy(spdlog::sinks::retry_strategy::fixed)
          .set_retry_interval(std::chrono::milliseconds(200))
          .set_send_timeout(std::chrono::milliseconds(1000))
          .set_network_check_interval(std::chrono::seconds(10))
          .set_cache_dir("./test_udp_cache")
          .set_max_cache_file_size(10 * 1024 * 1024)  // 10MB
          .set_max_cache_files(2)
          .set_cache_discard_policy(spdlog::sinks::discard_policy::newest);

    REQUIRE(config.max_retries == 5);
    REQUIRE(config.retry_strategy == spdlog::sinks::retry_strategy::fixed);
    REQUIRE(config.retry_interval == std::chrono::milliseconds(200));
    REQUIRE(config.send_timeout == std::chrono::milliseconds(1000));
    REQUIRE(config.network_check_interval == std::chrono::seconds(10));
    REQUIRE(config.cache_dir == "./test_udp_cache");
    REQUIRE(config.max_cache_file_size == 10 * 1024 * 1024);
    REQUIRE(config.max_cache_files == 2);
    REQUIRE(config.cache_discard_policy == spdlog::sinks::discard_policy::newest);
}

TEST_CASE("udp_sink_reliable_network_status", "[udp_sink]")
{
    // Create UDP sink with default configuration
    spdlog::sinks::udp_sink_config config("127.0.0.1", 11094);
    auto sink = std::make_shared<spdlog::sinks::udp_sink_mt>(config);

    // Initially, network should be up (we haven't tried to send anything yet)
    REQUIRE(sink->is_network_up() == true);

    // Log a message to a non-existent server
    auto logger = std::make_shared<spdlog::logger>("udp_test", sink);
    logger->set_level(spdlog::level::info);
    logger->info("Test message");

    // Wait for retries to complete
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Network should be marked as down
    REQUIRE(sink->is_network_up() == false);
}
