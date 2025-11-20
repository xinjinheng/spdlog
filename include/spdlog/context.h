// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#include <spdlog/details/os.h>
#include <spdlog/fmt/fmt.h>
#include <unordered_map>
#include <string>
#include <memory>
#include <atomic>
#include <mutex>

// Define SPDLOG_ENABLE_CONTEXT if it's not already defined
#ifndef SPDLOG_NO_CONTEXT
    #ifndef SPDLOG_ENABLE_CONTEXT
        #define SPDLOG_ENABLE_CONTEXT 1
    #endif
#endif

#ifdef SPDLOG_ENABLE_CONTEXT
    #include <thread>
    #ifndef SPDLOG_NO_TLS
        #define SPDLOG_CONTEXT_USE_TLS 1
    #endif
#endif

namespace spdlog {
namespace details {
struct context_impl
{
    using map_t = std::unordered_map<std::string, std::string>;
    map_t data_;

    context_impl() = default;
    context_impl(const context_impl &other)
    {
        std::lock_guard<std::mutex> lock(other.mutex_);
        data_ = other.data_;
    }

    context_impl &operator=(const context_impl &other)
    {
        if (this == &other)
            return *this;
        std::lock_guard<std::mutex> lock(mutex_), lock_other(other.mutex_);
        data_ = other.data_;
        return *this;
    }

    void set(const std::string &key, const std::string &value)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        data_[key] = value;
    }

    std::string get(const std::string &key) const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = data_.find(key);
        return it != data_.end() ? it->second : "";
    }

    void remove(const std::string &key)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        data_.erase(key);
    }

    void clear()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        data_.clear();
    }

    bool empty() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return data_.empty();
    }

    map_t copy() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return data_;
    }

private:
    mutable std::mutex mutex_;
};

#ifdef SPDLOG_CONTEXT_USE_TLS
    extern thread_local std::unique_ptr<context_impl> tls_context_;
#endif
}

class SPDLOG_API context
{
public:
    context() = default;
    ~context() = default;

    context(const context &other) = delete;
    context &operator=(const context &other) = delete;

    static void set(const std::string &key, const std::string &value)
    {
#ifdef SPDLOG_CONTEXT_USE_TLS
        if (!details::tls_context_)
        {
            details::tls_context_ = details::make_unique<details::context_impl>();
        }
        details::tls_context_->set(key, value);
#endif
    }

    static void set(const std::string &key, fmt::format_string<> fmt)
    {
        set(key, fmt::format(fmt));
    }

    template<typename... Args>
    static void set(const std::string &key, fmt::format_string<Args...> fmt, Args &&...args)
    {
        set(key, fmt::format(fmt, std::forward<Args>(args)...));
    }

    static std::string get(const std::string &key)
    {
#ifdef SPDLOG_CONTEXT_USE_TLS
        if (details::tls_context_)
        {
            return details::tls_context_->get(key);
        }
#endif
        return "";
    }

    static void remove(const std::string &key)
    {
#ifdef SPDLOG_CONTEXT_USE_TLS
        if (details::tls_context_)
        {
            details::tls_context_->remove(key);
        }
#endif
    }

    static void clear()
    {
#ifdef SPDLOG_CONTEXT_USE_TLS
        if (details::tls_context_)
        {
            details::tls_context_->clear();
        }
#endif
    }

    static std::unique_ptr<details::context_impl> capture()
    {
#ifdef SPDLOG_CONTEXT_USE_TLS
        if (details::tls_context_)
        {
            return details::make_unique<details::context_impl>(*details::tls_context_);
        }
#endif
        return nullptr;
    }

    static void restore(const std::unique_ptr<details::context_impl> &ctx)
    {
#ifdef SPDLOG_CONTEXT_USE_TLS
        if (ctx)
        {
            details::tls_context_ = details::make_unique<details::context_impl>(*ctx);
        }
        else
        {
            details::tls_context_.reset();
        }
#endif
    }

    template<typename Fn>
    static void with(const std::unique_ptr<details::context_impl> &ctx, Fn &&fn)
    {
        auto old_ctx = capture();
        restore(ctx);
        try
        {
            fn();
        }
        catch(...)
        {
            restore(old_ctx);
            throw;
        }
        restore(old_ctx);
    }
};

// Macros for common context fields
#define SPDLOG_SET_TRACE_ID(trace_id) spdlog::context::set("trace_id", trace_id)
#define SPDLOG_SET_SPAN_ID(span_id) spdlog::context::set("span_id", span_id)
#define SPDLOG_SET_PARENT_SPAN_ID(parent_span_id) spdlog::context::set("parent_span_id", parent_span_id)
#define SPDLOG_SET_USER_ID(user_id) spdlog::context::set("user_id", user_id)
#define SPDLOG_SET_REQUEST_ID(request_id) spdlog::context::set("request_id", request_id)

#define SPDLOG_TRACE_ID() spdlog::context::get("trace_id")
#define SPDLOG_SPAN_ID() spdlog::context::get("span_id")
#define SPDLOG_PARENT_SPAN_ID() spdlog::context::get("parent_span_id")
#define SPDLOG_USER_ID() spdlog::context::get("user_id")
#define SPDLOG_REQUEST_ID() spdlog::context::get("request_id")

} // namespace spdlog
