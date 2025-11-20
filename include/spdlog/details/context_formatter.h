// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#include <spdlog/details/log_msg.h>
#include <spdlog/details/pattern_formatter.h>
#include <spdlog/context.h>

namespace spdlog {
namespace details {

template <typename ScopedPadder>
class context_formatter final : public flag_formatter
{
public:
    explicit context_formatter(std::string key, padding_info padinfo)
        : flag_formatter(padinfo), key_(std::move(key))
    {}

    void format(const details::log_msg &msg, const std::tm &, memory_buf_t &dest) override
    {
        std::string value;
        if (msg.context)
        {
            value = msg.context->get(key_);
        }
        else
        {
            // Fallback to current thread context if log_msg context is not available
            value = spdlog::context::get(key_);
        }
        ScopedPadder p(value.size(), padinfo_, dest);
        fmt_helper::append_string_view(string_view_t(value.data(), value.size()), dest);
    }

private:
    std::string key_;
};

} // namespace details
} // namespace spdlog