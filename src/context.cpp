// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#include <spdlog/context.h>

#ifdef SPDLOG_COMPILED_LIB

#ifdef SPDLOG_CONTEXT_USE_TLS
thread_local std::unique_ptr<spdlog::details::context_impl> spdlog::details::tls_context_;
#endif

#endif // SPDLOG_COMPILED_LIB
