#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

int main() {
    // 设置日志格式包含上下文
    auto logger = spdlog::stdout_color_mt("test_logger");
    logger->set_pattern("%Y-%m-%d %H:%M:%S.%e [%t] %^%l%$ %v [trace_id=%{trace_id}, user_id=%{user_id}]");
    
    // 设置全局上下文
    spdlog::context::set("trace_id", "12345");
    spdlog::context::set("user_id", "alice");
    
    // 输出日志，应该包含上下文
    logger->info("Testing context functionality");
    
    // 清除上下文
    spdlog::context::clear();
    
    // 再次输出日志，上下文应该为空
    logger->info("Context cleared");
    
    return 0;
}