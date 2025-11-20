#include <spdlog/spdlog.h>
#include <spdlog/fmt/ostr.h>
#include <thread>

int main() {
    // 设置日志格式，包含trace_id和user_id
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%t] [trace_id=%{trace_id}, user_id=%{user_id}] %v");
    
    // 设置全局上下文
    spdlog::context::set("trace_id", "global_trace_12345");
    spdlog::context::set("user_id", "global_user_67890");
    
    // 主线程日志
    spdlog::info("Main thread message");
    
    // 创建子线程
    std::thread t([]() {
        // 子线程继承上下文
        spdlog::info("Child thread message (inherited context)");
        
        // 在子线程中修改上下文
        spdlog::context::set("user_id", "child_user_54321");
        spdlog::info("Child thread message (modified user_id)");
        
        // 设置新的上下文变量
        spdlog::context::set("request_id", "child_req_98765");
        spdlog::info("Child thread message (with request_id)");
    });
    
    // 等待子线程完成
    t.join();
    
    // 主线程日志，验证上下文未被修改
    spdlog::info("Main thread message after child thread");
    
    // 清除上下文
    spdlog::context::clear();
    spdlog::info("Main thread message after context clear");
    
    return 0;
}