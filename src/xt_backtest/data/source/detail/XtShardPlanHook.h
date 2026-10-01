#pragma once

/// @file
/// @brief 分片取用计划落定后的注入点（测试专用）。
///
/// 计划（哪些天复用、哪些天要建）与随后的动作之间存在真实的时间窗：取锁后重采源签名、
/// 交付池内分片路径，都发生在计划算完之后，而这中间别的进程可以改源文件、可以删分片。
/// 窗内事件靠外部时序复现不了（窗宽是微秒级且不可观测），故在计划落定处留一个显式注入
/// 点，让测试在确定的位置制造它。
///
/// 整条 seam 由 `XT_BT_TEST_SEAMS` 编译期开关圈起来。开关只挂在
/// `xt_bt_data_test_seams`（`xt_backtest/data/CMakeLists.txt` 里那份 object library，
/// 唯一消费者是需要注入的测试可执行文件）——`xt_bt_data` 与 `XtBackTestRunner` 一律不带，
/// 哪怕同一个构建树开着 `CLOUDTRADER_BUILD_TESTS`。不带开关时
/// `invoke_plan_settled_hook()` 是空的 inline 函数，装卸接口连声明都没有，钩子本身也
/// 不占任何静态存储。

#ifdef XT_BT_TEST_SEAMS
#include <functional>
#include <utility>
#endif

namespace xt_bt_shard_detail {

#ifdef XT_BT_TEST_SEAMS

/// @brief 计划落定回调；无参数无返回，只用于在窗内制造外部改动。
using PlanSettledHook = std::function<void()>;

/// @brief 安装钩子；传空 function 即摘除。非线程安全，只在测试单线程装卸。
void set_plan_settled_hook(PlanSettledHook hook);

/// @brief 触发钩子（未安装时为空操作）。
void invoke_plan_settled_hook();

/// @brief RAII 装卸；析构必摘，避免钩子泄漏到别的用例。
class ScopedPlanSettledHook {
public:
    explicit ScopedPlanSettledHook(PlanSettledHook hook) {
        set_plan_settled_hook(std::move(hook));
    }
    ~ScopedPlanSettledHook() { set_plan_settled_hook(nullptr); }

    ScopedPlanSettledHook(const ScopedPlanSettledHook&) = delete;
    ScopedPlanSettledHook& operator=(const ScopedPlanSettledHook&) = delete;
};

#else

inline void invoke_plan_settled_hook() {}

#endif  // XT_BT_TEST_SEAMS

}  // namespace xt_bt_shard_detail
