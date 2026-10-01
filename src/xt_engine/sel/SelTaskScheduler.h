#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "api/XtStrategyRuntimeTypes.h"  // TaskPeriod enum

namespace xt_engine_sel {

/**
 * @brief add_task() 接收的 selector 调度任务。
 *
 * @note `period` / `day` / `time` / `trdtpl` / `session` 定义 task 的触发条件；
 *       `last_exe_date` 用于同一交易日去重。
 */
struct SelTaskInfo {
    uint32_t    id = 0;                ///< 唯一 task id（engine 分配）
    std::string name;                  ///< 策略名 / 任务标签（log 用）
    std::string strategy_id;           ///< 关联的 strategy_id（多策略 SEL）
    TaskPeriod  period = TaskPeriod::None;
    /// @brief Weekly: 1-7 (Mon=1)；Monthly: 1-31；Yearly: MMDD；Daily/Minute: ignored。
    uint32_t    day = 0;
    uint32_t    time = 0;              ///< HHMM 触发时分（如 1500 = 15:00）
    std::string trdtpl = "CHINA";      ///< 交易日历模板（默认 SSE/CHINA）
    std::string session = "default";   ///< Session 名（CTA bar session 复用）
    bool        strict_time = false;   ///< true: 严格相等才触发；false: ≥ time 即触发
    uint32_t    last_exe_date = 0;     ///< 最近一次触发日期（YYYYMMDD），同日去重
};

/**
 * @brief 提供 selector scheduler 使用的交易日历查询。
 *
 * @note 本接口只定义查询 contract：`is_holiday` 判断指定模板日期是否休市，
 *       `next_trading_date` 返回指定模板下的下一交易日；数据来源由实现方决定。
 */
class IHolidayProvider {
public:
    virtual ~IHolidayProvider() = default;
    /// @brief date(YYYYMMDD) 是否节假日（含周末与否由 ifWeekend 决定）。
    [[nodiscard]] virtual bool is_holiday(const std::string& trdtpl,
                                          uint32_t date,
                                          bool ifWeekend) const = 0;
    /// @brief 给定日期 + period type 返回下个交易日（节假日顺延）。
    [[nodiscard]] virtual uint32_t next_trading_date(const std::string& trdtpl,
                                                     uint32_t curDate) const = 0;
};

/**
 * @brief 提供 selector task 的 caller-driven 调度接口。
 *
 * 调用方按自己的 clock/timer 调用 `on_minute_end()`。
 * scheduler 只判定应触发任务并在调用线程执行 FireCallback；不创建线程，
 * 也不持有执行循环。
 */
class ISelTaskScheduler {
public:
    virtual ~ISelTaskScheduler() = default;
    using FireCallback = std::function<void(const SelTaskInfo& task,
                                            uint32_t curDate,
                                            uint32_t curTime,
                                            int64_t fireTime_ms)>;
    /// @brief 每分钟末调用：检测哪些 task 在此刻应触发，调 FireCallback。
    /// @note 检查包含 holiday rollover 与同日去重；FireCallback 在调用线程内执行。
    virtual void on_minute_end(uint32_t curDate, uint32_t curTime, FireCallback cb) = 0;
    /// @brief 注册 task。
    virtual uint32_t add_task(SelTaskInfo info) = 0;
    /// @brief 遍历 tasks（debug / inspection）。
    [[nodiscard]] virtual std::vector<SelTaskInfo> list_tasks() const = 0;
};

/**
 * @brief 提供以内存任务表保存 SelTaskInfo 的 selector scheduler。
 *
 * @note Daily/Weekly/Monthly/Yearly task 通过 `last_exe_date` 做同日去重；
 *       Minute 周期每分钟都可触发。
 */
class SelTaskScheduler : public ISelTaskScheduler {
public:
    explicit SelTaskScheduler(const IHolidayProvider* holiday_provider);

    uint32_t add_task(SelTaskInfo info) override;
    [[nodiscard]] std::vector<SelTaskInfo> list_tasks() const override;
    void on_minute_end(uint32_t curDate, uint32_t curTime, FireCallback cb) override;

private:
    /// @brief 判断单个 task 在 (curDate, curTime) 是否应触发（含 holiday rollover）。
    [[nodiscard]] bool should_fire_(SelTaskInfo& task,
                                    uint32_t curDate,
                                    uint32_t curTime) const;

    const IHolidayProvider* holiday_provider_ = nullptr;
    std::vector<SelTaskInfo> tasks_;
    uint32_t next_task_id_ = 1;
};

}  // namespace xt_engine_sel
