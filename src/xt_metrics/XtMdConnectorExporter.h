#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

namespace ct::metrics {

/// @brief MD connector 进程专用的轻量 Prometheus exporter。
///
/// connector 是独立进程，不跑 runner 的 XtMetricRegistry；它自带一个小
/// registry 和 /metrics endpoint，只发布 xt_md_connector_*。
/// connector 主环约 1Hz 调 setter 提交状态；runner 交易指标不经此 exporter。
class XtMdConnectorExporter {
public:
    XtMdConnectorExporter();
    ~XtMdConnectorExporter();
    XtMdConnectorExporter(const XtMdConnectorExporter&) = delete;
    XtMdConnectorExporter& operator=(const XtMdConnectorExporter&) = delete;

    /// @brief 启动 HTTP /metrics endpoint 并注册 connector families。
    /// @param port       监听端口（topology md_connector.metrics_port，如 9120）。
    /// @param ring_label 共享内存 ring 名；作为 Prometheus `ring` label 暴露，
    ///                   必须与 reader / panel 使用的 connector 标识一致。
    /// @param bind_addr  监听地址；生产默认 127.0.0.1（prom-agent 本机 scrape）。
    /// @return true=启动成功；false=已启动或 bind 失败（connector 不因此退出，仅
    ///         降级无 metrics）。
    bool start(uint16_t port,
               std::string_view ring_label,
               std::string_view bind_addr = "127.0.0.1");

    /// @brief 停止 exposer（幂等；析构亦调）。停止前置 up=0。
    void stop();

    /// @brief 是否正在 serve /metrics。
    bool is_running() const noexcept;

    // ── 周期性快照（connector ~1Hz 主环调用）─────────────────────────────

    /// @brief 设 xt_md_connector_up（1=serving）。start() 成功后置 1，异常态置 0。
    void set_up(bool up);

    /// @brief 更新 connector ring 发布进度指标。
    ///
    /// ring.head() 写入 xt_md_connector_ring_head，表示 connector 发布到共享内存
    /// ring 的绝对位置。head 增量累加到 xt_md_connector_published_ticks_total，
    /// 供 Prometheus 用 `rate(...)` 计算广播 tick/s。
    ///
    /// 首次调用只建立基准；head 回退时不累加 counter，只更新 gauge。
    void update_ring_head(uint64_t ring_head);

    /// @brief 设 xt_md_connector_check_health（1=健康，0=stale/重连中）。
    ///
    /// false 会累加 xt_md_connector_check_health_fail_total。panel 用
    /// `rate(...) > 0` 观测 stale/reconnect 自愈窗口。
    /// @note 值 1 也覆盖「窗外 offhours 抑制」——窗外 check_health 一律返 true（不评估），
    /// 用 set_stale_watchdog_active 区分「真健康」与「offhours 未评估」。
    void set_check_health(bool healthy);

    /// @brief 设 xt_md_connector_stale_watchdog_active（1=stale watchdog armed，
    /// 0=expected-tick 窗外抑制）。connector ~1Hz 计算窗内/外并 emit；gate 未启用
    /// （binance / gate disabled）恒 1。与 check_health 配对，让「窗外静默」可归因。
    void set_stale_watchdog_active(bool active);

    /// @brief 设 xt_md_connector_uptime_seconds。
    void set_uptime_seconds(double seconds);

    /// @brief 设 xt_md_connector_epoch（info gauge；段创建 steady ns，reader 换代可见性）。
    void set_epoch(uint64_t epoch);

private:
    // 隐藏 prometheus-cpp 类型，避免内部 metric family 布局外露到 public header。
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace ct::metrics
