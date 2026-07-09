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

    bool start(uint16_t port,
               std::string_view ring_label,
               std::string_view bind_addr = "127.0.0.1");

    void stop();

    bool is_running() const noexcept;

    void set_up(bool up);

    void update_ring_head(uint64_t ring_head);

    void set_check_health(bool healthy);

    void set_uptime_seconds(double seconds);

    void set_epoch(uint64_t epoch);

private:
    // 隐藏 prometheus-cpp 类型，避免内部 metric family 布局外露到 public header。
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}
