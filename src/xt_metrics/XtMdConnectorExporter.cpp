#include "xt_metrics/XtMdConnectorExporter.h"

#include <exception>
#include <string>

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>

namespace ct::metrics {

struct XtMdConnectorExporter::Impl {
    std::shared_ptr<prometheus::Registry> registry;
    std::unique_ptr<prometheus::Exposer> exposer;
    bool running = false;

    uint64_t last_head = 0;
    bool have_last_head = false;

    prometheus::Gauge* up = nullptr;
    prometheus::Gauge* ring_head = nullptr;
    prometheus::Counter* published_total = nullptr;
    prometheus::Gauge* check_health = nullptr;
    prometheus::Counter* check_health_fail_total = nullptr;
    prometheus::Gauge* uptime = nullptr;
    prometheus::Gauge* epoch = nullptr;
};

XtMdConnectorExporter::XtMdConnectorExporter() : impl_(std::make_unique<Impl>()) {}

XtMdConnectorExporter::~XtMdConnectorExporter() { stop(); }

bool XtMdConnectorExporter::start(uint16_t port,
                                  std::string_view ring_label,
                                  std::string_view bind_addr) {
    if (impl_->running) return false;
    try {

        impl_->registry = std::make_shared<prometheus::Registry>();

        const prometheus::Labels lbl{{"ring", std::string(ring_label)}};

        auto& fam_up = prometheus::BuildGauge()
            .Name("xt_md_connector_up")
            .Help("MD connector exposer serving (1=up, 0=stopping)")
            .Register(*impl_->registry);
        impl_->up = &fam_up.Add(lbl);

        auto& fam_head = prometheus::BuildGauge()
            .Name("xt_md_connector_ring_head")
            .Help("Current shm ring head (absolute monotonic ticks published)")
            .Register(*impl_->registry);
        impl_->ring_head = &fam_head.Add(lbl);

        auto& fam_pub = prometheus::BuildCounter()
            .Name("xt_md_connector_published_ticks_total")
            .Help("Total ticks published to the shm ring (rate() = broadcast tick/s)")
            .Register(*impl_->registry);
        impl_->published_total = &fam_pub.Add(lbl);

        auto& fam_health = prometheus::BuildGauge()
            .Name("xt_md_connector_check_health")
            .Help("Gateway check_health result (1=healthy, 0=stale/reconnecting)")
            .Register(*impl_->registry);
        impl_->check_health = &fam_health.Add(lbl);

        auto& fam_health_fail = prometheus::BuildCounter()
            .Name("xt_md_connector_check_health_fail_total")
            .Help("check_health=false occurrences (stale/reconnect proxy; rate()>0 = MD self-healing)")
            .Register(*impl_->registry);
        impl_->check_health_fail_total = &fam_health_fail.Add(lbl);

        auto& fam_uptime = prometheus::BuildGauge()
            .Name("xt_md_connector_uptime_seconds")
            .Help("Seconds since connector exposer start")
            .Register(*impl_->registry);
        impl_->uptime = &fam_uptime.Add(lbl);

        auto& fam_epoch = prometheus::BuildGauge()
            .Name("xt_md_connector_epoch")
            .Help("Connector epoch (steady ns at segment create; reader re-attach generation)")
            .Register(*impl_->registry);
        impl_->epoch = &fam_epoch.Add(lbl);

        const std::string endpoint = std::string(bind_addr) + ":" + std::to_string(port);

        impl_->exposer = std::make_unique<prometheus::Exposer>(endpoint);
        impl_->exposer->RegisterCollectable(impl_->registry);

        impl_->up->Set(1.0);
        impl_->running = true;
        return true;
    } catch (const std::exception&) {
        impl_->exposer.reset();
        impl_->registry.reset();
        impl_->running = false;
        return false;
    }
}

void XtMdConnectorExporter::stop() {
    if (!impl_->running) return;
    if (impl_->up) impl_->up->Set(0.0);
    impl_->exposer.reset();
    impl_->running = false;
}

bool XtMdConnectorExporter::is_running() const noexcept { return impl_->running; }

void XtMdConnectorExporter::set_up(bool up) {
    if (!impl_->running || !impl_->up) return;
    impl_->up->Set(up ? 1.0 : 0.0);
}

void XtMdConnectorExporter::update_ring_head(uint64_t head) {
    if (!impl_->running) return;
    if (impl_->ring_head) impl_->ring_head->Set(static_cast<double>(head));
    if (impl_->published_total && impl_->have_last_head && head >= impl_->last_head) {
        impl_->published_total->Increment(static_cast<double>(head - impl_->last_head));
    }
    impl_->last_head = head;
    impl_->have_last_head = true;
}

void XtMdConnectorExporter::set_check_health(bool healthy) {
    if (!impl_->running) return;
    if (impl_->check_health) impl_->check_health->Set(healthy ? 1.0 : 0.0);
    if (!healthy && impl_->check_health_fail_total) impl_->check_health_fail_total->Increment();
}

void XtMdConnectorExporter::set_uptime_seconds(double seconds) {
    if (!impl_->running || !impl_->uptime) return;
    impl_->uptime->Set(seconds);
}

void XtMdConnectorExporter::set_epoch(uint64_t epoch_ns) {
    if (!impl_->running || !impl_->epoch) return;
    impl_->epoch->Set(static_cast<double>(epoch_ns));
}

}
