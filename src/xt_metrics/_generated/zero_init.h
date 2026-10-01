// AUTO-GENERATED metrics zero-init contract — DO NOT EDIT by hand.
// Source: observability metrics registry (metrics.yaml)
//
// Runner is expected to include this header from XtMetricRegistry and
// force-register every entry below at startup (zero observation),
// so that Prometheus scrape always exposes the family. Bucket schema
// hashes published as xt_runner_bucket_schema_info for Gate 1 cross-check.

#pragma once

#include <array>
#include <string_view>

namespace xt::metrics::contract {

inline constexpr std::string_view kManifestVersion = "2026.05.26-1";

enum class MetricType { Gauge, Counter, Histogram };

// Bucket family descriptors (referenced by histogram entries).
struct BucketFamily {
    std::string_view name;
    std::string_view version;
    std::string_view hash;       // sha256[:16] of canonical_json
    std::array<double, 16> buckets;
    std::size_t bucket_count;
};

inline constexpr std::array<BucketFamily, 3> kBucketFamilies = {{
    BucketFamily{"order_us", "2026.05.26-2", "1c445810527da52e", {{50, 100, 500, 1000, 2500, 5000, 7500, 10000, 15000, 25000, 50000, 100000, 250000, 500000, 1000000, 0.0}}, 15},
    BucketFamily{"md_us", "2026.05.26-1", "2059563eb120b723", {{1000, 1500, 2000, 3000, 5000, 8000, 13000, 20000, 35000, 60000, 100000, 250000, 0.0, 0.0, 0.0, 0.0}}, 12},
    BucketFamily{"rpc_us", "2026.05.26-1", "fd5e50d702df24ad", {{100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000, 100000, 250000, 1000000, 0.0, 0.0, 0.0, 0.0}}, 12},
}};

// Metrics that must be force-registered at runner startup
// (zero observation) so Prometheus exposition is non-lazy.
struct ZeroInitMetric {
    std::string_view name;
    MetricType type;
    std::string_view help;
    std::array<std::string_view, 8> labels;
    std::size_t label_count;
    std::string_view buckets_ref;  // empty for non-histogram
};

inline constexpr std::array<ZeroInitMetric, 5> kZeroInitMetrics = {{
    ZeroInitMetric{"xt_runner_contract_version_info", MetricType::Gauge, "Runner manifest version (info gauge, value always 1; version label matches manifest_version)", {{"version", "", "", "", "", "", "", ""}}, 1, ""},
    ZeroInitMetric{"xt_runner_bucket_schema_info", MetricType::Gauge, "Bucket schema fingerprint (sha256[:16] over canonical_json({family,bucket_version,buckets_sorted})); cross-checked by Gate 1 against yaml-computed hash", {{"family", "bucket_version", "hash", "", "", "", "", ""}}, 3, ""},
    ZeroInitMetric{"xt_submits_total", MetricType::Counter, "Total order submits emitted by runner", {{"strategy", "book", "symbol", "side", "", "", "", ""}}, 4, ""},
    ZeroInitMetric{"xt_submit_to_ack_us", MetricType::Histogram, "Submit→Ack latency distribution (μs); query via histogram_quantile()", {{"strategy", "book", "symbol", "", "", "", "", ""}}, 3, "order_us"},
    ZeroInitMetric{"xt_submit_to_fill_us", MetricType::Histogram, "Submit→Fill latency distribution (μs); query via histogram_quantile()", {{"strategy", "book", "symbol", "", "", "", "", ""}}, 3, "order_us"},
}};

}  // namespace xt::metrics::contract
