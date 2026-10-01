#!/usr/bin/env bash
# Ubuntu 22.04 / GLIBC 2.35，显式构建公开 targets 与插件。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${XT_OSS_BUILD_DIR:-$HOME/builds/xyybtrader-oss-dev-jammy}"
cd "$ROOT/src"
cmake --preset oss-dev-jammy -B "$BUILD" "$@"
cmake --build "$BUILD" --target XtRunner XtBackTestRunner XtSelBacktestRunner \
    xt_public_tests hft_tick_revert_strategy sel_weights_csv_strategy -j "${XT_BUILD_JOBS:-4}"
cd "$ROOT/var/plugins/strategies"
sha256sum libhft_tick_revert_strategy.so > libhft_tick_revert_strategy.so.sha256
sha256sum libsel_weights_csv_strategy.so > libsel_weights_csv_strategy.so.sha256
