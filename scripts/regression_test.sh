#!/bin/bash
# 完整回归入口：6 个 HFT 基线场景全部 byte-identical 才算过。
# 细节与单场景用法见 scripts/run_baselines.sh。
set -euo pipefail
exec bash "$(dirname "$0")/run_baselines.sh" "$@"
