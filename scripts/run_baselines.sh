#!/usr/bin/env bash
# 可按场景单独运行 HFT byte-identical 基线。
set -euo pipefail
exec python3 "$(dirname "$0")/validate_dev.py" --regression-only --hft "$@"
