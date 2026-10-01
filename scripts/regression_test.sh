#!/usr/bin/env bash
# HFT 六场景与 SEL 三组 golden；临时配置/报告由 Python finally 清理。
set -euo pipefail
exec python3 "$(dirname "$0")/validate_dev.py" --regression-only "$@"
