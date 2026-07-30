#!/bin/bash
# HFT 回归基线 runner —— 6 个场景与仓内 golden 做 byte-identical 比对。
#
# 用法：
#   bash scripts/run_baselines.sh                # 跑全部 6 个场景
#   bash scripts/run_baselines.sh ag2206_only    # 只跑一个
#
# 回测器查找顺序：环境变量 XT_RUNNER_BIN > build/XtBackTestRunner > build*/XtBackTestRunner。
# 判据：每个场景的 fills.csv 与 data/regression/baselines/<场景>/expected_fills.csv
# 逐字节一致（diff bit-exact）。任何 drift / 失败 → exit 非 0。
#
# 前置：已按 README 构建，且 var/plugins/strategies/libhft_tick_revert_strategy.so 就位。

set -euo pipefail
cd "$(dirname "$0")/.."
PROJECT_DIR="$(pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

ALL_SCENARIOS=(ag2206_only ag2212_only roll_29col holiday ag2206_vollimit ag2206_vollimit_split)

SCENARIOS=()
for arg in "$@"; do
    case "$arg" in
        ag2206_only|ag2212_only|roll_29col|holiday|ag2206_vollimit|ag2206_vollimit_split)
            SCENARIOS+=("$arg") ;;
        *) echo "unknown scenario: $arg (可选: ${ALL_SCENARIOS[*]})"; exit 2 ;;
    esac
done
if [ ${#SCENARIOS[@]} -eq 0 ]; then
    SCENARIOS=("${ALL_SCENARIOS[@]}")
fi

stamp_ns() { date +%s%N 2>/dev/null || printf '%s000000000' "$(date +%s)"; }
extract_metric() { sed -n "s/.*$1=\([0-9][0-9]*\).*/\1/p" | tail -1; }
sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

# ── 定位回测器 ──────────────────────────────────────────────────────────
RUNNER="${XT_RUNNER_BIN:-}"
if [ -z "$RUNNER" ]; then
    for cand in "$PROJECT_DIR/build/XtBackTestRunner" "$PROJECT_DIR"/build*/XtBackTestRunner; do
        if [ -x "$cand" ]; then RUNNER="$cand"; break; fi
    done
fi
if [ -z "$RUNNER" ] || [ ! -x "$RUNNER" ]; then
    echo -e "${RED}❌ 找不到 XtBackTestRunner。先按 README 构建，或用 XT_RUNNER_BIN 指定路径${NC}" >&2
    exit 1
fi

# ── 插件与安全配置（loader 默认 strict signing + allowlist） ─────────────
SO="$PROJECT_DIR/var/plugins/strategies/libhft_tick_revert_strategy.so"
if [ ! -f "$SO" ]; then
    echo -e "${RED}❌ 策略插件缺失: $SO（见 README 构建第 3 步）${NC}" >&2
    exit 1
fi
printf '%s  %s\n' "$(sha256_of "$SO")" "$(basename "$SO")" > "$SO.sha256"
export CLOUDTRADER_PLUGIN_ALLOWLIST_ENFORCE=1
export CLOUDTRADER_PLUGIN_STRICT_SIGNING=1
export CLOUDTRADER_PLUGIN_ALLOWLIST=libhft_tick_revert_strategy.so

SELF_BASE="$PROJECT_DIR/data/regression/baselines"
echo "runner: $RUNNER"
echo ""

PASS=0
FAIL=0
DRIFT=0

for scn in "${SCENARIOS[@]}"; do
    CFG="$PROJECT_DIR/configs/baselines/${scn}.yaml"
    if [ ! -f "$CFG" ]; then
        echo -e "${RED}❌ MISSING: config $CFG${NC}"
        FAIL=$((FAIL + 1))
        continue
    fi

    TS=$(stamp_ns)
    REPORT_DIR="/tmp/xt_baseline_${scn}_${TS}"
    # per-run 临时目录：config 与隔离用的空 market.yaml 同目录（loader 在 config
    # 旁查找 market.yaml），避免多用户/并发 job 共享 /tmp/market.yaml 互踩。
    TMPD=$(mktemp -d "/tmp/xt_baseline_cfg_${scn}.XXXXXX")
    TMP_CFG="$TMPD/backtest.yaml"
    sed -e "s|\${PROJECT_DIR}|${PROJECT_DIR}|g" \
        -e "s|/tmp/xt_baseline_${scn}|${REPORT_DIR}|" "$CFG" > "$TMP_CFG"
    echo '{}' > "$TMPD/market.yaml"
    rm -rf "$PROJECT_DIR/data/ticks/cache"

    printf "=== %s ===\n" "$scn"
    EXIT_CODE=0
    OUTPUT=$("$RUNNER" --config "$TMP_CFG" 2>&1) || EXIT_CODE=$?
    rm -rf "$TMPD"

    if [ $EXIT_CODE -ne 0 ]; then
        echo -e "  ${RED}❌ runner failed (exit=$EXIT_CODE)${NC}"
        echo "$OUTPUT" | tail -2
        FAIL=$((FAIL + 1))
        continue
    fi

    TRADES=$(printf '%s\n' "$OUTPUT" | extract_metric trades)
    [ -n "$TRADES" ] || TRADES="?"

    REAL=$(ls -td "${REPORT_DIR}"* 2>/dev/null | head -1) || true
    if [ -z "$REAL" ] || [ ! -f "$REAL/fills.csv" ]; then
        echo -e "  ${RED}❌ no fills.csv${NC}"
        FAIL=$((FAIL + 1))
        continue
    fi

    # 注：golden 缺失计 FAIL（比内部版更严，刻意——公开仓的信任锚不允许静默跳过）。
    EXPECTED="$SELF_BASE/$scn/expected_fills.csv"
    if [ ! -f "$EXPECTED" ]; then
        echo -e "  ${YELLOW}⚠ golden missing: $EXPECTED${NC}"
        echo "    xt_trades=$TRADES (no golden to compare)"
        rm -rf "$REAL"
        FAIL=$((FAIL + 1))
        continue
    fi
    EXP_LINES=$(wc -l < "$EXPECTED")
    ACT_LINES=$(wc -l < "$REAL/fills.csv")
    if diff -q "$EXPECTED" "$REAL/fills.csv" > /dev/null 2>&1; then
        echo -e "  ${GREEN}✅ BIT-EXACT  (trades=$TRADES lines=$ACT_LINES)${NC}"
        PASS=$((PASS + 1))
    else
        DIFFLINES=$(diff "$EXPECTED" "$REAL/fills.csv" 2>/dev/null | wc -l || echo 0)
        echo -e "  ${YELLOW}Δ DRIFT  trades=$TRADES (expected $((EXP_LINES-1))), diff_lines=$DIFFLINES${NC}"
        DRIFT=$((DRIFT + 1))
    fi

    rm -rf "$REAL"
done

echo ""
# %b 而非 %s：颜色常量是 '\033[...]' 字面量，%s 会把转义序列原样打出来
# （2026-07-29 发布预演实测：终判行显示成 "\033[0;32mPASS=6\033[0m"）。
printf "Results: %bPASS=%d%b  %bDRIFT=%d%b  %bFAIL=%d%b\n" \
    "$GREEN" "$PASS" "$NC" "$YELLOW" "$DRIFT" "$NC" "$RED" "$FAIL" "$NC"
if [ $FAIL -gt 0 ] || [ $DRIFT -gt 0 ]; then exit 1; fi
exit 0
