#!/usr/bin/env python3
"""convert_ticks.py — vendor tick 数据接入工具（纯 stdlib）。

把常见 vendor 日度打包的 tick CSV（每合约每交易日一个文件，CTP 口径列名）整理成
项目数据目录树，并可生成可直接运行的回测 config：

    每日 zip（内含 <合约>_<day>.csv，带或不带日期子目录）或散装 CSV
        │  校验表头（引擎别名映射口径）+ 行抽样校验
        ▼
    <out>/<EXCHANGE>/<品种>/<合约>/<YYYY-MM-DD>.csv     ← 内容逐字节原样拷贝
        │
        ▼  --emit-config
    quickstart 回测 yaml（datasource.files 绝对路径 + so 按 config 目录相对解析 +
    同目录 market.yaml 兜底）

重要：引擎的 CSV reader 做表头**别名映射**（InstrumentID≡code、UpperLimitPrice≡
UpperLimit 等），vendor 列名多数直接可读，**本工具不改写任何数据行**——只做
组织、校验和 config 生成。列语义与运行须知见 docs/data_format.md。

用法：
    # 一个目录的日度 zip → 数据树（交易所可自动推断常见品种，否则 --exchange）
    python3 tools/convert_ticks.py --src ~/Downloads/202601 --out data/ticks

    # 只要某些合约 + 顺手生成可跑的回测 config
    python3 tools/convert_ticks.py --src ~/Downloads/202601 --out data/ticks \
        --contracts AP605 --emit-config configs/backtest_ap605.yaml
"""

import argparse
import csv
import io
import re
import zipfile
from pathlib import Path

# ── 引擎表头别名（与 src/xt_backtest/util/TickCsvUtil.cpp build_tick_spec_from_header 同步）──
ALIAS = {
    "instrumentid": "instrument", "code": "instrument",
    "tradingday": "trading_day",
    "updatetime": "update_time", "updatetimems": "update_time",
    "updatemillisec": "update_ms",
    "lastprice": "last_price", "price": "last_price",
    "volume": "volume", "turnover": "turnover", "openinterest": "open_interest",
    "upperlimit": "upper_limit", "upperlimitprice": "upper_limit",
    "lowerlimit": "lower_limit", "lowerlimitprice": "lower_limit",
    "highprice": "high", "lowprice": "low",
}
for i in range(1, 6):
    for side in ("bid", "ask"):
        ALIAS[f"{side}price{i}"] = f"{side}_price{i}"
        ALIAS[f"{side}volume{i}"] = f"{side}_qty{i}"

# 本工具的接入 gate（引擎硬校验更宽：只有合约代码 + LastPrice 缺失才丢行，其余静默 0；
# 但缺 trading_day/update_time 的数据排序 key 退化，缺 volume 的量能逻辑全 0——按可用性挡下）。
REQUIRED = ["instrument", "trading_day", "update_time", "last_price", "volume"]
RECOMMENDED = ["bid_price1", "ask_price1", "bid_qty1", "ask_qty1",
               "turnover", "open_interest"]

# 常见品种 → 交易所（树布局用；引擎不读 ExchangeID）。不全则 --exchange 指定。
PRODUCT_EXCHANGE = {
    # SHFE
    "ag": "SHFE", "au": "SHFE", "cu": "SHFE", "al": "SHFE", "zn": "SHFE",
    "ni": "SHFE", "rb": "SHFE", "hc": "SHFE", "ru": "SHFE", "sn": "SHFE", "pb": "SHFE",
    # INE
    "sc": "INE", "lu": "INE", "nr": "INE",
    # DCE
    "i": "DCE", "j": "DCE", "jm": "DCE", "m": "DCE", "y": "DCE", "p": "DCE",
    "c": "DCE", "cs": "DCE", "a": "DCE", "b": "DCE", "l": "DCE", "v": "DCE",
    "pp": "DCE", "eg": "DCE", "eb": "DCE", "pg": "DCE",
    # CZCE（品种大写）
    "AP": "CZCE", "CF": "CZCE", "SR": "CZCE", "TA": "CZCE", "MA": "CZCE",
    "FG": "CZCE", "SA": "CZCE", "RM": "CZCE", "OI": "CZCE", "SF": "CZCE",
    "SM": "CZCE", "UR": "CZCE", "PK": "CZCE", "CJ": "CZCE",
}

FNAME_RE = re.compile(r"^([A-Za-z]+\d+)_(\d{8})\.csv$")


def norm_col(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.strip().lower())


def split_contract(contract: str):
    m = re.match(r"^([A-Za-z]+)(\d+)$", contract)
    return (m.group(1), m.group(2)) if m else (contract, "")


def lookup_exchange(product: str, explicit: str) -> str:
    if explicit:
        return explicit
    for key in (product, product.upper(), product.lower()):
        if key in PRODUCT_EXCHANGE:
            return PRODUCT_EXCHANGE[key]
    return ""


def validate_csv(text_stream, contract: str, day: str, path_label: str):
    """校验表头覆盖接入 gate 列 + 抽样首行数据一致性。返回 (行数, warnings)。"""
    warnings = []
    reader = csv.reader(text_stream)
    try:
        header = next(reader)
    except StopIteration:
        return 0, [f"{path_label}: 空文件"]
    mapped = {ALIAS.get(norm_col(c)) for c in header}
    mapped.discard(None)

    missing = [c for c in REQUIRED if c not in mapped]
    if missing:
        raise SystemExit(f"❌ {path_label}: 缺接入必需列 {missing}（表头别名映射后）。"
                         f" 表头={header}")
    for c in RECOMMENDED:
        if c not in mapped:
            warnings.append(f"{path_label}: 缺推荐列 {c}"
                            f"（引擎会回退：盘口缺→last_price/qty_base，其余按 0）")

    idx = {ALIAS.get(norm_col(c)): i for i, c in enumerate(header)}
    n = 0
    for row in reader:
        if not row:
            continue
        if n == 0:
            inst = row[idx["instrument"]].strip()
            tday = row[idx["trading_day"]].strip()
            if inst != contract:
                warnings.append(f"{path_label}: 首行 InstrumentID={inst} 与文件名合约 {contract} 不一致")
            if tday != day:
                warnings.append(f"{path_label}: 首行 TradingDay={tday} 与文件名日期 {day} 不一致")
        n += 1
    return n, warnings


def iter_sources(src: Path):
    """产出 (contract, day, data_bytes, label)。支持日度 zip（两种内部布局）与散装 CSV。"""
    for z in sorted(src.glob("*.zip")):
        with zipfile.ZipFile(z) as zf:
            for info in sorted(zf.namelist()):
                m = FNAME_RE.match(Path(info).name)
                if not m or info.endswith("/"):
                    continue
                yield m.group(1), m.group(2), zf.read(info), f"{z.name}:{info}"
    for c in sorted(src.glob("*.csv")):
        m = FNAME_RE.match(c.name)
        if m:
            yield m.group(1), m.group(2), c.read_bytes(), str(c)


def emit_config(path: Path, product: str, contract: str, files: list, days: list):
    cfg_dir = path.resolve().parent
    # 引擎对 strategies[].so 做 config-目录相对解析；datasource files 是 cwd 相对
    # （此处写绝对路径规避）。详见 docs/data_format.md §5。
    so_abs = (Path.cwd() / "var/plugins/strategies/libhft_tick_revert_strategy.so").resolve()
    try:
        so_rel = so_abs.relative_to(cfg_dir)
        so_path = str(so_rel)
    except ValueError:
        import os
        so_path = os.path.relpath(so_abs, cfg_dir)
    file_lines = "\n".join(f"        - {Path(f).resolve()}" for f in files)
    tpl = f"""# 由 tools/convert_ticks.py 生成的 quickstart 回测 config
# 运行（四件事都必须，详见 docs/data_format.md「运行须知」）：
#   CLOUDTRADER_PLUGIN_ALLOWLIST=libhft_tick_revert_strategy.so \\
#     ./build/XtBackTestRunner --config {path}
#   1) config 必须用 --config 传（裸位置参数会被静默忽略并回退搜索默认 profile）
#   2) 插件 .so 必须在 CLOUDTRADER_PLUGIN_ALLOWLIST，且旁边有匹配的 .so.sha256
#      （loader 默认 strict signing：sidecar 缺失/不匹配都拒载；构建说明含生成命令）
#   3) 本 config 同目录必须有 market.yaml（本工具已兜底生成空 {{}}；若目录里已有
#      内容非空的 market.yaml，其字段会 deep-merge 覆盖本文件同路径字段）
#   4) report_dir 会自动追加时间戳后缀（report → report_<ts>/）
log:
  level: info
  console: false
commodities:
  {product}:
    vol_scale: 10.0        # TODO: 改成该品种真实合约乘数
    price_tick: 1.0        # TODO: 改成该品种真实最小变动价位
fees:
  {product}: {{open: 0, close: 0, close_today: 0, by_volume: false}}
backtest:
  start_date: {days[0]}
  end_date:   {days[-1]}
  report_dir: /tmp/xt_bt_{contract}
  latency_model:
    type: constant
    entry_us: 0
    response_us: 0
  queue_model:
    type: ratio
    ratio: 0.0
  match_delay_ticks: 2
  session_offset_minutes: 300
  roll:
    active: false          # 单合约回测显式关掉换月最稳妥
  session:
    enabled: true
    sections:              # TODO: 按品种交易时段调整；夜盘品种加 "21:00:00-02:30:00" 等
      - "09:00:00-10:15:00"
      - "10:30:00-11:30:00"
      - "13:30:00-15:00:00"
  fill:
    split: false
    min_qty: 1.0
    max_qty: 1000.0
    qty_tick: 1.0
  account:
    cash: 10000000
  cost:
    slippage: 0
  risk:
    max_order_qty: 10000
    max_order_notional: 0
    max_pos_qty: 10000
    allow_short: true
  datasource:
    mode: tick
    data_mode: memory
    tick:
      files:
{file_lines}
      symbols:
        - {contract}
      volume_mode: cumulative   # vendor Volume 为当日累计量时用 cumulative；逐笔增量用 incremental
      use_bin_cache: false
  strategies:
    - id: hft_tick_revert
      so: {so_path}
      symbol: {contract}
      tick_window: 10
      qty: 1
      use_ioc: 0
      history_ticks: 256
      history_bars: 64
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(tpl, encoding="utf-8")
    # market.yaml 兜底：config 同目录缺失会拒启动；已存在则不动（内容会 merge 覆盖，注释已提示）。
    market = cfg_dir / "market.yaml"
    if not market.exists():
        market.write_text("{}\n", encoding="utf-8")
        return True
    return False


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", required=True, help="vendor 数据目录（日度 zip 或散装 CSV）")
    ap.add_argument("--out", required=True, help="输出数据树根（如 data/ticks）")
    ap.add_argument("--exchange", default="", help="交易所代码（品种不在内置映射时必填）")
    ap.add_argument("--contracts", default="", help="只处理这些合约（逗号分隔），默认全部")
    ap.add_argument("--emit-config", default="", help="生成 quickstart 回测 yaml 到此路径（需 --contracts 单合约）")
    args = ap.parse_args()

    src, out = Path(args.src).expanduser(), Path(args.out)
    if not src.is_dir():
        raise SystemExit(f"❌ --src 不是目录: {src}")
    only = {c.strip() for c in args.contracts.split(",") if c.strip()}

    written, all_warn = [], []
    for contract, day, data, label in iter_sources(src):
        if only and contract not in only:
            continue
        product, _ = split_contract(contract)
        exchange = lookup_exchange(product, args.exchange)
        if not exchange:
            raise SystemExit(f"❌ 品种 {product} 不在内置交易所映射，请加 --exchange")

        n, warns = validate_csv(io.TextIOWrapper(io.BytesIO(data), encoding="utf-8-sig"),
                                contract, day, label)
        all_warn.extend(warns)

        dst = out / exchange / product / contract / f"{day[:4]}-{day[4:6]}-{day[6:]}.csv"
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(data)          # 原样拷贝，不改写数据行
        written.append((str(dst), contract, day, n))
        print(f"  {label} → {dst}  ({n} rows)")

    if not written:
        raise SystemExit("❌ 没有匹配的输入文件（期望 <合约>_<YYYYMMDD>.csv 命名）")
    for w in all_warn:
        print(f"  ⚠ {w}")
    print(f"✅ {len(written)} 个文件进树，{len(all_warn)} 条警告")

    if args.emit_config:
        contracts = sorted({c for _, c, _, _ in written})
        if len(contracts) != 1:
            raise SystemExit(f"❌ --emit-config 需要单合约（--contracts），当前: {contracts}")
        contract = contracts[0]
        product, _ = split_contract(contract)
        files = [p for p, c, _, _ in written if c == contract]
        days = sorted({d for _, c, d, _ in written if c == contract})
        wrote_market = emit_config(Path(args.emit_config), product, contract, files, days)
        print(f"✅ config 已生成: {args.emit_config}（先改 TODO 项：vol_scale/price_tick/session）"
              + ("；同目录已兜底空 market.yaml" if wrote_market else ""))


if __name__ == "__main__":
    main()
