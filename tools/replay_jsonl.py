#!/usr/bin/env python3
"""replay_jsonl.py — 把 live 录制 (recordings/live.jsonl) 的行情转成可回测的 tick CSV。

「录了就能回放」的一条命令入口：

    python3 tools/replay_jsonl.py \
        --jsonl var/live_runs/ctp/recordings/live.jsonl \
        --out data/replay --emit-config configs/replay_ag2612.yaml

    ./build/XtBackTestRunner --config configs/replay_ag2612.yaml

- 输入支持多个 --jsonl（含轮转出的 live.jsonl.<epoch>），按事件时间归并；
- 只消费 MD_TICK 事件，其余（委托/成交/连接）忽略；
- 输出与 docs/data_format.md 同一列口径（CTP vendor 别名，L5 盘口，Volume 为
  当日累计 → volume_mode: cumulative）；
- 单位换算：recorder JSONL 的盘口量 bid_vol/ask_vol 是定点 ×1e6
  （XtMdTickRecorderUtil::fill_md_depth_fp），两 venue 都换算回真实量；
  binance 的 volume 也是定点（CTP 为原始手数），按 exchange_id 区分处理；
- Turnover / OpenInterest 恒写 0（JSONL MD_TICK 不含），依赖持仓量的
  策略回放会失真；
- --emit-config 生成的回测 config 里，委托/成交由撮合引擎按录制行情重新模拟——
  回放成交不等于（也不应等于）实盘成交，用于策略行为复盘与撮合假设对照。
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

HEADER = ("code,ExchangeID,TradingDay,ActionDay,UpdateTime,UpdateMillisec,"
          "LastPrice,Volume,Turnover,OpenInterest,"
          "BidPrice1,BidPrice2,BidPrice3,BidPrice4,BidPrice5,"
          "AskPrice1,AskPrice2,AskPrice3,AskPrice4,AskPrice5,"
          "BidVolume1,BidVolume2,BidVolume3,BidVolume4,BidVolume5,"
          "AskVolume1,AskVolume2,AskVolume3,AskVolume4,AskVolume5")


def hhmmssmmm_split(action_time: int) -> tuple[str, int]:
    hhmmss, ms = divmod(int(action_time), 1000)
    return f"{hhmmss // 10000:02d}:{hhmmss // 100 % 100:02d}:{hhmmss % 100:02d}", ms


def fp_to_qty(v) -> "int | float":
    """recorder 定点量（×1e6）→ 真实量；整数值还原为 int 保持 CSV 紧凑。"""
    q = v / 1e6
    return int(q) if float(q).is_integer() else q


def tick_row(e: dict) -> str:
    t, ms = hhmmssmmm_split(e["action_time"])
    bid, ask = e.get("bid") or [], e.get("ask") or []
    bv, av = e.get("bid_vol") or [], e.get("ask_vol") or []
    lvl = lambda a, i: a[i] if i < len(a) else 0
    # binance 路径 volume 由 to_recorder_fp 定点化；CTP 写原始手数。
    vol = e["volume"]
    if e.get("exchange_id") == "BINANCE":
        vol = fp_to_qty(vol)
    cells = [e["code"], e.get("exchange_id", ""), e["trading_date"], e["action_date"], t, ms,
             e["last"], vol, 0, 0]
    cells += [lvl(bid, i) for i in range(5)] + [lvl(ask, i) for i in range(5)]
    cells += [fp_to_qty(lvl(bv, i)) for i in range(5)] + [fp_to_qty(lvl(av, i)) for i in range(5)]
    return ",".join(str(c) for c in cells)


def main() -> int:
    ap = argparse.ArgumentParser(description="live.jsonl MD_TICK → 回测 tick CSV")
    ap.add_argument("--jsonl", action="append", required=True,
                    help="录制文件, 可多次给（含轮转 live.jsonl.<epoch>）")
    ap.add_argument("--out", required=True, help="CSV 输出目录")
    ap.add_argument("--code", help="只导出该合约（默认全部）")
    ap.add_argument("--emit-config", help="顺手生成可跑的回测 config（单合约时可用）")
    args = ap.parse_args()

    ticks: dict[tuple[str, int], list[tuple[int, str]]] = defaultdict(list)
    dropped = 0
    for path in args.jsonl:
        with open(path, encoding="utf-8") as f:
            for line in f:
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    dropped += 1
                    continue
                if e.get("type") != "MD_TICK":
                    continue
                if args.code and e.get("code") != args.code:
                    continue
                key = (e["code"], int(e["action_date"]))
                ticks[key].append((int(e["ts_local_ns"]), tick_row(e)))
    if not ticks:
        print("no MD_TICK found — 检查 --jsonl 路径 / --code 过滤", file=sys.stderr)
        return 1

    out_root = Path(args.out)
    files: list[Path] = []
    dates: set[int] = set()
    codes: set[str] = set()
    for (code, day), rows in sorted(ticks.items()):
        rows.sort(key=lambda r: r[0])  # 录制本身单调, 排序兜底多文件归并
        p = out_root / code / f"{day}.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8", newline="\n") as f:
            f.write(HEADER + "\n")
            f.writelines(r[1] + "\n" for r in rows)
        print(f"  {p}  ticks={len(rows)}")
        files.append(p)
        dates.add(day)
        codes.add(code)
    if dropped:
        print(f"  (skipped {dropped} unparsable lines)")

    if args.emit_config:
        if len(codes) != 1:
            print("--emit-config 需要单合约（用 --code 过滤）", file=sys.stderr)
            return 1
        code = next(iter(codes))
        product = "".join(ch for ch in code if ch.isalpha())
        # 相对路径会按 config 所在目录解析（见 docs/data_format.md 运行须知）→ 全部绝对化
        so_path = Path("var/plugins/strategies/libhft_tick_revert_strategy.so").resolve()
        roll_path = Path("configs/roll/ag_roll_recon.path").resolve()
        cfg = f"""# 由 tools/replay_jsonl.py 生成：回放 live 录制行情（委托由撮合引擎重模拟）
log: {{level: info, console: false}}
commodities:
  {product}: {{vol_scale: 15.0, price_tick: 1.0}}   # 按品种实际值调整
fees:
  {product}: {{open: 0, close: 0, close_today: 0, by_volume: false}}
backtest:
  start_date: {min(dates)}
  end_date:   {max(dates)}
  report_dir: /tmp/xt_replay_{code}
  latency_model: {{type: constant, entry_us: 0, response_us: 0}}
  queue_model: {{type: ratio, ratio: 0.0}}
  queue_cancel_ratio: 0.0
  ack_delay_ticks: 0
  order_timeout_ticks: 0
  error_rate: 0.0
  volume_limit: false
  match_delay_ticks: 2
  session_offset_minutes: 300
  session:
    enabled: true
    sections: ["21:00:00-02:30:00", "09:00:00-10:15:00", "10:30:00-11:30:00", "13:30:00-15:00:00"]
  roll: {{active: false, logical_symbol: "{product}@main", path: {roll_path}, dispatch_parallel_ticks: false}}
  fill: {{split: false, min_qty: 1.0, max_qty: 1000.0, qty_tick: 1.0}}
  account: {{cash: 100000000}}
  cost: {{slippage: 0}}
  risk: {{max_order_qty: 10000, max_pos_qty: 10000, allow_short: true}}
  datasource:
    mode: tick
    data_mode: memory
    tick:
      files:
{chr(10).join(f'        - {p.resolve()}' for p in files)}
      symbols: [{code}]
      volume_mode: cumulative
      use_bin_cache: false
  strategies:
    - id: hft_tick_revert
      so: {so_path}
      symbol: {code}
      tick_window: 10
      qty: 1
      use_ioc: 0
      history_ticks: 256
      history_bars: 64
"""
        Path(args.emit_config).parent.mkdir(parents=True, exist_ok=True)
        with open(args.emit_config, "w", encoding="utf-8", newline="\n") as f:
            f.write(cfg)
        print(f"  config: {args.emit_config}  (记得放同目录 market.yaml, 空 {{}} 即可)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
