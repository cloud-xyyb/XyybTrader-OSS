# Tick 数据格式与运行须知

本文是回测 tick 数据的一些讲解

## 1. CSV 列：别名映射，不要求固定列序

引擎按**表头名**识别列（大小写、下划线不敏感），常见 vendor 列名有内置别名——
CTP 口径的 vendor CSV（如 `TradingDay,InstrumentID,UpdateTime,...`）**通常无需转换
直接可读**。列序无关，多余的列被忽略。

| 引擎语义 | 接受的表头别名 | 必需性 |
|---|---|---|
| 合约代码 | `code` / `InstrumentID` | **必需** |
| 交易日 | `TradingDay` | **必需**（YYYYMMDD） |
| 时间 | `UpdateTime`（`HH:MM:SS`，可含毫秒；缺失时回退 `UpdateTimeMs` 列名） | **必需** |
| 毫秒 | `UpdateMillisec`（与 UpdateTime 叠加） | 可选 |
| 最新价 | `LastPrice` / `Price` | **必需**（解析失败整行丢弃） |
| 成交量 | `Volume` | **必需**（语义见 §2） |
| 成交额 / 持仓量 | `Turnover` / `OpenInterest` | 推荐，缺省 0 |
| 涨跌停 | `UpperLimit(Price)` / `LowerLimit(Price)` | 可选；缺失/非法归 0 = **不限价 fail-open** |
| 盘口 L1 | `BidPrice1` `BidVolume1` `AskPrice1` `AskVolume1` | 强烈推荐（回退规则见 §3） |
| 盘口 L2-5 | `Bid/AskPrice2-5`、`Bid/AskVolume2-5` | 可选 |
| 当日高低 | `HighPrice` / `LowPrice` | 可选 |

引擎的**硬校验**只有两条：
1. 合约代码为空或 `LastPrice` 解析失败时丢弃该行
2. `TradingDay`/`UpdateTime`/`Volume` 缺失不会报错而是**静默 0**（但时间如果为 0 会导致 bug）。

**不支持的列名**（常见但引擎不读）：`ExchangeID`、`ActionDay`（见 §4）、
`AveragePrice`、`OpenPrice`/`ClosePrice`/结算参考价类。表头里的 `EpochUs` 会被识别成索引但**当前不消费**——不要依赖它作时间源。

另有一个 **simple 两列格式**（`code,price`，无时间列）：仅用于合成数据冒烟，
盘口按 last_price 双边填充，时间 key 为 0——不要用于真实回测。

## 2. Volume 语义（配错 = 静默算错，最重要的一个开关）

`datasource.tick.volume_mode` 必须与数据口径一致：

| 配置 | 数据里的 Volume 含义 | 典型来源 |
|---|---|---|
| `cumulative`（默认） | **当日累计**成交量（CTP 快照口径，单调递增） | CTP / 多数 vendor 快照数据 |
| `incremental` | 本 tick 的**增量**成交量 | 逐笔/清洗后数据 |

引擎按 mode 分别落到 `total_volume`（内部差分出逐 tick 增量）或 `volume`。
**配反不会报错**，但会导致 bug，cumulative 数据配成 incremental 会把累计量当增量，成交量相关逻辑全部虚高；反之则近乎没有交易。简单的数据 mode 判断方法：日内 Volume 单调递增 → cumulative。

## 3. 盘口回退规则

- **L1 缺价** → 用 `LastPrice` 填充 bid1/ask1（点差为 0：对撮合过于乐观，仅剩
  last-price 的数据只适合粗验，不适合 HFT 撮合结论）；**L1 缺量** → 用
  `qty_base` 填充。
- **L2-5 缺价** → 继承上一档价格；**缺量保持 0**。
- CSV 最多识别 5 档；引擎内部深度超过 5 档的槽位延续 L5 价、量为 0。

## 4. 日期语义：ActionDay 由引擎推导，CSV 里的 ActionDay 列被忽略

- `TradingDay` = 交易日（CTP 口径：夜盘属于**下一交易日**）。
- 自然日（action_date）引擎自行推导：`UpdateTime >= 19:00` 的 tick 判为夜盘，
  action_date = TradingDay 的**前一个工作日**（周一夜盘回推到上周五）。
- 推导不感知节假日（长假后首日的夜盘回推会偏差一天），影响仅限自然日展示与跨日窗口统计，撮合与交易日归属不受影响。
- tick 排序 key = `TradingDay + UpdateTime`：**多文件输入按此归并**，同一交易日的夜盘+日盘可以在同一文件，也可以拆开。

## 5. 数据目录组织与接入

推荐目录树（`tools/convert_ticks.py` 自动整理 vendor 日度 zip/CSV 并校验表头）：

```
data/ticks/<EXCHANGE>/<品种>/<合约>/<YYYY-MM-DD>.csv
```

```bash
# vendor 日度 zip（内含 <合约>_<YYYYMMDD>.csv）→ 数据树 + 可跑的回测 config
python3 tools/convert_ticks.py --src ~/Downloads/202601 --out data/ticks \
    --contracts AP605 --emit-config configs/backtest_ap605.yaml
```

config 里用显式文件列表接入：

```yaml
backtest:
  datasource:
    mode: tick
    data_mode: memory
    tick:
      files:
        - /abs/path/data/ticks/CZCE/AP/AP605/2026-01-05.csv
      symbols: [AP605]        # 行级过滤：只回放列出的合约
      volume_mode: cumulative
      use_bin_cache: false    # true 时首跑生成 bin cache 加速重放（含失效指纹）
```

**路径解析语义（混合口径，写错会找不到文件/插件）**：

| config 字段 | 相对路径基准 |
|---|---|
| `datasource.tick.files` | **运行时 cwd**（建议写绝对路径；`--emit-config` 已输出绝对路径） |
| `strategies[].so` | **config 文件所在目录**（loader realpath 失败即拒载，无 fallback） |
| `report_dir` | config 文件所在目录 |

## 6. 运行须知

1. **config 用 `--config <path>`（或 `-c` / `XT_CONFIG` env）传；裸 `.yaml/.yml` 位置参数也接受**
   （`XtBackTestRunner my.yaml` 等价 `--config my.yaml`）。其它无法识别的裸参数会打告警忽略；
   什么都没传时回退搜索 cwd / 可执行文件上层目录里的 `backtest.yaml`。
2. **策略插件白名单**：`CLOUDTRADER_PLUGIN_ALLOWLIST=libXXX_strategy.so`（逗号分隔多个）必须点名要加载的 .so，否则拒载（loader 默认 fail-closed；`CLOUDTRADER_PLUGIN_ALLOWLIST_ENFORCE=0` 可显式关闭，仅限 dev）。
3. **插件 sha256 sidecar 默认必需**：loader 默认 strict signing，`.so` 同目录**必须**存在 `<name>.so.sha256` 且内容与当前 .so 的 sha256 一致；缺失或不匹配都拒载（重编插件后要同步重新生成 sidecar）。dev 可用
   `CLOUDTRADER_PLUGIN_STRICT_SIGNING=0` 显式关闭校验。
4. 加载 profile 时，loader 要求同目录必须存在 market.yaml。它会按路径 deep-merge 到最终配置里，并覆盖主 profile 的同名字段；如果只是想让临时配置目录与仓库默认 market 配置隔离，放一个内容为 {} 的空 market.yaml 即可。--emit-config 生成配置时会自动补这个空文件。
5. 期货单合约（例如：ag2606）回测请显式关闭换月：在 backtest profile 里写 `roll: {active: false}`。否则 runner 会尝试加载换月表；表缺失时会启动失败。
6. **report_dir 自动加时间戳后缀**：`report_dir: /tmp/foo` 实际产出
   `/tmp/foo_<epoch_ms>/`（`fills.csv` / `trade_excursions.csv` /
   `daily_breakdown.json`）。
7. **session 过滤在数据源层生效**：`backtest.session.sections` 之外的 tick（含集合竞价 08:59）不进引擎；夜盘品种记得加夜盘 section（跨午夜写单条，如 `"21:00:00-02:30:00"`）。
8. 运行目录必须可写：runner 启动时会在当前 cwd 下创建 `logs/ct.log`。如果从只读目录或系统目录启动，会因为 logger 初始化失败而拒绝启动。建议固定从仓库根目录运行。

## 回测产物（report_dir 内三个文件）

- **`fills.csv`**

`fills.csv` 记录逐笔成交，一行一个 fill。regression 会对该文件做逐字节比对，因此它是 bit-exact 回归的核心输出。

| 类别 | 关键列 | 说明 |
|---|---|---|
| 运行元数据 | `mode`、`run_id`、`strategy_id`、`symbol`、`config_hash`、`code_version`、`session_date` | 标识运行、策略、合约、配置与代码版本；回测下部分字段可能为空 |
| 时间 | `ts_ms`、`ts_event_ns`、`ts_recv_ns` | 成交毫秒时间戳，以及事件 / 接收双时钟 ns |
| ID / 追踪 | `order_id`、`trade_id`、`trace_id` | 委托、成交、策略侧追踪标签 |
| 成交 | `side`、`qty`、`price`、`fee`、`slippage` | 买卖方向、数量、价格、手续费、滑点 |
| 开平 | `is_close`、`is_today`、`close_offset` | 开平仓与今昨仓标记，如 `OPEN` / `CLOSE` |
| 归属 / 盈亏 | `trading_day`、`realized_pnl` | 交易日，以及平仓时产生的已实现盈亏 |
- **`trade_excursions.csv`** —— 逐笔往返（开仓→平仓 round-trip）：开/平仓日期时间与价格、
  `realized_pnl`、`mae`/`mfe`（最大不利/有利偏移，另有对应价格列）、`ticks_held`（持仓
  tick 数）。看单笔质量、拿 MAE/MFE 调止损。
- **`daily_breakdown.json`** —— 按交易日拆分的汇总（`schema_version`/`start_date`/
  `end_date`/`days`/`breakdown`），做逐日绩效分析的输入。

## 回放 live 录制（replay）

live 录的行情可以喂回回测器复盘策略行为：

```bash
python3 tools/replay_jsonl.py \
    --jsonl var/live_runs/ctp/recordings/live.jsonl \
    --code ag2612 --out data/replay --emit-config configs/replay_ag2612.yaml
./build/XtBackTestRunner --config configs/replay_ag2612.yaml
```

- 支持多个 `--jsonl`（含轮转出的 `live.jsonl.<epoch>`），按时间归并；生成的 config
  按需改 `commodities` 费率参数；
- 回放中的委托/成交由撮合引擎按录制行情**重新模拟，不等于实盘成交**（实测同一段
  15 分钟录制：live 77 笔 vs 回放 61 笔，差异来自真实柜台排队/IOC 与撮合模型假设）——
  用途是策略行为复盘与撮合假设对照，不是成交回放；
- fbs event store（`events/`）是另一 schema 的原始双轨归档，本工具只消费明文 JSONL。

## 录制数据量与维护

单合约实测（CTP ag，连续交易 15 分钟，~1.7 tick/s）：

| 产物 | 15 分钟实测 | 外推每交易日（≈12h 盘面） |
|---|---|---|
| `recordings/live.jsonl` | 1.2 MB | ~50-70 MB |
| `recordings/events/`（fbs 双轨） | 132 KB | ~6-10 MB |
| `report_contract/*.csv`（1s 采样） | 756 KB | ~35-45 MB |
| `var/logs/ct.log` | 120 KB | 数 MB（level=info） |

高频 venue（例如 binance）可到**数 GB/天**，随 tick 速率线性。

- **轮转已内建**（示例 config 默认开）：`recording.rotate_bytes: 1073741824`（1 GiB）
  + `rotate_interval_sec: 3600` → 出 `live.jsonl.<epoch>`，fbs 联动滚出
  `events.<epoch>/`。单文件不会无界增长，**但滚出的历史件不会自动清理**。
- **压缩与清理自备**，示例 crontab（每天 04:00 压缩 1 天前的轮转件、删 30 天前的）：

```cron
0 4 * * * cd /path/to/repo && find var/live_runs -name "live.jsonl.[0-9]*" -mtime +1 -exec gzip {} \; ; find var/live_runs \( -name "live.jsonl.*.gz" -o -name "events.[0-9]*" \) -mtime +30 -exec rm -rf {} +
```

- 长期跑建议配置磁盘监控告警
