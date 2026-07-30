# XyybTrader

<p align="center">
    <img src="https://img.shields.io/badge/language-C%2B%2B17-blue.svg"/>
    <img src="https://img.shields.io/badge/platform-linux-yellow.svg"/>
    <img src="https://img.shields.io/badge/license-MIT-orange.svg"/>
</p>

<p align="center">
    <a href="#xyybtrader-的优势">优势</a> ·
    <a href="#怎么写一个-hft-策略">写策略</a> ·
    <a href="#性能与验证">性能</a> ·
    <a href="#相关工具beta">相关工具</a> ·
    <a href="#quick-start">Quick Start</a> ·
    <a href="#数据">数据</a>
</p>

#### XyybTrader 是什么

XyybTrader 是一套 C++17 写的高频交易系统，覆盖 tick 级回测、仿真撮合，到实盘交易、风控、录制。

策略以`.so` 插件的形式接入`runner`，即插即拔单独编译。回测和实盘维护同一套策略语义，差异只留 replayer、虚拟时钟、撮合模型等环境边界上。

#### 三类策略引擎

XyybTrader 支持 **HFT**、**CTA**（beta）、**SEL**（beta）三类策略，共用策略接口、账户、风控；本仓目前开源 HFT，CTA 和 SEL 会在后续版本陆续开源。

- **HFT 引擎** —— 高频策略引擎, 逐笔处理行情策略，单线程事件驱动，具有极低的系统延迟。
- **CTA 引擎**（beta）—— 时间驱动 + 事件驱动，策略按目标仓位表达意图，引擎进行差量下单和开平拆分；适合分钟到日线级的趋势、均值回归类策略。
- **SEL 引擎**（beta）—— 时间驱动，策略输出一组目标权重，引擎设置仓位；适合因子选股等策略等。

#### 架构

实盘架构：行情和回报通过 gw 回调进 spsc 无锁队列，由 worker 单线程串行消费，`XtHftEngine` 把 tick、order、fill 分发给策略；下单、策略计算、风控在同一个进程里完成。

![live 架构](./docs/assets/arch-overview-live-architecture.svg)

多 runner 组合盘：独立的 MD connector 进程统一获取行情，将 tick 写进共享内存 ring，多个 runner 通过共享内存读取同一份行情；下单、账户、风控和安全锁留在各自 runner 进程内，交易账户互相隔离，单盘故障不影响其他盘。

相比每个盘各连一次行情, 多 runner 接入同一 md 有效避免行情接入限额，多盘不会在同一个 IP 限额里互相挤兑；行情解析一次多盘复用；所有盘看到同一份 tick 序列，跨盘复盘时间轴完全对齐。

![多 runner 组合盘架构](./docs/assets/arch-overview-multibook-md-fanout.svg)

回测：replayer 按虚拟时钟回放历史数据, 模拟交易所数据报，backtest gateway 和撮合引擎替代真实交易所撮合，策略、账户、风控的语义与live一致。

![backtest 架构](./docs/assets/arch-overview-backtest-architecture.svg)

#### XyybTrader 的优势

* **极低的交易延迟**
    > - 全程单线程事件循环：回调进无锁队列，worker 串行消费，没有锁竞争，行为确定
    > - 进程内下单管线亚微秒：策略 `on_tick` 到 venue 提交前的本地全链路（含同步风控）实测约 1 µs，分级数字和口径见下文「性能与验证」

* **回测优化**
    > - 同一份策略在回测和实盘可以获得同样的组件支持
    > - 延迟模型、排队模型可注入，且支持实盘数据分析模拟
    > - 高效回测，单核约 76K ticks/s，全年 1480 万 tick 三分多钟跑完，结果稳定
* **内置风控**
    > - 下单前进行账户级和单策略两层风控
    > - watchdog 强平阶梯：策略越界时会自动从告警升级到强制平仓
    > - 支持收盘强平兜底：被动价平仓失败会自动切 IOC 激进平仓，不把仓位带过夜
* **稳定的实盘以及断线恢复**
    > - 重启恢复以交易所回查为基础，反向重建订单簿和账本，不留幽灵仓位、孤儿挂单
    > - 全量录制：行情、委托、回报、成交双轨落盘（JSONL + FlatBuffers），事后整段 replay 对账
* **支持多策略、多组合盘**
    > - 一个 runner 可以挂多个策略，order 和 fill 按策略路由，支持策略粒度的风控管理
    > - 行情用共享内存 ring 分发：connector 连一次交易所，多个组合盘进程读同一份行情，交易账户互相隔离
* **强大的回测分析工具**
    > - bt viewer 辅助分析：单次复盘、参数扫过拟合判定、策略效果分析
    > - 实盘控制台集中管多个 runner，一键暂停、平仓、kill switch

#### 怎么写一个 HFT 策略

策略继承 `XtStrategyBase`，`on_init` 里 `ctx->sub_tick(code)` 订阅合约，主回调是 `on_tick`：

![HFT 策略实盘闭环](./docs/assets/arch-readme-hft-live-loop.svg)

最小可用形态如下：

```cpp
#include "xt_strategy_engine/XtStrategyBase.h"
#include "xt_strategy_engine/StrategyABI.h"
#include "api/IStrategyContext.h"
#include "xt_core/market/XtTick.h"
#include "xt_core/order/XtOrderTypes.h"

class MyStrategy : public XtStrategyBase {
public:
    void init(const XtStrategyInitConfig& cfg) override {
        symbol_ = cfg.get_string("symbol", "ag2606");   // yaml 参数
    }
    void on_init(IStrategyContext* ctx) override {
        ctx->sub_tick(symbol_.c_str());                 // 订阅后 engine 才喂 tick
    }
    void on_tick(IStrategyContext* ctx, const XtTick& tk) override {
        if (tk.bid_price_fp[0] <= 0) return;
        if (ctx->get_position_fp(symbol_.c_str()) != 0) return;
        // 买一价挂 1 手；返回 order_id，0 = 被风控或网关拒单
        ctx->buy_fp(symbol_.c_str(), tk.bid_price_fp[0], order_exec_to_fp(1),
                    IStrategyContext::TIF_GTC, "demo", XtOffset::Open);
    }
private:
    std::string symbol_;
};

XT_STRATEGY_ABI_EXPORTS(MyStrategy)  // 导出 create/destroy + ABI 版本校验符号
```

策略的最小流程：

- **订阅行情**：`sub_tick` 之后 engine 按订阅过滤 tick 喂给策略，不直接接触 MD gateway。
- **读状态**：最新 tick、挂单、仓位、资金、参数，通过 Context 获取。
- **发单撤单**：`buy_fp`、`sell_fp`、`cancel`通过`Context`实现，风控、成本、账户、venue 提交不需要策略处理。
- **收回报**：`on_order`、`on_fill`、等回调处理。
- **插件约定**：需要符合`runner`的`ABI`要求，loader 加载时校验 ABI 版本和内存布局。

repo 带五个能直接参考的插件：`hft_tick_revert` 是最小可用的 tick 反转（订阅、换月、仓位、平仓路径都覆盖），`hft_market_making` 维护在途 GTC 做市单，`hft_microprice_mm` 是 microprice 信号驱动的做市变体，`hft_market_verify` 用一笔市价单跑通全链路，`hft_conformance` 是全功能自检压测策略。

#### 性能与验证

下面都是分级 micro-bench 实测数字（本地链路的数字不含网络、柜台、交易所撮合或真实回报延迟）

进程内下单延迟，用 `xt_inner_latency_bench` 测同一条策略 `on_tick -> buy` 热路径：mock TD 直接返回，
`XT_BENCH_DRAIN=1` 每笔提交后立即灌 synthetic fill，样本 500,000、warmup 20,000，取 5 轮中位数。

| 档位 | flag | 覆盖范围 | 中位耗时 |
|---|---|---|---|
| L1 骨架 | `--mock` | fp 转换 + 策略虚调用链 + id 分配，不进入引擎订单管线 | 40.5 ns |
| L2 管线 | `--no-risk` | L1 + OrderGateway + 订单簿记 + 无锁快照发布 + terminal fill drain，跳过风控 | 361.1 ns |
| L3 完整同步风控 | 默认 | L2 + 同步保证金/持仓限额风控 | 822.4 ns |

分级口径的含义：L1 是派发骨架下界；L2 是生产订单管线的本地固定成本；L3 是打开完整同步风控后的本地上界。

回测，用 `hft_conformance` 全功能压测，单核：

| 档位 | 规模 | 结果 |
|---|---|---|
| 全年档 | 14.8M ticks、3.28M orders、2 次换月 | 195.5 秒跑完，约 76K ticks/s、17K orders/s，峰值内存 881 MB，断言零违例 |
| 确定性 | 全年档连跑两次 | 回测结果完全一致 |


#### 支持的交易接口

- **期货**
    - CTP —— SDK 随仓附带，SimNow 仿真可直连；行情、交易、断线恢复、报撤单与回报映射
- **加密货币**
    - Binance USDT-M 永续 —— demo trading / testnet 可直连；行情、交易、REST + WebSocket 双通道、User Data Stream、对账
- **股票**
    - 中泰XTP（beta）

#### 相关工具（beta）

相关工具还在测试，beta 版打包发布在本仓 [GitHub Releases](../../releases/latest)（zip，解压即用）：
- **Linux 版（主发布物）**：解压后运行 `./xt-viewer`（WSL2 内也可，Windows 浏览器开 http://localhost:8770）。
- **macOS 版**：未签名，首次运行需 Gatekeeper「右键 → 打开」放行。
- 首次启动是单文件自解压，慢几秒到十几秒属正常；**工具还在测试，稳定后会发布正式版。**

**Backtest Viewer** —— 本机跑的回测查看器。回测产物一行命令接入，页面按 tearsheet 布局：权益、回撤、成本分解、逐笔明细。支持三层 agent 分析：

![Backtest Viewer](./docs/assets/screenshot-backtest-viewer.png)

- **AI 调查卡**：agent 复盘单次回测，给根因、证据和下一步
- **参数扫复盘**：回测参数矩阵扫描辅助。
- **变体搜索**：agent 自己改参数、跑回测、看结果、再改，一轮轮迭代。

**Live Panel（暂未发布）** —— 实盘控制台。多个 runner 的策略状态、当日盈亏、风控状态，支持暂停恢复、手动平仓、kill switch。控制链路走 gRPC。

#### Quick Start

> **第一次上手（图形化路径）** → [QUICK_START.md](./QUICK_START.md)：从编译引擎到用 Backtest
> Viewer 入库数据、跑出第一份回测报告的逐步走查。

> 下方 4/5 两步引用的 `*.yaml.example` 实盘模板随发布树提供。

**1. 构建（Ubuntu 22.04+）**

> 项目目前仅支持 linux——windows 需要在 **WSL2（Ubuntu 22.04）** 里执行以下全部步骤；
> Backtest Viewer 会自动经 `wsl` 调起这里构建出的二进制。

```bash
# 依赖
sudo apt-get install -y build-essential cmake ninja-build libcurl4-openssl-dev libssl-dev libboost-dev libboost-system-dev
# Apache Arrow / Parquet（需要官方 apt 源: https://arrow.apache.org/install/）
wget "https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb"
sudo apt-get install -y ./apache-arrow-apt-source-latest-*.deb && sudo apt-get update
sudo apt-get install -y libarrow-dev libparquet-dev

# 构建主体（回测器 + 实盘 runner）
cmake -S src -B build -DCMAKE_BUILD_TYPE=Release -DXT_BUILD_CONTROL_GRPC=OFF
cmake --build build -j$(nproc)

# 构建策略插件（独立编译为 .so）
cmake -S src/xt_strategy_plugin/hft_tick_revert -B build-strategy -DXT_SRC_ROOT=$(pwd)/src
cmake --build build-strategy -j$(nproc)
mkdir -p var/plugins/strategies && cp build-strategy/libhft_tick_revert_strategy.so var/plugins/strategies/
# 开发期想跳过校验，设 CLOUDTRADER_PLUGIN_STRICT_SIGNING=0
(cd var/plugins/strategies && sha256sum libhft_tick_revert_strategy.so | awk '{print $1}' > libhft_tick_revert_strategy.so.sha256)
```

> 本步 `-DXT_BUILD_CONTROL_GRPC=OFF` 的产物跑回测与实盘 runner 足够；控制面 gRPC
> 供 Live Panel 使用，Panel 未随本仓发布，保持 OFF 即可。

**2. 第一个回测**

```bash
bash scripts/run_baselines.sh ag2206_only
```

建议通过回归脚本启动本用例。脚本会展开配置中的 `${PROJECT_DIR}`，
生成本次隔离的 market 配置，启动 `XtBackTestRunner`，并在结束后将 fills
输出与 golden 文件做逐字节比对。

直接运行 `XtBackTestRunner` 时需注意：

- 配置文件可以通过 `--config xxx.yaml` 或裸 `xxx.yaml` 位置参数传入。
  显式传入的路径不存在时会 fail-fast。不要裸跑 runner：完全不传参、
  或裸参数不是 `.yaml` 时，runner 会回退到默认 profile 搜索链。
- 策略插件必须加入 `CLOUDTRADER_PLUGIN_ALLOWLIST`。
- `report_dir` 会自动追加时间戳后缀，实际输出目录不是配置里的原始路径。
```bash
export PROJECT_DIR=$(pwd)
sed "s|\${PROJECT_DIR}|$PROJECT_DIR|g" configs/baselines/ag2206_only.yaml > /tmp/bt.yaml
echo '{}' > /tmp/market.yaml     # 隔离 market 合并，loader 在 config 同目录找它
CLOUDTRADER_PLUGIN_ALLOWLIST=libhft_tick_revert_strategy.so \
  ./build/XtBackTestRunner --config /tmp/bt.yaml
```

**3. 跑完整的 regression 测试**

```bash
bash scripts/regression_test.sh
```

**4. 实盘，CTP / SimNow**

```bash
cp configs/secrets.yaml.example configs/secrets.yaml   # 填 SimNow investor 账号密码
cp configs/live_hft_ctp.yaml.example configs/live_hft_ctp.yaml
CLOUDTRADER_PLUGIN_ALLOWLIST=libhft_tick_revert_strategy.so \
  ./build/xt_live/hft/XtRunner --config configs/live_hft_ctp.yaml
```

**5. 实盘，Binance demo trading**

```bash
cp configs/secrets.yaml.example configs/secrets.yaml   # 填 demo API key/secret，权限只勾 Enable Futures
cp configs/live_hft_binance.yaml.example configs/live_hft_binance.yaml
CLOUDTRADER_PLUGIN_ALLOWLIST=libhft_tick_revert_strategy.so \
  ./build/xt_live/hft/XtRunner --config configs/live_hft_binance.yaml
```

> `configs/secrets.yaml` 在 `.gitignore` 里，凭证不会进仓。凭证按主配置相同的 YAML 路径写，
> 加载时 deep-merge 覆盖。

#### 数据

- **自带样例**：`data/regression/` 里有几个交易日的期货 tick 数据，快速开始和回归直接能跑。
- **CSV 按表头识别列，不要求固定列序**：CTP 口径的 vendor 列名，比如 `InstrumentID`、`TradingDay`、`UpdateTime`、`BidPrice1`，引擎有内置别名，**多数 vendor 数据不用转换就能直接使用**，多余的列自动忽略。列语义与成交量口径见 [docs/data_format.md](./docs/data_format.md)。
- **数据接入**：先确认 vendor 表头能否被直接识别；不匹配时，按 [docs/data_format.md](./docs/data_format.md) 的列口径自行转换（仓内 `tools/convert_ticks.py` 可作转换参考）。

#### 免责声明

- 本项目只为技术研究和教育目的开源，不构成任何投资建议。
- 实盘交易有真实资金风险。接真实账户之前，请先在 SimNow 或 Binance demo 环境充分验证。
- 作者不对使用本框架造成的任何损失负责。

#### 许可证与致谢

- [MIT](./LICENSE)。第三方组件的许可证见 [THIRD_PARTY_NOTICES.md](./THIRD_PARTY_NOTICES.md)。
