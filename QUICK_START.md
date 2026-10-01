> dev 的 SEL 示例见 [SEL_BACKTEST.md](docs/SEL_BACKTEST.md)。

# 快速上手：从编译到第一份回测报告

本文带你从零跑通一次完整回测：**编译引擎 → 启动 Backtest Viewer → 入库数据 → 生成配置 → 跑 → 读报告**。

分两部分：

- **引擎**（本仓，C++）—— 回测器 `XtBackTestRunner` 与实盘 runner `XtRunner`，源码随仓，自己编译。
- **Backtest Viewer**（`xt-viewer`）—— 本机跑的图形回测工作台，单文件分发，不随本仓开源。
  它不是必需品：只用命令行也能跑回测（见 [README](./README.md) 的「快速开始」）；
  viewer 的价值在于**数据入库、配置生成、报告解读**这三段体验。

Viewer 全程只在本机工作，不联交易所、不上传数据。

```
① 编译引擎 ──▶ ② 启动 viewer ──▶ ③ 首次设置 ──▶ ④ 数据入库 ──▶ ⑤ 生成 config 跑回测 ──▶ ⑥ 读报告
     §1              §2              §3            §4               §5                    §6
```

> Viewer 界面**默认英语**。下文按「`English label`（中文）」给出，两者都是界面原文，
> 中文是切到 zh 语言后的显示文本。

---

## 1. 编译引擎

平台：**Ubuntu 22.04+**。Windows 用户在 **WSL2（Ubuntu 22.04）** 里编译和运行引擎，
viewer 则跑在 Windows 侧——这是最常见的组合，§3 会讲两侧路径怎么对上。

### 1.1 依赖

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build wget time \
    libcurl4-openssl-dev libssl-dev libboost-dev libboost-system-dev
# ↑ time 供 viewer 采引擎运行指标（峰值内存等）；缺它 viewer 仍能跑，指标降级并提示

# Apache Arrow / Parquet（必需，先加官方 apt 源: https://arrow.apache.org/install/）
wget -P /tmp "https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb"
sudo apt-get install -y /tmp/apache-arrow-apt-source-latest-*.deb && sudo apt-get update
sudo apt-get install -y libarrow-dev libparquet-dev
```

### 1.2 构建主体

在仓库根目录：

```bash
bash scripts/build_dev.sh
```

主要产物：

| 路径 | 用途 |
|---|---|
| `$HOME/builds/xyybtrader-oss-dev-jammy/XtBackTestRunner` | **回测器** —— viewer 调用的就是它，§3 要填的路径 |
| `$HOME/builds/xyybtrader-oss-dev-jammy/xt_live/hft/XtRunner` | 实盘 runner（CTP / Binance），本文不涉及 |

### 1.3 策略插件

统一构建入口已生成 hft_tick_revert、sel_weights_csv 两个插件和 SHA-256 sidecar。其它 HFT 示例插件可按各自 CMakeLists.txt 独立构建。

### 1.4 自检：跑一次回归

仓里自带几个交易日的样例 tick 数据和 golden 输出，直接可跑：

```bash
bash scripts/regression_test.sh
```

判据为 HFT 六场景与 SEL 三组的 **10 次输出比较全部 BIT-EXACT**。另运行 `python3 scripts/validate_dev.py`，核对全部 289 项测试且零 skip。
到这里引擎侧就通了。

**记下 `$HOME/builds/xyybtrader-oss-dev-jammy/XtBackTestRunner` 的绝对路径**，§3 要用。

> 引擎的几个运行约定（`--config` 必须显式传、插件 allowlist、`market.yaml`、单合约要关换月、
> report_dir 时间戳后缀等）集中在 [docs/data_format.md](./docs/data_format.md) 的「运行须知」。
> 用 viewer 时这些由 viewer 代劳，手写 config 时要自己照顾。

---

## 2. 启动 Backtest Viewer

1. 从本仓 GitHub Releases 下载对应平台的 zip（旁边的 `.sha256` 可选校验：Linux
   `sha256sum -c <zip名>.sha256`，macOS `shasum -a 256 -c <zip名>.sha256`），解压后是扁平布局：

   ```
   xt-viewer               回测查看器（UI + 后端全内嵌，单文件；Windows 后补版为 xt-viewer.exe）
   README.txt              运行说明
   SHA256SUMS              包内文件清单校验值
   compatibility.json      绑定的引擎/viewer 源码版本
   provenance.json / sbom.cyclonedx.json / licenses/    构建溯源与许可证
   ```

2. 运行后它会自己拉起浏览器指向 `http://localhost:8770`
   （设 `XT_VIEWER_NO_BROWSER=1` 可禁用自动开浏览器）：

   - **Linux / WSL2**：`./xt-viewer`（WSL 内跑时用 Windows 浏览器开 localhost 即可）；
   - **macOS**：未签名，首次运行需 Gatekeeper「右键 → 打开」放行；
   - 首次启动是单文件自解压，慢几秒到十几秒属正常。

3. 端口被占用时按优先级覆盖：`--port` > 环境变量 `XT_VIEWER_PORT` > `PORT` > `8770`。

   ```
   ./xt-viewer --port 8899
   ```

**分发包不含任何回测结果**——第一次打开列表是空的，属正常。
已有的回测 bundle 放 exe 同目录的 `bundles/` 下，或用 `XT_VIEWER_BUNDLES_DIR` 指向别处。

---

## 3. 首次设置（两项必填）

点右上角 **⚙**（`Viewer settings` / viewer 设置）打开设置抽屉。
每个字段带一个来源徽标：`env` / `saved`（已保存）/ `default`（默认）——
被环境变量遮蔽的已存值会明确警示，不会静默失效。

### 3.1 `engine_bin · hft`（引擎二进制）

填 §1.2 产出的 **`XtBackTestRunner` 绝对路径**。
输入框旁有**自动发现下拉**，能扫到的候选会直接列出来。

> 留空会走内置默认解析链——那条链是给特定目录布局准备的，自建 `build/` 目录的用户请显式填。

同组还有两个字段：

| 字段 | 填什么 |
|---|---|
| `Engine repo path`（引擎仓路径） | 本仓根目录（引擎运行环境内的路径） |
| `Engine exec mode`（引擎运行方式） | 空 = 平台默认；跨 WSL 调用填 `wsl`；同机直跑填 `local`；`off` = 不调引擎，只看已有结果 |

**Windows + WSL2 组合**：viewer 在 Windows 侧、引擎在 WSL 里，`Engine exec mode` 选 `wsl`，
上面两个路径填 **WSL 侧的形式**（`/home/…` 或 `/mnt/<盘>/…`）。

### 3.2 `Data root`（数据根目录）

**这是最容易填错的一项。** 界面提示原文：

> 这台电脑的数据根 · …… · config 里 `${data_root}` 的替换值 ·
> **指数据集根(normalized_v2/zip 所在)，不是引擎仓的 `data/` 目录**

提示语随 viewer 运行平台变化，按提示里说的形态填：

| viewer 跑在 | 该填什么 |
|---|---|
| Windows（引擎在 WSL 里跑） | **WSL 形式** `/mnt/<盘>/…` |
| WSL 内 | WSL 侧路径（`/mnt/<盘>/…` 或 `~` 下均可） |
| macOS / Linux 本机 | 与本机路径相同，直接粘贴 |

> ⚠️ 常见错法：填成本仓的 `data/` 目录。那是**仓自带的回归样例目录**，不是你的数据集根。
> 数据集根是你放 `normalized_v2/` 成品目录、或放月度 tick ZIP 的那个目录。

**只看已有回测结果不需要设置这两项**——它们只在真正调起引擎（入库 / 跑回测）时才用得上。

---

## 4. 数据入库（第一个数据集）

「入库」= 把原始 tick 数据转成引擎直读的 `normalized_v2` 布局：按
`<交易所>/<品种>/<合约>/<YYYY-MM-DD>.csv` 归树、每交易日一个文件（例如
`SHFE/ag/ag2206/2022-05-27.csv`；列口径见 [docs/data_format.md](./docs/data_format.md)，
仓内 `tools/convert_ticks.py` 产的就是这个布局）。

左侧竖排导航两个入口：`Workbench`（工作台）/ `Experiments`（实验）。
在 `Workbench` 的数据源节点进 **`Data station`（数据站）**。

### 4.1 登记数据源

点 **`+ Add data source`（＋ 添加数据源）**，类型三选一：

| 选项 | 什么时候用 |
|---|---|
| `Data source roots (ZIP)`（数据来源根(ZIP)） | 手上是**月度 tick ZIP**，需要入库 |
| `Ingested data dir (normalized_v2)`（已入库数据目录) | 手上**已经是 normalized_v2 成品目录**，直接登记，跳过 4.2 |
| `Config discovery dirs`（config 发现目录） | 只是想让某目录下的 config 出现在运行下拉里 |

填绝对目录（可点浏览按钮选择）→ `Add to list`（加入列表）→ `Scan`（扫描）。

> 落在 `Data root` 下的成品数据集会**自动发现**，不必重复登记；只有不在数据根下的才需要手动登记。

> **零数据起步**：本仓自带的 `data/regression/sample_ticks`（ag2206/ag2212 各 8 个交易日）
> 就是现成的 normalized_v2 形布局 —— 选 `Ingested data dir` 把它登记进来，即可不准备任何
> 自有数据先把 ⑥ 步全流程走通（样本只有 8 日，只作冒烟，研究结论以 ≥30 交易日为准）。
>
> ⚠️ `configs/baselines/*.yaml` 是回归脚本专用（含 `${PROJECT_DIR}` 占位符，由
> `run_baselines.sh` 展开），**不要**把它登记成 config 发现目录直接喂 viewer 跑。

### 4.2 入库（ZIP 源才需要）

进月度格子视图，图例：

| 图例 | 含义 |
|---|---|
| `Complete`（完整） | 该月数据齐全 |
| `Partial`（部分） | 按工作日近似，不扣节假日 |
| `has data`（有数据） | 覆盖未评估——重扫可生成逐日计数 |
| `Ingested`（已入库） | 月粒度：normalized_v2 该月存在任意文件，**非逐日核对** |
| `None`（无） | 无数据 |

勾选品种 / 合约 → **`Ingest selected products/contracts into normalized_v2:`（入库选中品种/合约到 normalized_v2:）**。

任务按月拆队列（每月一个 + 一个收尾聚合任务），可取消。提交前会给体量预估——注意提示原文：

> 注:体量为解压 tick CSV 估算未校准；normalized_v2 产物实测可达 ~3×

完成后弹提示，带一个直达按钮 **`Build config`（去建 config）**——点它直接进第 5 步。
部分失败 / 结果不确定时**不会**给这个按钮，只给 `View queue`（查看队列）引导你核对（不谎报成功）。

> 已经是 CTP 口径的 vendor CSV 往往**无需转换直接可读**（引擎按表头别名识别列）。
> 列语义、volume 口径、盘口回退规则见 [docs/data_format.md](./docs/data_format.md)。
> 不用 viewer 时，本仓的 `tools/convert_ticks.py` 做同样的归树 + 校验 + 生成配置。

---

## 5. 生成 config 并跑第一个回测

### 5.1 从数据工作台生成 config

不用手写 yaml。按数据形态选入口：

| 位置 | 按钮 |
|---|---|
| 展开品种 → 具体合约行 | `New config`（新建 config） |
| tick 源级 | `New config (pick a contract)`（新建 config（选合约）） |
| bar 源级 | `New config from this data`（从此数据新建 config） |
| 入库完成提示 / 已入库常驻行 | `Build config`（去建 config）/ `New config`（建 config） |

生成的 config 顶部带一条来源横幅：品种、窗口、约多少交易日（**取自所选数据源实际覆盖，
非模板默认**），以及成本假设的口径。生成后直接打开编辑器让你核对。

> ⚠️ 生成器**不硬编码** `vol_scale` / `fees` / `session`——这些是品种事实，配错会静默缩放权益曲线。
> 横幅会点名要你核对。

### 5.2 `data_mode` 自动选择

引擎的 `backtest.datasource.data_mode` 只有两个值，生成器**按估算行数自动选**，
并把判据写进行尾注释：

| 档 | 触发条件 | 代价 |
|---|---|---|
| `memory` | 估算行数 ≤ **2,500,000** | 全量驻留，小数据集最快 |
| `stream` | 估算行数 > 2,500,000 | 边读边喂，内存恒定，每跑重读 CSV |
| `stream`（fail-safe） | **估不出**（清单无行数） | 同上——「保守 = 不会炸的那一档」 |

阈值按真实字节账定的：一行 tick 常驻 **832 B**（20 档盘口），250 万行 ≈ **2.0 GiB** 常驻。
生成的注释会把这笔账原样写给你看，便于按自己机器复核。

> 确知数据集小、想更快 → 在编辑器里把 `stream` 改回 `memory`。反之，多合约 / 多年 / 参数扫描
> 用 `memory` 有 OOM 风险（见 §7.2）。

### 5.3 Run

回到 `Run backtest`（运行回测）面板，选刚生成的 config → 跑。

---

## 6. 读报告

报告按 tearsheet 顺序分区

### 6.1 `Suggestion`（建议）卡 —— 报告顶部

告诉你「**现在该做的那一件事**」。


它按研究次序自动定位你在哪一步：数字可信 → 有成交 → 正期望 → sweep 显著性 → WF 样本外。

### 6.2 研究门禁可信度

门禁**判仪器**, 回答「这轮回测的数字可不可信」.

### 6.3 实验结果

实验结果与分析图表

---

## 7. 常见问题

### 7.1 「引擎二进制陈旧」提示是什么意思

设置抽屉的引擎二进制候选里可能出现 `older than source HEAD`（早于源码 HEAD）标记，
提示行会说「可能在跑已修过的旧代码，重新构建再跑」。

**判据 = 二进制的 mtime < 仓 HEAD 那笔 commit 的时刻**，是**必要非充分**条件。
提示文案自己披露了局限：

> 判据只比对 commit 时刻：未提交的改动看不见，两侧时钟偏移也会误判

所以：看到它 → 重新构建引擎（§1.2）再跑，方向上总是安全的；**没看到它也不等于二进制和你的
工作树一致**。这个提示宁可多报一次，也不漏报「可能在跑旧代码」。

### 7.2 `memory` 还是 `stream`

报告的系统区会给这条告示：

> `data_mode:memory` 全量驻留，内存随输入线性增长 — 多合约 / 多年 / 参数扫描有 OOM 风险。

`memory` 会把所有数据读入内存,可能导致 **OOM**（
`stream` 会分段读取**每跑重读 CSV**。生成器默认会推荐默认值
手动改之前先算一下 `行数 × 832 B`。

### 7.3 初次运行缓慢

生成的 config 带 `use_bin_cache: true`：

> CSV→bin 缓存（参数指纹命名 + 清单失效防护）：**首跑构建、复跑秒读**；
> `false` 会每次全量重读 CSV

首跑要把 CSV 转成 bin 缓存，初次运行缓慢。缓存按**参数指纹**命名并带失效防护——
换了数据源或换了相关参数会自动重建，不会读到陈旧缓存。

### 7.4 回放日期比你设的窗口首日还早

> 夜盘归属次一交易日：周五 21:00 的行情算下周一的数据——回放日期早于窗口首日不是错

期货夜盘（如 21:00 起）与次日日盘同属**一个交易日**。数据文件按挂钟自然日命名，
回测与报告按交易日聚合，两者在夜盘差一天——这就是「文件日期对不上交易日」的根源
> 遇到看不懂的术语：导航底部的 **`?`**（`Glossary` / 术语表）是词汇表入口。

---

## 附：AI 调查功能

报告详情页顶部的 AI 调查卡需要凭证。exe 版内嵌的是 **backtest-only** agent 入口，
只注册回测结果研究工具，不含任何实盘观测 / 控制 / 部署能力。

测试版, 仅供娱乐。

---

## 下一步

- 写自己的策略 → `src/xt_strategy_plugin/` 下五个参考插件，从 `hft_tick_revert` 读起
