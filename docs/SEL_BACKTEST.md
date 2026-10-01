# SEL 回测

dev 包含 SEL 引擎、scheduler、CSV 权重策略、回测订单网关、模拟撮合、费用、风控和公司行动处理。SEL 实盘与 XTP 接入不在此分支中。

在 Ubuntu 22.04（Windows 使用 WSL2 jammy）安装 README 所列依赖后，从仓库根运行：

~~~bash
bash scripts/build_dev.sh
python3 scripts/validate_dev.py
export CLOUDTRADER_PLUGIN_ALLOWLIST=libsel_weights_csv_strategy.so
$HOME/builds/xyybtrader-oss-dev-jammy/XtSelBacktestRunner configs/backtest_sel_demo.yaml
~~~

构建目录可通过 XT_OSS_BUILD_DIR 指定，构建和验证必须使用同一值。构建入口同时生成两个回归插件及 SHA-256 sidecar。

示例输出为 reports/sel_demo/trades.csv 和 summary.json，预期 5 笔成交。重复运行成交 CSV 和 summary JSON 应逐字节一致。回归入口将配置和报告放在独立临时目录，并在成功或失败后清理。

输入：
- trading_dates.csv：交易日列表，头行为 date。
- close_prices.csv：date,code,close；调仓数量使用前一交易日可见价格，模拟成交使用当日收盘价。
- weights.csv：日期、标的和目标权重，具体表头见随仓样例。normalize 控制权重归一化。
- corporate_actions.csv：公司行动；sel_ca 示例覆盖现金分红、到账窗口、送转及配股认购。

调仓按 SELL→BUY 顺序执行，账户与风控约束仍生效；目标权重不保证每笔意图最终成交。手续费数值是固定回归参数，不代表当前市场费率。

交易日历优先读取 trading_dates_csv；该参数为空时，datalake.root 下的 ashare/calendar.parquet 可提供日历。列为 date（Int32/Int64）、is_trading_day（Bool/Int8）、exchange（字符串），SEL 读取 SSE 并按 date_from/date_to 闭区间过滤。公开测试会生成真实 Parquet 文件验证这条 fallback。

回归集合为 HFT 六场景、SEL demo、SEL large、SEL 公司行动（成交和事件两种输出），共 10 次字节比较。289 项测试的名称与源码 hash 记录在 src/xt_tests/expected_tests.json，验证入口比较 gtest、CTest 和 JUnit 完整集合，并拒绝 skip。

样例来源和每文件 hash 见 [DATA_PROVENANCE.md](DATA_PROVENANCE.md)。
