# 回归样例

从仓库根运行 bash scripts/build_dev.sh 和 bash scripts/regression_test.sh。

- HFT：ag2206_only、ag2212_only、roll_29col、holiday、ag2206_vollimit、ag2206_vollimit_split；逐字节比对 fills.csv。
- SEL demo：30 个交易日、3 个股票、2 次调仓，比对 trades_baseline.csv。
- SEL large：合成价格、5 个股票、3 次调仓，比对 expected_trades.csv。
- SEL CA：手工公司行动样例，同时比对成交和 corporate_action_events.csv。

数据路径、模式和来源见 [数据说明](../../docs/SEL_BACKTEST.md) 与 [来源清单](../../docs/DATA_PROVENANCE.md)。
