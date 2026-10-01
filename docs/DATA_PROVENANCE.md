# SEL fixture 来源与校验

这些是项目内维护的合成回归 fixture，随本项目 MIT 许可发布；没有另行指定个人作者。

- sel_demo：固定 mock 收盘价、30 个交易日、3 个标的、两次权重调整。golden 是本项目回测器的预期成交。
- sel_demo_large：历史构造说明记录为 GBM 随机 walk，5 个标的、3 次调仓；包含用于前日价格的 warmup 行。原始随机 seed 未记录，不声称能重新随机生成同一数据，使用下列固定文件 hash 复现。
- sel_ca：手工构造的交易日、价格、权重和公司行动，检验分红应收/支付、送转到账、配股认购。golden 包含成交及公司行动事件。
- 股票代码用于演示，价格并非这些证券的真实行情。原有 sample_ticks 是历史期货数据，来源与权限说明仍见 THIRD_PARTY_NOTICES.md。

| 文件 | SHA-256 |
|---|---|
| data/regression/sel_ca/close_prices.csv | 7614addc36b52d911efe192bbc935357b5603c1cfc3d292a196e5e481c5eab7b |
| data/regression/sel_ca/corporate_actions.csv | d26aafaffc8ea1d07ec5dd45b365cc369b2b9a5090685851421e352564b6308c |
| data/regression/sel_ca/expected_corporate_action_events.csv | 16af55a0dcdfce1c43a158640167119ca7d6acc0c9669f18224aae4e843e220e |
| data/regression/sel_ca/expected_trades.csv | 9b5b66f442f0fc40a186ab3b75ea2de6c54e45c512f7a6c8ea580315b1143c0d |
| data/regression/sel_ca/trading_dates.csv | fb04b5d40893a6431d73f8479fb1dff889cd997117e4683d133400f51eed0814 |
| data/regression/sel_ca/weights.csv | e01f5faea6536ce319534ba58842991570342ca8f677ad6e2fee2f72d615e62c |
| data/regression/sel_demo/close_prices.csv | 84f4d537d99842f19c9a4e32d190e2716b477923544b3624448bfba2211cef34 |
| data/regression/sel_demo/trades_baseline.csv | ba7868e9b6d415a9fa1e2822522af21a3e15fc76a384aea9a9eaa1a754207655 |
| data/regression/sel_demo/trading_dates.csv | c60207d8a684335812d7395bdc8adc772150ae9b65b2b3900ca5be66b8f63359 |
| data/regression/sel_demo/weights.csv | 16cc02607aa16f4003bc792bbb7ac4eafbe217ee8340c1b20bcc60c574bafda6 |
| data/regression/sel_demo_large/close_prices.csv | 8eddd292795fbf49d069a82b14b74c0bb50d95535e0b6377fb6599aa5ac6a6b8 |
| data/regression/sel_demo_large/trading_dates.csv | 10082f8bf7bd51714e8011f2199303043f74e51e7ab0a864daaa2dfca2a74a1e |
| data/regression/sel_demo_large/weights.csv | 1aa1f184bf4a31e4ad69b9a5928afcc040da1f6ff80d4493474884abedeaf7fa |
| data/regression/sel_demo_large_wt_parity/expected_trades.csv | fac43f19426665cc890e554a691a09aa6d022d5563a27968c105d8a512a84866 |
