# 回归基线（Regression Baselines）

byte-identical 回归是本仓的行为不变量锚：4 个 HFT 场景，每个场景产出的 `fills.csv`
与仓内 golden **逐字节**比对，任何 drift 即失败。

```bash
bash scripts/regression_test.sh          # 全部 4 个场景
bash scripts/run_baselines.sh holiday    # 只跑一个
```

| 场景 | 描述 | config |
|---|---|---|
| `ag2206_only` | ag2206 单合约 5 交易日 | `configs/baselines/ag2206_only.yaml` |
| `ag2212_only` | ag2212 单合约 5 交易日 | `configs/baselines/ag2212_only.yaml` |
| `roll_29col`  | ag2206 + ag2212 双合约 5 交易日 | `configs/baselines/roll_29col.yaml` |
| `holiday`     | ag2206 + ag2212 跨节假日 8 交易日（含周末跳空） | `configs/baselines/holiday.yaml` |

目录约定：

- `baselines/<场景>/expected_fills.csv` — golden 输出
- `baselines/<场景>/manifest.yaml` — 自描述清单（config / 输入数据 / golden 的 sha256）
- `sample_ticks/` — 样例 tick 数据，`<交易所>/<品种>/<合约>/<日期>.csv`；
  列语义与 volume 口径见 [docs/data_format.md](../../docs/data_format.md)
