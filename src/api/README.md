# src/api placement rules

`src/api/` 只放全仓共享、provider-neutral、模块无单一 ownership，或刻意用于打断依赖方向的稳定契约。这里不是“所有接口”的默认目录。

## 可以放在这里

- 跨 venue / 跨引擎的 gateway、strategy、risk、execution 抽象，例如 `ITradeGateway`、`IMdGateway`、`IExecutionGateway`、`IStrategyContext`。
- 策略 ABI、运行时 DTO、跨模块快照和事件载荷，例如 `StrategySid`、`XtRiskSnapshot`、`XtPositionSnapshot`、`BrokerVenueTruth`、`AliveInflightOrder`。
- 为了解耦上层编排与下层实现而定义的中立 sink/provider，例如 `IRejectSink`、`IKillSwitchSink`、`IFeeRuleProvider`。
- 多个模块都必须直接包含，且无法归属于某个业务模块的基础契约。

## 不放在这里

- 具体实现、factory、adapter、parser、writer、transport helper。
- 只服务某个模块的接口或 sink。例：`IReportSink` 属于 report 写出契约，放在 `src/xt_report/core/IReportSink.h`。
- venue-specific SDK/REST/WebSocket 适配层内部类型。
- 只为了缩短 include path 而新增的 forwarding header。
- 模块已经有清晰 ownership 的 DTO 或 helper；这类文件优先放回 `src/<module>/`。

## 模块内 public 契约

模块拥有的 public 数据结构或接口放在模块目录下：

- 模块内部跨子目录共享：`src/<module>/core/`、`src/<module>/api/`、`src/<module>/common/` 按现有模块约定选择。
- 调用方只需要该模块能力时，优先 include 模块路径，例如 `xt_report/core/IReportSink.h`。
- 只有当契约已经变成多个上层模块共同依赖、且不再属于原模块所有权时，才考虑迁入 `src/api/`。

## folder 规则

当前 `src/api/` 保持扁平结构，避免 include path 二次迁移和 forwarding header 增殖。只有满足以下条件时才考虑分 folder：

- 文件数量或职责已经形成稳定分组，且现有命名无法表达边界。
- 能给出完整迁移清单、兼容策略和删除 forwarding header 的时间点。
- 迁移后不会把模块 ownership 模糊化。

## 新增文件检查清单

新增到 `src/api/` 前先确认：

1. 是否有明确的跨模块调用方。
2. 是否 provider-neutral，不绑定某个 venue、reporter、recorder 或 runner 实现。
3. 是否属于稳定契约，而不是某个模块的临时 helper。
4. 是否会让低层模块反向依赖高层模块；如果只是为了解依赖环，优先评估模块内归位或拆出更小的中立类型。
5. 是否需要同步更新相关 module context 和架构文档。
