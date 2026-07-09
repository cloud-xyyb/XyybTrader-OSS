#pragma once

/// @brief 提供被动挂单入队时的 queue_ahead 估算。
///
/// `XtOrderManager` 在 submit/refresh 路径用当前价位可见总量查询本单前方排队量。
/// 返回值单位与 level_qty 一致，且应非负。
///
/// 预期在回测主线程串行调用；注入后实现对象不应再被外部修改。
class IQueueModel {
public:
    virtual ~IQueueModel() = default;

    virtual double estimate_queue_ahead(double level_qty,
                                        bool is_resubmit) const = 0;
};
