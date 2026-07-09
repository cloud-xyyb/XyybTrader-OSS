#pragma once

#include "xt_core/market/XtRawTick.h"

/**
 * @brief 回测行情读取器抽象（market data reader interface）。
 *
 * @details
 * 单消费者顺序 reader。`init()`、`reset()`、`next_tick()` 必须串行调用；
 * `next_tick(out)==true` 表示写出下一条 tick，返回 `false` 后用
 * `has_io_error()` 区分正常 EOF 与 I/O 错误。
 *
 * @warning 本接口不承诺并发安全，不得从多个线程同时调用读取或 reset 方法。
 */
class IMarketDataReader {
public:

    virtual ~IMarketDataReader() = default;

    [[nodiscard]] virtual bool init() = 0;
    virtual void reset() = 0;
    [[nodiscard]] virtual bool next_tick(XtRawTick& out) = 0;

    [[nodiscard]] virtual bool has_io_error() const { return io_error_; }

protected:
    bool io_error_ = false;
};
