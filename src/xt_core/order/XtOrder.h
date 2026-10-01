#pragma once

#include <cstdint>
#include <cstring>
#include <type_traits>

#include "XtOffset.h"
#include "XtOrderTypes.h"

namespace xt_core {

/**
 * @brief 策略提交到执行层的订单意图 POD。
 *
 * 描述一次提交请求本身：合约、方向、价格、数量、TIF、开平语义、
 * 可选限价约束和用户标签。
 *
 * `code` 与 `tag` 使用固定数组；调用方通过 set_code()/set_tag() 写入，
 * 结构保持 trivially_copyable，供热路径按值或 memcpy 传递。
 */
struct XtOrder {
    /**
     * 合约码，固定 32 字节，null 终止，无堆分配。
     * 使用 set_code() 赋值，直接读取 code（等同 const char*）。
     */
    char code[32] = {};

    /** true = 买入，false = 卖出。 */
    bool is_buy = true;

    /**
     * 报单价（fixed-point，scale = kOrderExecFpScale）。
     * 执行层在入口处调用 order_exec_from_fp() 转换为 double 后使用。
     */
    int64_t price_fp = 0;

    /**
     * 报单量（fixed-point，scale = kOrderExecFpScale）。
     * 执行层在入口处调用 order_exec_from_fp() 转换为 double 后使用。
     */
    int64_t qty_fp = 0;

    /** 报单有效期（time-in-force）。 */
    XtTimeInForce tif = XtTimeInForce::GTC;

    /** 开平语义（open/close offset），详见 XtOffset。 */
    XtOffset close_offset = XtOffset::Open;

    /** 限价约束（fixed-point）。> 0 时：买单成交价不得高于 limit，卖单不得低于 limit；<= 0 表示市价语义。 */
    int64_t limit_price_fp = 0;

    /** 用户标签（如策略名、信号名），固定长度，null 终止。 */
    char tag[64] = {};

    /**
     * @brief 安全赋值合约码。
     * 最多复制 31 个字符并自动补 null 终止。
     */
    void set_code(const char* s) noexcept {
        if (s) {
            std::strncpy(code, s, sizeof(code) - 1);
            code[sizeof(code) - 1] = '\0';
        } else {
            code[0] = '\0';
        }
    }

    /** @brief 安全赋值标签。 */
    void set_tag(const char* s) noexcept {
        if (s) {
            std::strncpy(tag, s, sizeof(tag) - 1);
            tag[sizeof(tag) - 1] = '\0';
        } else {
            tag[0] = '\0';
        }
    }
};

} // namespace xt_core

// POD invariant: hot-path order intent type (char[32] code, char[64] tag, no std::string).
static_assert(std::is_trivially_copyable_v<xt_core::XtOrder>,
              "xt_core::XtOrder must remain trivially_copyable for hot-path transport");
