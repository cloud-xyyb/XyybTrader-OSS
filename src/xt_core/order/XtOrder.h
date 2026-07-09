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

    char code[32] = {};

    bool is_buy = true;

    int64_t price_fp = 0;

    int64_t qty_fp = 0;

    XtTimeInForce tif = XtTimeInForce::GTC;

    XtOffset close_offset = XtOffset::Open;

    int64_t limit_price_fp = 0;

    char tag[64] = {};

    void set_code(const char* s) noexcept {
        if (s) {
            std::strncpy(code, s, sizeof(code) - 1);
            code[sizeof(code) - 1] = '\0';
        } else {
            code[0] = '\0';
        }
    }

    void set_tag(const char* s) noexcept {
        if (s) {
            std::strncpy(tag, s, sizeof(tag) - 1);
            tag[sizeof(tag) - 1] = '\0';
        } else {
            tag[0] = '\0';
        }
    }
};

}

static_assert(std::is_trivially_copyable_v<xt_core::XtOrder>,
              "xt_core::XtOrder must remain trivially_copyable for hot-path transport");
