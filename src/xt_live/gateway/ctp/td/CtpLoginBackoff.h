#pragma once

/// @file
/// @brief CTP TD login 失败退避的纯决策 helper（无副作用，gtest 直测）。

namespace ctp_login {

inline bool is_credential_class_login_error(int err_id) {
    switch (err_id) {
        case 3:
        case 48:
        case 63:
        case 75:
        case 131:
        case 140:
        case 141:
        case 143:
            return true;
        default:
            return false;
    }
}

inline int compute_login_backoff_ms(int login_retry_base_ms, int err_id, int fails) {
    constexpr int kCap5min        = 5 * 60 * 1000;
    constexpr int kCap30s         = 30 * 1000;
    constexpr int kCap1h          = 60 * 60 * 1000;
    constexpr int kCtpErrNotInited = 7;

    if (is_credential_class_login_error(err_id)) {

        return kCap1h;
    }

    const int cap = (err_id == kCtpErrNotInited) ? kCap30s : kCap5min;
    int retry = login_retry_base_ms < 1000 ? 1000 : login_retry_base_ms;
    if (fails > 1) {

        const int shift = (fails - 1 < 12) ? (fails - 1) : 12;
        const long long scaled = static_cast<long long>(retry) << shift;
        retry = (scaled > cap) ? cap : static_cast<int>(scaled);
    }
    return retry;
}

}
