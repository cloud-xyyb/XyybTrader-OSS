#pragma once

/// @file
/// @brief CTP TD login 失败退避的纯决策 helper（无副作用，gtest 直测）。

namespace ctp_login {

/// @brief 凭证/运营干预类登录错误：重试无意义，只有运营改密码/认证信息才能修复。
///
/// 集合按 vendored SDK `third_party/CTP6.3.15/error.xml` 官方定义：
///   3   INVALID_LOGIN               不合法的登录（账号/密码错）
///   48  INVALID_INVESTORIDORPASSWORD 无效的投资者或者密码（实盘柜台密码错主码）
///   63  AUTH_FAILED                 客户端认证失败（appid/authcode 错）
///   75  LOGIN_FORBIDDEN             连续登录失败次数超限（券商侧账号锁定）
///   131 WEAK_PASSWORD               弱密码须修改后登录
///   140 FIRST_LOGIN                 首次登录必须修改密码
///   141 PWD_OUT_OF_DATE             密码已过期
///   143 IP_FORBIDDEN                登录失败次数过多，IP 被禁（75 的 IP 级同构）
/// 剔除在外：142 PWD_MUST_DIFF 属改密码操作错非登录回包；4/49（未激活/IP
/// 白名单）重试同样徒劳但不会向锁定放大，维持瞬态档保守。
///
/// @note 凭证类走快退避不仅无效，还会把 3/48/63 类主动磨成 75/143 锁定
///       （2026-07-16 pf_a 实踩：密码错 5s 起跳指数重试 → 失败计数飙升）。
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

/// @brief 按最近 ErrorID 分级计算 login retry 退避。
///
/// @param login_retry_base_ms 配置基准（floor 1s——CTP login 请求按秒限速）。
/// @param err_id              最近一次登录失败的 CTP ErrorID。
/// @param fails               连续失败次数（登录成功清零）。
/// @return 凭证类固定 1h；NOT_INITED(7) 指数 cap 30s；其它瞬态错误指数 cap 5min。
inline int compute_login_backoff_ms(int login_retry_base_ms, int err_id, int fails) {
    constexpr int kCap5min        = 5 * 60 * 1000;
    constexpr int kCap30s         = 30 * 1000;
    constexpr int kCap1h          = 60 * 60 * 1000;
    constexpr int kCtpErrNotInited = 7;

    if (is_credential_class_login_error(err_id)) {
        // 运营干预才能修：固定 1h，防快速重试放大/再触发券商锁定。
        return kCap1h;
    }

    const int cap = (err_id == kCtpErrNotInited) ? kCap30s : kCap5min;
    int retry = login_retry_base_ms < 1000 ? 1000 : login_retry_base_ms;
    if (fails > 1) {
        // shift 上限防止长时间 retry 后移位溢出。
        const int shift = (fails - 1 < 12) ? (fails - 1) : 12;
        const long long scaled = static_cast<long long>(retry) << shift;
        retry = (scaled > cap) ? cap : static_cast<int>(scaled);
    }
    return retry;
}

}  // namespace ctp_login
