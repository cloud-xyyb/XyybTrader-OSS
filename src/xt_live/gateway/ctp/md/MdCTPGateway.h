#pragma once

#include <memory>

#include "api/IMdGateway.h"
#include "ctp/md/CTPMdCfg.h"

/// @brief CTP 行情网关的对外接口层，实现 IMdGateway。
///
/// MdCTPGateway 只负责 IMdGateway 的调用转发；
/// 行情登录、订阅、解析、重连全在底层 ParserCTP。
/// @warning callbacks 在 CTP SDK 回调线程同步执行，必须快速返回、只做入队，
/// 不得反向调用 gateway 生命周期接口或阻塞 I/O。
class MdCTPGateway : public IMdGateway {
public:

    MdCTPGateway();
    ~MdCTPGateway() override;
    MdCTPGateway(MdCTPGateway&&) noexcept;
    MdCTPGateway& operator=(MdCTPGateway&&) noexcept;

    MdCTPGateway(const MdCTPGateway&) = delete;
    MdCTPGateway& operator=(const MdCTPGateway&) = delete;

    void configure_ctp(const CTPMdCfg& ctp_cfg);

    bool init(const Config& cfg) override;

    bool start() override;

    void stop() override;

    Capabilities capabilities() const override;

    void bind_callbacks(Callbacks callbacks) override;

    bool check_health(uint32_t now_ms) override;

private:
    /// @brief 隐藏 `ParserCTP` 实现细节，减少 header 对 CTP SDK 适配细节的暴露。
    class Impl;
    std::unique_ptr<Impl> impl_;
};
