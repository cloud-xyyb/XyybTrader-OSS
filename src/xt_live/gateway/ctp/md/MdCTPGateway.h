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
    // ── 生命周期 ──────────────────────────────────────────────
    MdCTPGateway();
    ~MdCTPGateway() override;
    MdCTPGateway(MdCTPGateway&&) noexcept;
    MdCTPGateway& operator=(MdCTPGateway&&) noexcept;

    MdCTPGateway(const MdCTPGateway&) = delete;
    MdCTPGateway& operator=(const MdCTPGateway&) = delete;

    // ── CTP 专属配置（非虚，具体类接口）──────────────────────
    /// @brief 注入 CTP 专属配置（broker / flow_dir）；需在 init() 之前调用。
    /// @note ParserCTP 只读取已注入的 CTPMdCfg；provider-neutral Config 不承载
    ///       broker / flow_dir。
    void configure_ctp(const CTPMdCfg& ctp_cfg);

    // ── IMdGateway: 生命周期 ──────────────────────────────────────────────
    /** @brief 初始化底层 ParserCTP（init underlying parser）。 */
    bool init(const Config& cfg) override;
    /** @brief 启动底层 MD 通道。 */
    bool start() override;
    /** @brief 停止底层 MD 通道。 */
    void stop() override;

    // ── IMdGateway: 能力与回调 ──────────────────────────────────────────────
    /** @brief 返回 CTP MD 通道能力描述（channel capabilities）。 */
    Capabilities capabilities() const override;
    /// @brief 绑定统一行情回调包，并转发给底层 `ParserCTP`。
    /// @note 必须在 `start()` 前完成绑定；运行中重绑不保证与 SDK 回调线程同步。
    void bind_callbacks(Callbacks callbacks) override;

    // ── IMdGateway: 健康检查 ──────────────────────────────────────────────
    /// @brief 检查行情通道健康度，必要时由底层 `ParserCTP` 重建 SDK 会话。
    /// @param now_ms 当前 steady clock 毫秒时间戳，由控制线程传入。
    /// @return true 表示通道健康或尚未达到 stale 阈值；false 表示本次触发重建。
    bool check_health(uint32_t now_ms) override;

private:
    /// @brief 隐藏 `ParserCTP` 实现细节，减少 header 对 CTP SDK 适配细节的暴露。
    class Impl;
    std::unique_ptr<Impl> impl_;  ///< owning PIMPL；生命周期随 `MdCTPGateway`
};
