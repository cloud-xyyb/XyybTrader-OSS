#include "ctp/md/MdCTPGateway.h"

#include "ctp/md/ParserCTP.h"

#include <memory>
#include <utility>

class MdCTPGateway::Impl {
public:
    ParserCTP ctp;
};

MdCTPGateway::MdCTPGateway()
    : impl_(std::make_unique<Impl>()) {}

MdCTPGateway::~MdCTPGateway() = default;

MdCTPGateway::MdCTPGateway(MdCTPGateway&&) noexcept = default;

MdCTPGateway& MdCTPGateway::operator=(MdCTPGateway&&) noexcept = default;

void MdCTPGateway::configure_ctp(const CTPMdCfg& ctp_cfg) {
    impl_->ctp.configure_ctp(ctp_cfg);
}

bool MdCTPGateway::init(const Config& cfg) {
    return impl_->ctp.init(cfg);
}

bool MdCTPGateway::start() {
    return impl_->ctp.start();
}

void MdCTPGateway::stop() {
    impl_->ctp.stop();
}

IMdGateway::Capabilities MdCTPGateway::capabilities() const {

    return Capabilities{
        false,
    };
}

void MdCTPGateway::bind_callbacks(Callbacks callbacks) {
    impl_->ctp.bind_callbacks(std::move(callbacks));
}

bool MdCTPGateway::check_health(uint32_t now_ms) {
    return impl_->ctp.check_health(now_ms);
}
