#include "BinanceWsUrl.h"

namespace binance_u {

BinanceWsUrl parse_binance_ws_url(std::string_view url, std::string_view default_port) {
    BinanceWsUrl out;
    std::string_view rest = url;

    if (auto p = rest.find("://"); p != std::string_view::npos) rest = rest.substr(p + 3);

    const auto slash = rest.find('/');
    const std::string_view host_port =
        (slash == std::string_view::npos) ? rest : rest.substr(0, slash);
    out.path_root = (slash == std::string_view::npos) ? "" : std::string(rest.substr(slash));

    const auto colon = host_port.find(':');
    if (colon == std::string_view::npos) {
        out.host = std::string(host_port);
        out.port = std::string(default_port);
    } else {
        out.host = std::string(host_port.substr(0, colon));
        out.port = std::string(host_port.substr(colon + 1));
    }
    return out;
}

}
