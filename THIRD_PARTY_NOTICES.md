# Third-Party Notices

本仓库 vendored（随仓分发）与依赖的第三方组件及其许可证。各组件完整许可证文本见
`src/third_party/<组件>/` 目录内的 LICENSE 文件，或下列上游链接。

## Vendored（`src/third_party/`，随仓分发）

| 组件 | 用途 | 许可证 |
|---|---|---|
| [flatbuffers](https://github.com/google/flatbuffers) | 事件录制二进制序列化 | Apache-2.0 |
| [nlohmann_json](https://github.com/nlohmann/json) | JSON 解析/生成 | MIT |
| [spdlog](https://github.com/gabime/spdlog) | 日志（内嵌 bundled [fmt](https://github.com/fmtlib/fmt)，MIT） | MIT |
| [yaml-cpp](https://github.com/jbeder/yaml-cpp) | YAML 配置解析 | MIT |
| [prometheus-cpp](https://github.com/jupp0r/prometheus-cpp) | Prometheus 指标暴露（内嵌 bundled [civetweb](https://github.com/civetweb/civetweb)，MIT，编入 pull exposer；及 bundled googletest，BSD-3-Clause，仅上游测试用） | MIT |

上游 vendored 树可能各自内嵌其依赖（如 yaml-cpp 测试目录内的 googletest，BSD-3-Clause），
均随其树内 LICENSE 文件原样分发。

## CTP SDK（`src/third_party/CTP6.3.15/`）

- 版权归**上海期货信息技术有限公司**（Shanghai Futures Information Technology Co., Ltd.，上期技术）所有。
- 本仓库按业界通行做法（参照 WonderTrader 等开源交易框架）原样（as-is）附带 CTP API
  头文件与动态库，仅为方便编译对接 CTP 柜台；**不主张对其任何权利**，其使用受上期技术
  相关条款约束。
- 如上期技术要求移除，本仓库将移除该目录，届时使用者需自行从柜台服务商获取 SDK 并放置
  于相同路径。

## 系统依赖（不随仓分发，构建时需自行安装）

| 组件 | 用途 | 许可证 |
|---|---|---|
| [Boost](https://www.boost.org/)（asio / beast，header-only） | Binance WebSocket / 网络 | BSL-1.0 |
| [Apache Arrow / Parquet](https://arrow.apache.org/) (`libarrow-dev` / `libparquet-dev`) | 事件 parquet 读取 | Apache-2.0 |
| [libcurl](https://curl.se/) | Binance REST 客户端 | curl license（MIT 类） |
| OpenSSL | TLS（WebSocket / REST） | Apache-2.0（3.x；1.1.x 为 OpenSSL/SSLeay 双许可） |

## 数据

- `data/regression/` 内附带的期货分笔样例数据仅供回归测试与示例回测使用，不构成任何
  投资建议；数据为历史行情片段，版权归原交易所所有。
