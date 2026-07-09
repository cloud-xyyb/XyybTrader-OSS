# CTP SDK Notice

本目录内的 CTP API 头文件与动态库（`ThostFtdc*.h`、`linux/` 下的
`thostmduserapi_se.so` / `thosttraderapi_se.so` 等）版权归
**上海期货信息技术有限公司**（Shanghai Futures Information Technology Co., Ltd.）所有，
**不适用本仓库根目录的 MIT 许可证**。

- 本仓库按业界通行做法（参照 WonderTrader、vnpy 等开源交易框架的公开先例）
  **原样（as-is）附带**这些文件，仅为方便编译对接 CTP 柜台；对其不主张任何权利，
  未做任何修改。
- 其使用受上期技术及相关柜台服务商的条款约束；接入生产柜台前请确认你与
  期货公司之间的接入协议。
- 如版权方要求移除，本仓库将移除该目录。届时使用者需自行从 SimNow
  （https://www.simnow.com.cn/）或期货公司渠道获取同版本 SDK，解压至本目录
  相同路径即可恢复构建。

完整的第三方组件清单见仓库根目录 `THIRD_PARTY_NOTICES.md`。
