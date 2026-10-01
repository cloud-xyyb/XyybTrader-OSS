#pragma once

/// @file
/// @brief MD connector sidecar 文件路径推导（writer/reader 同源）。
///
/// connector（writer）与 `XtShmMdGateway`（reader）用同一份规整逻辑定位与 shm ring
/// 关联的 sidecar 文件（单实例 `.lock`、订阅集 `.subs`），避免两侧命名漂移导致
/// reader 找不到 writer 写的文件。ring 名形如 `/xt_md_ctp`，前导斜杠不适合做文件名，
/// 统一把 `/` 替 `_`。

#include <string>

namespace xt_ipc {

/// @brief shm ring 名 → sidecar 文件 basename（不含目录与扩展名）。
/// @param ring_name shm 段名（如 `/xt_md_ctp`）。
/// @return `xt_md_connector` + 规整后的 ring 名（`/`→`_`），如 `xt_md_connector_xt_md_ctp`。
inline std::string md_sidecar_basename(const std::string& ring_name) {
    std::string base = "xt_md_connector";
    for (char c : ring_name) base += (c == '/') ? '_' : c;
    return base;
}

/// @brief 完整 sidecar 路径 = dir + "/" + basename + ext。
/// @param dir       sidecar 目录（connector 解析后的 lock 目录，如 `/run/xyybtrader`）。
/// @param ring_name shm 段名。
/// @param ext       扩展名（含点，如 `.lock` / `.subs`）。
inline std::string md_sidecar_path(const std::string& dir,
                                   const std::string& ring_name,
                                   const char* ext) {
    return dir + "/" + md_sidecar_basename(ring_name) + ext;
}

}  // namespace xt_ipc
