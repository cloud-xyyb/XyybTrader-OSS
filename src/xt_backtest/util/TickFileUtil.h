#pragma once

/// @file
/// @brief tick 文件发现、cache path 和 manifest helper。
///
/// 本文件提供 CSV tick 文件集合推导、bin cache 路径生成和 manifest signature 校验；

#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

#include "xt_core/market/XtRawTick.h"
#include "TickCsvUtil.h"

/// @brief tick 归并队列元素。
///
/// `key` 是 make_tick_sort_key() 生成的时序排序键，`seq` 用于同 key 时保持
/// 多文件输入的稳定顺序。
struct XtTickWithKey {
    XtRawTick tick;
    uint64_t key = 0;
    uint64_t seq = 0;
};

std::string sanitize_filename_part(const std::string& s);

std::string build_tick_cache_path(const std::string& cache_dir,
                                  uint32_t start_date,
                                  uint32_t end_date,
                                  const std::vector<std::string>& tick_symbols,
                                  double qty_base,
                                  XtTickVolumeMode vol_mode,
                                  const std::vector<std::string>& sources);

bool extract_date_from_filename(const std::string& name, uint32_t& out_date);

constexpr uint32_t kTickNightCarryGuardDays = 15;

uint32_t derive_night_carry_guard_days(std::vector<uint32_t> filename_days);

uint32_t derive_night_carry_guard_days_for_paths(const std::vector<std::string>& paths);

void append_tick_files_from_root(const std::string& root,
                                 uint32_t start_date,
                                 uint32_t end_date,
                                 std::vector<std::string>& out);

void append_all_tick_files_from_root(const std::string& root, std::vector<std::string>& out);

bool find_first_symbol_in_files(const std::vector<std::string>& files,
                                std::string& out,
                                double qty_base,
                                XtTickVolumeMode vol_mode);

std::vector<std::string> filter_files_for_auto_main(const std::vector<std::string>& files,
                                                    uint32_t start_date);

uint64_t compute_source_signature(const std::vector<std::string>& sources,
                                  double qty_base,
                                  XtTickVolumeMode vol_mode,
                                  uint32_t start_day,
                                  uint32_t end_day);

bool write_cache_manifest(const std::string& bin_path, uint64_t signature);

bool verify_cache_manifest(const std::string& bin_path,
                           const std::vector<std::string>& sources,
                           double qty_base,
                           XtTickVolumeMode vol_mode,
                           uint32_t start_day,
                           uint32_t end_day);
