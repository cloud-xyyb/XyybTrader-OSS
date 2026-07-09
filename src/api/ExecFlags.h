#pragma once

#include <cstdint>

enum class ExecFlag : uint32_t {
    None       = 0,
    ReduceOnly = 1u << 0,
    PostOnly   = 1u << 1,
    Hidden     = 1u << 2,
    Market     = 1u << 3,
};

using ExecFlags = uint32_t;

constexpr ExecFlags exec_none() { return 0; }

constexpr bool has_flag(ExecFlags f, ExecFlag bit) {
    return (f & static_cast<uint32_t>(bit)) != 0;
}

constexpr ExecFlags with_flag(ExecFlags f, ExecFlag bit) {
    return f | static_cast<uint32_t>(bit);
}
