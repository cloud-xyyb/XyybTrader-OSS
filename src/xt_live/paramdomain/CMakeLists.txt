cmake_minimum_required(VERSION 3.5)
project(XtParamDomain LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)

# ── xt_paramdomain — 线程安全 KV 参数底座（memory + POSIX shm 后端）─────────────
# frame-loop 每帧读 + panel(write RPC) 写共用，必须位于 control / xt_panel 之下，
# 否则 xt_panel → runner-core 成环。零业务依赖：仅 STL + libc/POSIX
# (semaphore / mmap / flock)，不 link 任何 xt_* 业务 lib。
add_library(xt_paramdomain STATIC
    XtLiveParamDomain.cpp
)

target_include_directories(xt_paramdomain PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}   # bare `#include "XtLiveParamDomain.h"`
    ${CMAKE_SOURCE_DIR}           # full-path `xt_live/paramdomain/XtLiveParamDomain.h`
)
