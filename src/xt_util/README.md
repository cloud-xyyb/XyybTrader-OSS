# xt_util 目录分层

`xt_util` 只放跨模块复用、无业务 ownership 的轻量工具。新增工具优先放入现有子目录：

- `time/`: 时间编码（date/time ↔ ns）、HHMM 分钟算术与区间判断、steady clock
  （`XtSteadyClock`，唯一时钟入口；纯 `std::chrono::steady_clock`，不走 RDTSC 直读——
  云 VM 跨核 TSC offset 不保证对齐）。
- `runtime/`: 进程/线程调优、shutdown sleep、systemd notify、live tap TLS、固定窗口限流。
- `string/`: 冷路径 string 工具、热路径 C-string helper、secret string 及 yaml-cpp 适配。
- `path/`: 路径拼接、目录探针、canonical root 校验、atomic file write。
- `stats/`: 延迟 histogram 与滚动窗口统计工具。
- `net/`: listen_addr loopback 判定与 host 提取。
- `indicator/`, `sync/`: 已有领域明确的 util 子模块，保持独立。

避免在顶层新增 `.h/.cpp`。如果一个 helper 只被单个 `.cpp` 使用，优先放入该文件
anonymous namespace；只有两个以上真实模块 owner 需要时，才提升到 `xt_util`。
