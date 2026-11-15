# Prometheus 热重载与进程管理（test_mock/hotReload）

## 一、功能概述

在 Prometheus 原有能力之上，增加基于 `prometheus.pid` 文件的进程管理能力，实现以下功能：

| 编号 | 功能 | 说明 |
|------|------|------|
| 1 | 启动记录 pid | Prometheus 启动时，在其运行目录下创建 `prometheus.pid` 文件，写入当前进程号 |
| 2 | 退出删除 pid | Prometheus 退出时（SIGTERM / Ctrl+C / 内部错误退出），删除 `prometheus.pid` 文件 |
| 3 | 子命令 reload | 执行 `prometheus reload`，读取 `prometheus.pid` 中的进程号，向该进程发送 **SIGHUP** 信号，触发配置热重载 |
| 4 | 子命令 stop | 执行 `prometheus stop`，读取 `prometheus.pid` 中的进程号，向该进程发送 **SIGTERM** 信号，触发优雅退出 |
| 5 | 禁用生命周期 API | 调用 `/-/reload`、`/-/quit` 接口时返回 **403** 和禁用信息 `Lifecycle API is not enabled.` |

## 二、目录结构与文件说明

```
test_mock/hotReload/
├── README.md     # 本说明文档
├── manager.go    # Manager：pid 文件管理 + 信号发送（package hotReload）
└── xfile.go      # 文件工具函数：存在性检查 / 创建写入 / 删除 / 读取（package hotReload）

cmd/prometheus/main.go   # 集成点（见下文"关键实现"）
```

## 三、整体流程

```
                         ┌─────────────────────────────┐
                         │   prometheus（启动，无子命令） │
                         └──────────────┬──────────────┘
                                        │ CreateAndRecordPid()
                                        ▼
                          运行目录创建 prometheus.pid（写入 pid）
                                        │
                              run.Group 启动各组件
                                        │
              ┌─────────────────────────┼──────────────────────────┐
              │                         │                          │
     收到 SIGHUP(kill -1)      收到 SIGTERM / Ctrl+C          组件异常退出
     Prometheus 内部 reload     优雅退出                       优雅退出
              │                         │                          │
              └─────────────────────────┴──────────────────────────┘
                                        │ Termination 清理函数
                                        ▼
                              RemovePid() 删除 prometheus.pid
```

```
  prometheus reload ──► 读取 prometheus.pid ──► 对目标 pid 发送 SIGHUP ──► 目标进程热重载配置
  prometheus stop  ──► 读取 prometheus.pid ──► 对目标 pid 发送 SIGTERM ──► 目标进程优雅退出并清理 pid 文件
```

## 四、关键实现说明

### 4.1 cmd/prometheus/main.go（集成点）

1. **子命令分发**（`main()` 入口处）：
   - 在 kingpin 解析 flag 之前，检查 `os.Args[1]` 是否为 `reload` / `stop`；
   - 命中则调用 `manager.Reload()` / `manager.Stop()` 后直接退出，不启动服务；
   - 其他情况（含无参数）走正常启动流程（**已做 `len(os.Args) > 1` 越界保护**）。

2. **启动写 pid**（`g.Run()` 之前）：
   ```go
   if err := manager.CreateAndRecordPid(); err != nil { ... os.Exit(1) }
   ```
   pid 文件已存在时视为"进程已在运行"，启动失败，防止重复启动。

3. **退出删 pid**（放在 Termination handler 的清理回调中，而不是 `defer`）：
   - 原因：`g.Run()` 返回错误时走 `os.Exit(1)`，`defer` 不会执行，pid 文件会残留；
   - run.Group 收尾时无论哪种退出路径（信号、Web 退出、组件报错）都会执行清理回调，保证 pid 文件被删除。

### 4.2 test_mock/hotReload/manager.go

| 方法 | 功能 |
|------|------|
| `NewManager()` | 获取当前工作目录，拼出 pid 文件路径 `<cwd>/prometheus.pid` |
| `CreateAndRecordPid()` | pid 文件不存在则创建并写入当前进程号；已存在则返回错误（防重复启动） |
| `RemovePid()` | 删除 pid 文件（幂等：文件不存在时返回 nil） |
| `Reload()` | 读 pid 文件 → 向目标进程发送 `syscall.SIGHUP` |
| `Stop()` | 读 pid 文件 → 向目标进程发送 `syscall.SIGTERM` |
| `getPID()` / `signal()` | 内部方法：读取 pid 内容并转换为 int / 查找进程并发送信号 |

### 4.3 test_mock/hotReload/xfile.go

文件操作工具函数：`FileExist` / `FileCreateAndWrite` / `FileDelete` / `FileRead`。
其中 `FileRead` 会去除末尾换行符，保证 `strconv.Atoi` 能正确解析 pid。

### 4.4 生命周期 API 禁用（403）

Prometheus 的 `--web.enable-lifecycle` flag **默认为 false**，此时 `web/web.go` 对
`/-/reload`、`/-/quit` 注册的是统一拒绝处理器：

```go
forbiddenAPINotEnabled := func(w http.ResponseWriter, _ *http.Request) {
    w.WriteHeader(http.StatusForbidden)               // 403
    w.Write([]byte("Lifecycle API is not enabled."))  // 禁用信息
}
```

因此只要**不开启** `--web.enable-lifecycle`（默认即不开启），即可满足"返回 403 + 禁用信息"的要求，
热重载只能通过子命令 `prometheus reload`（SIGHUP）触发。

## 五、使用示例

```bash
# 1. 启动（自动创建 prometheus.pid）
./prometheus --config.file=prometheus.yml

# 2. 热重载配置（向 pid 进程发送 SIGHUP）
./prometheus reload

# 3. 停止（向 pid 进程发送 SIGTERM）
./prometheus stop

# 4. 生命周期 API 已被禁用
curl -X POST http://127.0.0.1:9090/-/reload   # 403 Lifecycle API is not enabled.
curl -X POST http://127.0.0.1:9090/-/quit     # 403 Lifecycle API is not enabled.
```

对应系统信号（Linux）：

| 操作 | 信号 | 信号值 |
|------|------|--------|
| reload | SIGHUP | 1 |
| Ctrl+C | SIGINT | 2 |
| stop / kill | SIGTERM | 15 |

## 六、已知限制

1. **Windows 不支持进程信号**：`os.FindProcess(pid).Signal(...)` 在 Windows 上除 `Kill` 外均返回
   不支持错误，本功能面向 Linux/Unix 环境。
2. **pid 文件路径取自工作目录**（`os.Getwd()`），而非可执行文件所在目录；建议在固定目录下启动。
3. **异常宕机（kill -9 / 断电）不会删除 pid 文件**，下次启动会因 pid 文件存在而拒绝启动，
   需手动删除 `prometheus.pid`。
4. `FileRead` 单次最多读取 1024 字节，仅适用于 pid 这类极小的文件。
