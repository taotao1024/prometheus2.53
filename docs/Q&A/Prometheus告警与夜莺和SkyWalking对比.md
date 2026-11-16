# Prometheus 告警 vs 夜莺（Nightingale）vs SkyWalking 告警对比

> 三者定位不同：**Prometheus 的告警是"规则评估引擎 + 独立通知组件"的组合**，
> **夜莺（Nightingale，n9e）是平台化的一体化告警系统**（数据源查询、规则评估、事件管理、通知全内置），
> **SkyWalking 是 APM/可观测平台**（Trace 优先，指标/日志/告警一体化）。

## 一、核心对比（Prometheus vs 夜莺 vs SkyWalking）

| 维度 | Prometheus | 夜莺（n9e） | SkyWalking |
|------|------------|-------------|------------|
| **架构** | 规则评估在 Prometheus 内（`rules/` 包），通知由**独立的 Alertmanager** 完成，两个组件分开部署 | 中心化平台：server 内置告警引擎 + Web UI + 通知，无需额外组件；采集端配 `categraf` | OAP server **内置告警引擎**（无独立告警组件），随 OAP 集群部署 |
| **定位** | 指标监控系统：TSDB + PromQL + 规则引擎 | 告警运维平台：数据源查询 + 告警评估 + 通知闭环 | APM/Tracing 平台：链路追踪为核心，指标/日志/剖析一体化 |
| **核心数据模型** | 时序指标（label 维度） | 事件（告警历史）+ 对接外部数据源的指标查询 | **Trace/Span 优先**，指标由 OAL 从 trace 聚合而来 |
| **采集方式** | **pull**（HTTP 拉取 `/metrics`） | pull（查询外部数据源）+ categraf push | **push**（Java 等 agent 字节码自动插桩，gRPC 上报） |
| **规则定义方式** | YAML 规则文件（`rule_files`），纯代码化，需维护文件与 reload | Web UI 拖拽/表单配置，规则持久化在 MySQL；也支持 Terraform/API 管理 | YAML 规则文件（OAL/MAL 规则），需分发到 OAP 节点并重启生效 |
| **规则类型** | 只有 PromQL 表达式（记录规则 + 告警规则），一切靠写 PromQL | 阈值告警、同比/环比、nodata、主机告警（内置 CPU/内存/磁盘等）、PromQL 告警（兼容）、日志告警（新版）等 | OAL/MAL 阈值规则（针对内置聚合指标与外部 meter 指标），表达力弱于 PromQL |
| **告警粒度** | **每条 time series 一条告警**（表达式的每个结果序列都触发），序列爆炸靠 Alertmanager `group_by` 聚合 | 规则内直接配置 `group by` 维度聚合，按维度聚合成一条事件，天然防爆量 | 服务/实例/端点/拓扑维度（APM 视角，如"某服务 SLO 违约"、"节点间链路异常"） |
| **告警状态机** | `inactive → pending → firing`，靠 `for` 持续时间确认；状态在内存，重启靠 `<tsdb.path>/alerts` 文件恢复 | 事件有独立生命周期并**持久化到数据库**：触发 → 认领 → 处理 → 关闭，支持闭环管理与值班流转 | 无 pending 概念：`period` 窗口内命中次数 + `silence-period` 静默，无持久化闭环 |
| **通知渠道** | 无内置渠道；Alertmanager 通过 `route`/`receiver` 分发，邮件/webhook 原生，钉钉/飞书/企微/短信电话要靠 webhook 对接或第三方网关 | **内置**钉钉、飞书、企微、邮件、Slack、Telegram、短信、电话、webhook、FlashDuty 等，UI 直接配置 | 内置 webhook 处理器：钉钉、飞书、企微、Slack、PagerDuty 等，配置简单但路由/降噪弱 |
| **降噪能力** | Alertmanager：分组、`group_wait/interval`、抑制、静默 | 屏蔽规则（按时间/标签生效）、生效时间段、告警升级、聚合收敛、订阅规则、恢复通知 | 基础降噪（silence-period） |
| **告警历史** | 无持久化事件表；`ALERTS`/`ALERTS_FOR_STATE` 是时序数据，Alertmanager 也不存历史 | 每次触发/恢复都是数据库里的事件记录，可查询历史、统计 | 有告警消息记录，可在 UI 查看 |
| **多租户/权限** | 无；靠外部（如 Thanos/Mimir/AM 集群 + 前端）补 | 内置业务组（BG）、成员、权限、值班体系 | OAP 集群可按环境隔离，无内置权限/值班体系 |
| **HA** | 规则评估天然单实例（跑双份会重复告警）；Alertmanager 自带 gossip 集群 HA | server 支持多副本 + 数据库，告警引擎按规则分片，天然 HA | OAP 集群，告警规则在节点间自动分片 |
| **数据源** | 只查自己的 TSDB（或通过 Thanos/Mimir 查全局） | 数据源可挂多个 Prometheus / VictoriaMetrics / ELK 等，跨源告警 | 自有存储（BanyanDB/ES 等）；可通过 receiver 接入 Prometheus/OTLP 指标 |
| **配置可版本化** | ✅ YAML 进 Git，code review 友好 | ⚠️ 以 UI/API 为主，GitOps 生态弱一些 | ✅ YAML 进 Git |

### 本质差异总结

1. **拉取式引擎 vs 平台化系统**：Prometheus 告警是 pull 监控体系的自然延伸——规则本质是"持续运行的 PromQL"，优势是**与查询语言完全一致**（告警条件 = 你能在图表上验证的表达式）；
2. **通知职责分离 vs 内置**：Prometheus 刻意把"检测"（Prometheus）与"路由/降噪/通知"（Alertmanager）解耦，各司其职但部署运维复杂；夜莺把检测、事件、通知、值班合在一起，开箱即用；
3. **事件模型**：Prometheus 的告警是**内存中的时序状态**（firing/pending），没有"处理闭环"概念；夜莺的事件是数据库记录，带认领/处理人/关闭，更贴近国内运维的值班/工单流程；
4. **生态关系**：夜莺原生兼容 Prometheus 生态（categraf 可替换 node_exporter 等，数据可写 VictoriaMetrics/Prometheus，也支持直接对 Prometheus 数据源写 PromQL 规则），很多团队的实际组合是：**Prometheus/categraf 采集 + 夜莺做统一告警平台**。

## 二、加入 SkyWalking 对比

三者根本定位不同：**Prometheus 是指标监控系统（pull 时序数据库 + 规则引擎）**，
**SkyWalking 是 APM/可观测平台（Trace 优先，指标/日志/告警一体化）**，
**夜莺是告警平台化方案**。

### 1. 定位对比

| 维度 | Prometheus | SkyWalking | 夜莺（n9e） |
|------|------------|------------|-------------|
| **本质** | 指标监控系统：TSDB + PromQL + 规则引擎 | APM/Tracing 平台：链路追踪为核心，指标/日志/剖析一体化 | 告警运维平台：数据源查询 + 告警评估 + 通知闭环 |
| **核心数据模型** | 时序指标（label 维度） | **Trace/Span 优先**，指标由 OAL 语言从 trace 聚合而来，也有 log、meter | 事件（告警历史）+ 对接外部数据源的指标查询 |
| **采集方式** | **pull**（HTTP 拉取 `/metrics`） | **push**（Java 等 agent 字节码自动插桩，gRPC 上报） | pull（对 Prometheus/VictoriaMetrics 数据源查询）+ categraf push |
| **典型场景** | 基础设施/服务指标监控 | 微服务全链路追踪、性能诊断、拓扑分析 | 告警值班与事件闭环管理 |

### 2. 告警机制对比

| 维度 | Prometheus | SkyWalking | 夜莺 |
|------|------------|------------|------|
| **告警引擎位置** | Prometheus 内 + 独立 Alertmanager | **OAP server 内置**（无独立告警组件） | 夜莺 server 内置 |
| **规则语言** | **PromQL**（完整表达式语言，任意计算） | **OAL/MAL 规则**：对内置聚合指标（服务响应时间、成功率等）做 `threshold + period + silence` 比较，或 MAL 分析外部 meter 指标；表达能力远弱于 PromQL | 表单化阈值/同环比/nodata + PromQL（对 Prom 数据源） |
| **规则示例** | `expr: http_error_rate > 0.05, for: 5m` | `expression: sum(service_resp_time > 1000), period: 10, silence-period: 5`（10 分钟内 5 次超 1s） | UI 配置阈值/持续时间/恢复条件 |
| **状态机** | `inactive → pending(for) → firing`，有恢复通知 | 无 pending 概念：`period` 窗口内命中次数 + `silence-period` 静默 | 触发/恢复 + 认领/处理/关闭（事件闭环） |
| **通知渠道** | 无内置，Alertmanager 路由分发（webhook 原生） | 内置 webhook 处理器：钉钉、飞书、企微、Slack、PagerDuty 等，配置较简单但路由/降噪弱 | 内置最丰富：钉钉/飞书/企微/短信/电话/邮件/Telegram/webhook/FlashDuty |
| **降噪** | Alertmanager 分组/抑制/静默（需写路由 YAML） | 基础（silence-period） | 屏蔽规则、生效时间、聚合收敛、升级 |
| **告警历史** | 无持久化事件表（`ALERTS` 只是时序） | 有告警消息记录，可在 UI 查看 | 数据库持久化事件 + 统计 |
| **告警对象** | 任意 label 组合的时序 | **服务/实例/端点/拓扑维度**（APM 视角，如"某服务 SLO 违约"、"节点间链路异常"） | 任意数据源查询维度 + 主机维度 |
| **HA** | 规则评估单实例，AM gossip 集群 | OAP 集群，告警规则在节点间分片（自动） | server 多副本 + 分库分表 |

### 3. 本质差异总结

1. **数据模型决定告警形态**：
   - Prometheus：告警条件 = **任意 PromQL 表达式**，理论上能对任何可查询的指标写告警，灵活性最强，但每条 series 一条告警、靠 Alertmanager 收敛；
   - SkyWalking：指标从 **trace 聚合而来**（OAL 预定义：服务 RT、成功率、端点 P99 等），告警规则是对这些**内置指标做阈值判断**——表达不了复杂计算，换来的是"开箱即有的 SLO 告警模板"和链路视角（如"服务 A→B 链路异常"这种 Prometheus 没有原生概念的告警对象）；
2. **组件解耦程度相反**：Prometheus 检测与通知分离（两个组件、两种配置），SkyWalking 与夜莺都是单平台内置（部署简单）；
3. **互补而非替代**：SkyWalking 与 Prometheus 在实践中常**同时存在**——APM 追踪用 SkyWalking（Java/Go agent 插桩），指标监控用 Prometheus；SkyWalking 甚至支持接收 Prometheus 指标（Prometheus receiver/OTLP）和把告警/指标转发出去（OpenTelemetry 导出）。夜莺则常作为前两者的告警平台层（挂 Prometheus/VictoriaMetrics 数据源）。

## 三、告警流程对比分析（端到端）

### 1. Prometheus 告警流程（检测与通知分离，两段式）

```
 ① 数据采集（pull）
    scrape 每个目标一个 scrapeLoop，按 interval 拉取 → 解析 → relabel → 写入 TSDB
        │
        ▼
 ② 规则评估（Prometheus 内，rules/ 包）
    rule_files 加载 → Group 按 evaluation_interval 错峰评估
    AlertingRule.Eval：执行 PromQL
        │  结果序列数 = 告警对象数（每 series 一条）
        ▼
 ③ 状态机（Prometheus 内存中）
    expr 满足 且 for 未到 → PENDING（告警但不通知）
    expr 持续满足 ≥ for  → FIRING，写入 ALERTS/ALERTS_FOR_STATE 序列到 TSDB
    expr 不再满足        → resolved（EndsAt）
        │
        ▼
 ④ 模板渲染（template/ 包）
    annotations/labels 中的 {{ $labels.xxx }}、{{ $value }} 渲染
        │
        ▼
 ⑤ 推送（rules.SendAlerts → notifier/ 包）
    alert_relabel_configs 重标 → 内存队列（64 条/批）→ 并发 POST 各 Alertmanager
    （至少一个 AM 成功即不丢弃；AM 侧靠 resend-delay 重发兜底）
        │
        ▼
 ⑥ 通知处理（独立组件 Alertmanager）
    分组 group_by → group_wait/group_interval → 抑制/静默 → 路由树匹配
    → receiver（邮件/webhook/…）→ 重发 repeat_interval
```

**特点**：检测与通知跨**两个进程**、两种配置；中间靠内存队列衔接（无持久化）；状态机精细（for 确认防抖）但事件无闭环。

### 2. 夜莺告警流程（平台化，单组件闭环）

```
 ① 数据接入
    categraf/telegraf 采集 → 推送夜莺（或直写 TSDB/Prometheus）
    夜莺 server 仅作为"查询客户端"挂载多个数据源（Prometheus/VictoriaMetrics/ELK…）
        │
        ▼
 ② 规则定义（Web UI / MySQL 持久化）
    表单配置：阈值/同环比/nodata/PromQL + 严重等级 + 生效时间 + 屏蔽规则 + group by 维度
        │
        ▼
 ③ 规则评估（server 内置告警引擎，规则分片到多副本）
    周期查询数据源 → 按 group by 聚合成事件（防 series 爆炸）
        │
        ▼
 ④ 事件持久化（MySQL/PostgreSQL）
    触发/恢复都落库 → 事件 ID、严重度、处理状态可查询、可统计
        │
        ▼
 ⑤ 通知（内置渠道）
    屏蔽规则过滤 → 订阅/聚合收敛 → 告警升级 → 按通知配置直接发
    钉钉/飞书/企微/邮件/短信/电话/webhook…（无需第三方网关）
        │
        ▼
 ⑥ 闭环（UI 操作）
    认领 → 处理 → 关闭；值班/工单流转；FlashDuty 可承接升级
```

**特点**：全流程**单组件内闭环**；事件落库是最大差异点——告警是"记录"而非"时序状态"；UI 驱动，GitOps 弱。

### 3. SkyWalking 告警流程（APM 数据驱动，引擎内置）

```
 ① 数据采集（push）
    Java/Go agent 字节码插桩 → gRPC 上报 Trace/Meter 到 OAP server
    OAL 引擎从 trace 聚合出指标（服务 RT、成功率、端点 P99…）
        │
        ▼
 ② 指标存储
    BanyanDB/ES 等后端存储聚合指标
        │
        ▼
 ③ 规则定义（alert-settings.yml）
    OAL 规则：name + expression（如 sum(service_resp_time > 1000)）
            + period（窗口内评估次数）+ silence-period + tags
        │
        ▼
 ④ 规则评估（OAP 集群内置，规则自动分片）
    每个周期：对指定 metric 执行 expression → 达到 period 内命中阈值即触发
    （无 for/pending：命中即触发，靠 silence-period 防重复）
        │
        ▼
 ⑤ 通知（内置 webhook 处理器）
    组装告警消息（scope、name、period 内值序列）→ 转发到
    钉钉/飞书/企微/Slack/PagerDuty/webhook 处理器
        │
        ▼
 ⑥ 后续处理
    UI 查看告警消息记录；深度排查回到 trace/拓扑/profiling（APM 本职）
```

**特点**：告警对象是 **APM 维度**而非任意时序；表达力弱但 SLO 模板开箱即有；"排查闭环"发生在 trace/拓扑里而非告警系统里。

### 4. 数据采集模式对比：pull vs push（告警链路的最前端环节）

三者告警链路的第一环就分岔：**Prometheus 纯 pull、SkyWalking 纯 push、夜莺两者兼有（对数据源 pull、categraf push）**。采集模式直接决定后续"告警时效、目标发现方式、宕机判定"的差异。

**① pull 模式（Prometheus）——服务端主动拉取**

```
 Prometheus                                被监控目标（exporter/应用）
    │  ① 服务发现（kubernetes_sd/file_sd…）          │
    │  ② 按 scrape_interval 定时               │
    │  ───── GET /metrics ───────────────→     │  暴露文本端点即可
    │  ←──── 200 + 指标数据 ──────────────     │
    │  ③ 解析 → relabel → 写 TSDB              │
    │  ④ 目标失联（连续 N 个周期拉不到）           │
    │     → up{job} = 0 → 触发"目标 down"告警    │
```

- **目标发现权在服务端**：谁被抓取由 Prometheus 的 SD + relabel 决定，目标本身"被动暴露"；
- **宕机天然可观测**：`up == 0` 就是告警规则（拉不到=目标异常），不需要额外心跳；
- **时效上限 = scrape_interval**（默认 15s~30s），告警最小确认延迟 ≈ interval + for；
- 短生命周期任务（Job/CronJob）会拉不到 → 需 Pushgateway 补丁。

**② push 模式（SkyWalking / categraf 类 agent）——客户端主动上报**

```
 被监控应用（agent 插桩）                        服务端（OAP / 夜莺-categraf）
    │  自动/手动埋点采集                            │
    │  ───── gRPC/HTTP 批量上报 ────────→        │  接收、聚合、存储
    │        （持续流，秒级甚至实时）                │  └→ OAL 聚合 / 数据源查询
    │  ⑤ 心跳/连接中断                            │
    │     → 服务端按"心跳超时"判定实例失联           │  （SkyWalking 有内置
    │                                            │   instance 存活判定）
```

- **目标发现权在客户端**：应用启动即注册上报，容器/短任务天然覆盖，服务端无需维护 targets 列表；
- **时效更高**：秒级/准实时上报，告警延迟下限更低；
- **宕机判定靠超时推断**：收不到心跳 ≠ 客户端一定挂了（网络分区、agent 崩溃 vs 业务挂了需区分）；
- 采集逻辑下沉到 agent → 版本升级/资源占用需管理成百上千个 agent。

**③ 夜莺的混合形态**

```
 categraf ──push──→ 夜莺/TsunamiDB（内置采集场景）
 夜莺 server ──pull 查询──→ Prometheus/VictoriaMetrics 数据源（外部数据源场景）
```

- 对**数据源**是 pull（周期查询做告警评估），对**采集端**可 push（categraf）——两种模式在同一平台内并存，取长补短。

**④ 对告警链路的影响对比**

| 环节 | pull（Prometheus） | push（SkyWalking） | 混合（夜莺） |
|------|--------------------|--------------------|--------------|
| 目标发现 | 服务端 SD 决定（可审计、可控） | 客户端启动即注册 | 两者皆有 |
| 数据时效 | scrape_interval（15s~30s 级） | 秒级/准实时 | 取决于数据源采集 |
| 宕机判定 | 拉取失败 → `up=0`（显式事实） | 心跳超时推断（需区分网络/agent 故障） | 数据源中断感知 |
| 短生命周期任务 | 需 Pushgateway 补丁 | 天然覆盖（任务结束即上报完） | categraf 类 agent 天然覆盖 |
| 防火墙/NAT | 服务端需能**进**目标网络 | agent 只需能**出**（更易穿透） | 视数据源部署位置 |
| 资源消耗位置 | 目标侧只需暴露端点（轻） | agent 常驻消耗业务侧资源 | 分摊 |
| 采集配置变更 | 改服务端配置热重载即生效 | 逐个 agent 升级/改配置 | 视场景 |

**一句话概括**：pull 把"数据真相"收拢在服务端（可审计、up 语义明确，但时效受 interval 限制）；push 把"采集智能"分散到客户端（实时性好、覆盖动态实例，但宕机判定退化推断、agent 管理成本高）。**告警链路越靠前的环节，越决定时效；越靠后的环节（评估/通知），三者的设计差异越大。**

### 5. 流程关键节点对比

| 环节 | Prometheus | 夜莺 | SkyWalking |
|------|------------|------|------------|
| **数据从哪来** | 自己 pull 到自己的 TSDB | 查外部数据源（不采数） | agent push，OAL 从 trace 聚合 |
| **规则载体** | YAML（Git） | 数据库（UI） | YAML（Git） |
| **评估触发** | 周期 PromQL，每 series 一告警 | 周期查询 + group by 聚合 | 周期对内置指标做阈值判断 |
| **防抖** | `for` 持续时间（pending 确认） | 持续时间/次数配置 | `period` 内命中次数 + `silence-period` |
| **通知前置处理** | AM：分组/抑制/静默/路由树 | 屏蔽/收敛/升级/订阅 | 仅 silence-period |
| **事件持久化** | ❌（内存状态 + ALERTS 时序） | ✅ 数据库，可闭环 | ⚠️ UI 可查消息记录，无闭环 |
| **排查入口** | 回到 Prometheus 查询/Grafana | 事件详情 + 关联查询 | 回到 trace/拓扑/profiling |
| **重启影响** | 状态靠 `<tsdb>/alerts` 文件恢复 | 数据库，无影响 | OAP 内存重建，无影响 |

**一句话概括**：Prometheus 是"**检测在引擎、降噪在 AM、闭环靠自己**"的两段式管道；夜莺是"**检测、事件、通知、闭环全在一个平台**"的单体管道；SkyWalking 是"**APM 数据的附加产物**"——告警只是 trace 聚合指标的一种消费方式，核心竞争力仍在链路诊断。

## 四、选型参考

- **告警即代码**：已有 Prometheus + Alertmanager，规则少、团队熟悉 YAML → 维持现状，YAML 进 Git；
- **告警运营/值班/多渠道通知**：告警规则多、需要值班/闭环/多渠道通知/历史事件统计，不想维护 Alertmanager 路由 YAML → 夜莺这类平台化方案更合适；
- **需要链路追踪/性能诊断**（慢调用、拓扑、JVM 深度剖析）→ SkyWalking（或 Jaeger/Tempo + Grafana）；
- **常见组合**：
  - Prometheus/categraf 采集 + 夜莺做统一告警平台；
  - 大型微服务体系：**SkyWalking（APM）+ Prometheus（指标）+ 夜莺（统一告警平台）**，三者各管一层。
