# AlertManager 配置文件分析

> 分析对象：一份"Prometheus → Alertmanager → prometheus-webhook-dingtalk → 钉钉群"的最小生产配置。
> 结构：单根路由 + 单 webhook 接收器 + 一条 critical 抑制 warning 的规则。
> 先给出**逐行注释的原文**，再逐段解析，最后给出端到端流转与优化建议。

## 一、原文逐行注释版

```yaml
# ============================== 全局默认段 ==============================
global:
  # 当告警"没有 EndsAt"（如 v1 API 或部分采集源）时，
  # AM 在多久没收到该告警的更新后自动判定为"已恢复"。
  # Prometheus 走 v2 API 时每条告警自带 EndsAt，此值基本不生效。
  resolve_timeout: 5m
  # 全局出站 HTTP 默认行为，各 receiver 未覆盖时继承。
  http_config:
    follow_redirects: true     # 跟随 3xx 重定向
    enable_http2: true         # 允许 HTTP/2
  # SMTP 发信参数：本配置没有任何 email_configs，以下两项不会生效（冗余，可删）。
  smtp_hello: localhost        # SMTP HELO 标识
  smtp_require_tls: true       # 发信要求 TLS
  # 以下 6 个 *_url 均为官方默认值，只有用到对应 receiver 才有意义，
  # 显式列出只是"默认配置的显式化"，不影响行为。
  pagerduty_url: https://events.pagerduty.com/v2/enqueue
  opsgenie_api_url: https://api.opsgenie.com/
  wechat_api_url: https://qyapi.weixin.qq.com/cgi-bin/
  victorops_api_url: https://alert.victorops.com/integrations/generic/20131114/alert/
  telegram_api_url: https://api.telegram.org
  webex_api_url: https://webexapis.com/v1/messages

# ============================== 路由树（根路由） ==============================
route:
  # 根接收器：所有未被子路由命中的告警都发给它（本配置无子路由 → 全部落这里）。
  receiver: web.hook
  # 分组键：仅按告警名分组。
  # 不同主机/实例的同名告警会合并成一条通知（如 10 台机器同时 InstanceDown → 1 条消息）。
  group_by:
  - alertname
  # 根路由必须为 false：命中后不再继续匹配（本配置无子路由，无实际影响）。
  continue: false
  # 组内第一条告警到达后等待 10s 再发第一条通知（等同伴聚齐，减少消息数）。
  group_wait: 10s
  # 同组已发过通知后，新告警到达至少间隔 10s 才再发。
  # 与 group_wait 相同为激进配置：通知及时，风暴时每 10 秒一条消息。
  group_interval: 10s
  # 组未恢复时每小时重发一次通知（钉钉群里保证告警"还在"）。
  repeat_interval: 1h

# ============================== 抑制规则 ==============================
inhibit_rules:
- # 源告警：critical 级别（抑制的发起方）。
  source_match:
    severity: critical        # 旧写法；现代等价：source_matchers: ['severity="critical"']
  # 目标告警：warning 级别（被抑制方）。
  target_match:
    severity: warning         # 旧写法；现代等价：target_matchers: ['severity="warning"']
  # 匹配条件：三个标签全部相同的 critical 与 warning 才构成抑制关系。
  # ⚠️ 告警标签必须同时带 alertname/dev/instance——其中 dev 是自定义标签，
  #    若告警规则没有输出 dev，此规则永远不会命中。
  equal:
  - alertname
  - dev
  - instance

# ============================== 接收器（通知出口） ==============================
receivers:
- # 接收器名称（被 route.receiver 引用）。
  name: web.hook
  webhook_configs:
  - # 恢复时也发通知（钉钉场景必须开，否则告警恢复后群里"永远挂着"）。
    send_resolved: true
    # 出站 HTTP 配置，与全局相同（显式继承）。
    http_config:
      follow_redirects: true
      enable_http2: true
    # 目标地址：10.2.3.210:8060 是典型的 prometheus-webhook-dingtalk 部署形态
    # （AM → 转换器 → 钉钉机器人；AM 只做 POST，钉钉消息格式由转换器适配）。
    # HTTP + 内网 IP：内网部署可接受，跨网段建议 HTTPS。
    url: http://10.2.3.210:8060/dingtalk/webhook1/send
    # 单次 webhook 携带的告警条数上限，0 = 不限制。
    max_alerts: 0

# ============================== 通知模板 ==============================
# 空列表 = 使用 Alertmanager 内置默认模板，未自定义通知文案。
templates: []
```

## 二、逐段解析

### 2.1 `global` 段（全局默认）

| 配置 | 值 | 含义 | 评价 |
|------|-----|------|------|
| `resolve_timeout` | `5m` | 无 `EndsAt` 的告警多久未更新后自动判定恢复 | 默认值；Prometheus v2 API 自带 `EndsAt`，基本不生效 |
| `http_config` | 重定向/HTTP2 均开 | 全局出站 HTTP 默认行为 | 默认值 |
| `smtp_hello` / `smtp_require_tls` | `localhost` / `true` | SMTP 发信参数 | **冗余**——无 `email_configs`，不会生效，可删 |
| 6 个 `*_url` | 官方默认值 | 各集成 API 入口 | 只在用到对应 receiver 时才有意义 |

### 2.2 `route` 段（路由树根）

| 配置 | 值 | 含义 |
|------|-----|------|
| `receiver` | `web.hook` | 根接收器，未命中子路由的告警兜底 |
| `group_by` | `[alertname]` | 分组键——**不同实例的同名告警合并成一条通知** |
| `continue` | `false` | 根路由固定值，命中后不再往下匹配 |
| `group_wait` | `10s` | 首条告警到达后等 10s 聚齐同伴再发第一条通知 |
| `group_interval` | `10s` | 同组两次通知的最小间隔（新告警到达时） |
| `repeat_interval` | `1h` | 组未恢复时的重发周期 |

**分组行为推演**：10 台机器同时 `InstanceDown` → 首个 `group_wait: 10s` 内到达的告警合并成 **1 条**钉钉消息；若希望按主机区分，需把 `instance` 加入 `group_by`（代价是变成 10 条消息）。

### 2.3 `inhibit_rules` 段（抑制）

**语义**：同一 `alertname + dev + instance` 上同时存在 critical 与 warning 时，**critical 抑制 warning**——warning 静默不发通知（状态仍存在，critical 恢复后可见）。

- 典型用途：同一目标先触发 warning、又触发 critical 时只发 critical；
- `source_match`/`target_match` 是旧写法（仍支持），建议迁移 `source_matchers`/`target_matchers`；
- ⚠️ `equal` 中的 `dev` 是自定义标签：若告警规则未输出 `dev`，该抑制规则**永远不会命中**。

### 2.4 `receivers` 段（通知出口）

- `10.2.3.210:8060/dingtalk/...` 为典型的 **prometheus-webhook-dingtalk** 形态；
- `send_resolved: true` 正确（否则恢复事件不可见）；
- `max_alerts: 0`：单次 webhook 携带全部聚合告警，配合 `group_by: [alertname]` 合理；
- HTTP + 内网 IP：内网可接受，跨网段建议 HTTPS。

### 2.5 `templates: []`

使用 Alertmanager 内置默认模板（`.Defaults`），未自定义通知文案。

## 三、端到端流转（结合 Prometheus 侧）

```
Prometheus AlertingRule（rules/ → firing）
  → rules.SendAlerts → notifier 队列（重发由 ResendDelay + 组 interval 驱动）
  → POST http://AM:9093/api/v2/alerts          ← EndsAt 随告警携带
  → AM：去重（fingerprint = 标签 hash）
  → route 树匹配（无子路由 → 全部落 web.hook）
  → 分组 group_by=[alertname] → group_wait 10s 聚合
  → 抑制检查（critical 压 warning）
  → 无 mute_time → webhook POST
  → prometheus-webhook-dingtalk → 钉钉群
  → 周期性重发（repeat_interval 1h）
  → 恢复时 EndsAt 到期 → send_resolved 通知
```

## 四、可优化点

| 问题 | 建议 |
|------|------|
| `group_by` 只有 `alertname`，多实例告警合并过狠 | 视噪音程度决定是否加入 `instance`/`job`；或保留现状靠 webhook 端二次聚合 |
| 抑制规则依赖自定义标签 `dev` | 确认告警规则确实输出 `dev`，否则抑制失效；建议迁移 `source_matchers` 新语法 |
| `group_interval: 10s` 过短 | 风暴场景消息刷屏，可放宽到 `1m~5m`（`group_wait` 保持 10s 即可） |
| `smtp_*` 配置冗余 | 无 email receiver，可删 |
| 无子路由/无静默时段 | 需要"分级通知"（critical 电话、warning 群聊）时，增加子 `route` + `mute_time_intervals` |
| 单点 | 生产建议 AM 双实例（gossip 集群）+ Prometheus `alerting` 段配两个地址 |

## 五、一句话总结

这是一份结构清晰的"最小生产配置"：**单接收器、按告警名聚合、10 秒级通知时效、1 小时重发、critical 压 warning**。主要风险点是抑制规则依赖的自定义标签 `dev` 是否真实存在于告警标签中，以及告警风暴场景下 `group_interval: 10s` 的消息频率。
