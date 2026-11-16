// 告警接收端（mock webhook receiver）：模拟 prometheus-webhook-dingtalk，
// 接收 Alertmanager 通过 webhook_configs POST 过来的告警并打印。
//
// 配套的发送端是上级目录的 alertSend.go（推送端 → Alertmanager → 本接收端）。
//
// ============================ 配套配置方法 ============================
//
// 一、Prometheus 侧（prometheus.yml）——告警从 Prometheus 发往 Alertmanager：
//
//	alerting:
//	  alertmanagers:
//	    - static_configs:
//	        - targets: ['192.168.0.211:9093']   # Alertmanager 地址
//
//	# 若使用发送端 alertSend.go 直接推 AM，则 Prometheus 侧无需配置。
//
// 二、Alertmanager 侧（alertmanager.yml）——告警从 AM 转发到本接收端：
//
//	route:
//	  receiver: mock.hook            # 根接收器
//	  group_by: [alertName]          # 分组键（与发送端的标签名一致）
//	  group_wait: 10s
//	  group_interval: 10s
//	  repeat_interval: 1h
//
//	receivers:
//	- name: mock.hook
//	  webhook_configs:
//	  - url: http://<本接收端所在机器IP>:8060/webhook   # ← 指向本程序
//	    send_resolved: true                              # 恢复时也推送
//	    max_alerts: 0                                    # 不限制单次条数
//
//	# 修改后重载：curl -X POST http://192.168.0.211:9093/-/reload 或 SIGHUP
//
// 三、数据流转：
//
//	alertSend.go ──POST /api/v2/alerts──→ Alertmanager(192.168.0.211:9093)
//	             ──POST /webhook──────→ 本接收端（打印告警内容与跳转URL）
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
)

// WebhookPayload 对应 Alertmanager webhook_configs 的 POST 请求体。
// 完整结构见 Alertmanager 官方文档 "Webhook receiver"。
type WebhookPayload struct {
	Version           string            `json:"version"`           // 消息格式版本，如 "4"
	Receiver          string            `json:"receiver"`          // 命中的 receiver 名称
	Status            string            `json:"status"`            // 整组状态：firing / resolved
	Alerts            []WebhookAlert    `json:"alerts"`            // 本组包含的告警列表
	GroupLabels       map[string]string `json:"groupLabels"`       // 分组标签（route.group_by 的值）
	CommonLabels      map[string]string `json:"commonLabels"`      // 组内所有告警共有的标签
	CommonAnnotations map[string]string `json:"commonAnnotations"` // 组内共有的注释
	ExternalURL       string            `json:"externalURL"`       // AM 的外部地址
	TruncatedAlerts   int               `json:"truncatedAlerts"`   // 因 max_alerts 截断的数量
}

// WebhookAlert 单条告警（与 v2 API 的告警模型基本一致，多一个 status/fingerprint）
type WebhookAlert struct {
	Status       string            `json:"status"`       // firing / resolved
	Labels       map[string]string `json:"labels"`       // 告警标签
	Annotations  map[string]string `json:"annotations"`  // 告警注释（summary/description）
	StartsAt     string            `json:"startsAt"`     // 触发时间（RFC3339 字符串）
	EndsAt       string            `json:"endsAt"`       // 结束时间
	GeneratorURL string            `json:"generatorURL"` // 告警详情跳转链接
	Fingerprint  string            `json:"fingerprint"`  // 告警指纹（标签 hash）
}

// webhookHandler 处理 Alertmanager 的 POST /webhook 请求
func webhookHandler(w http.ResponseWriter, r *http.Request) {
	// 只接受 POST；AM 之外的工具探测时也能给出明确提示
	if r.Method != http.MethodPost {
		http.Error(w, "只接受 POST 请求", http.StatusMethodNotAllowed)
		return
	}

	var payload WebhookPayload
	// json.NewDecoder 直接从请求体流式反序列化 ≈ Jackson 的 readValue(inputStream, ...)
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "请求体不是合法的告警 JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// 打印组级信息
	log.Printf("====== 收到告警组 ======")
	log.Printf("receiver=%s  组状态=%s  分组标签=%v  告警数=%d",
		payload.Receiver, payload.Status, payload.GroupLabels, len(payload.Alerts))

	// 逐条打印告警内容与详情跳转 URL
	for i, a := range payload.Alerts {
		log.Printf("---- 告警[%d] ----", i+1)
		log.Printf("  状态:       %s", a.Status)
		log.Printf("  告警名:     %s", a.Labels["alertName"])
		log.Printf("  分组:       %s", a.Labels["group"])
		log.Printf("  实例:       %s", a.Labels["instance"])
		log.Printf("  级别:       %s", a.Labels["severity"])
		log.Printf("  摘要:       %s", a.Annotations["summary"])
		log.Printf("  描述:       %s", a.Annotations["description"])
		log.Printf("  触发时间:   %s", a.StartsAt)
		log.Printf("  指纹:       %s", a.Fingerprint)
		// generatorURL：告警详情的跳转地址（发送端定义为 http://192.168.0.210:9093/metrics）
		log.Printf("  详情跳转URL: %s", a.GeneratorURL)
	}

	// 返回 200 即告知 AM 投递成功（非 2xx 会触发 AM 的重试）
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}

func main() {
	// ============================================================
	// 1. 命令行参数
	// ============================================================
	// 注册监听端口（此时尚未解析，只定义参数名/默认值/帮助文本）。
	// 返回值是指针，解析完成后需用 *addr 解引用取实际值。
	addr := flag.String("addr", "192.168.0.118:8060", "监听地址与端口（如 :8060 或 0.0.0.0:8060）")
	// 真正扫描命令行输入并回填上述变量；解析失败会自动打印帮助并退出。
	flag.Parse()

	// ============================================================
	// 2. 注册路由并启动 HTTP 服务
	// ============================================================
	// ≈ Spring Boot 的 @PostMapping("/webhook")；Go 标准库即可，无需框架。
	http.HandleFunc("/webhook", webhookHandler)

	log.Printf("告警接收端已启动，监听 %s，等待 Alertmanager 推送（Ctrl+C 退出）", *addr)
	log.Printf("请确认 Alertmanager 的 webhook_configs.url 指向: http://<本机IP>%s/webhook", *addr)
	// http.ListenAndServe 阻塞运行；返回错误说明端口被占用等问题，直接终止。
	log.Fatal(http.ListenAndServe(*addr, nil))
}
