package main

// 引入依赖
import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"
)

// 定义常量
const (
	// Alertmanager v2 API 告警接收端点
	apiPath = "/api/v2/alerts"
	// 告警持续时长：endsAt 设为 now+持续时长，保证两次推送之间告警保持 firing 状态
	alertDuration = 2 * time.Minute
	// generatorURL：告警详情的跳转链接（AM 通知里可点击），指向本实例的 /metrics 页面
	generatorURL = "http://192.168.0.210:9093/metrics"
)

// 对应Java的POJO
// Alert 定义 Alertmanager v2 API 接收的告警结构（POST /api/v2/alerts 请求体的元素）
type Alert struct {
	// 反引号里的 json:"labels" 就是序列化时的字段名标签，等价于 @JsonProperty("labels")
	Labels       map[string]string `json:"labels"` // map[string]string ≈ Map<String,String>
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"` // ≈ OffsetDateTime startsAt
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL,omitempty"`
}

// baseLabels 返回三条告警共有的标签（instance/job/dev）
func baseLabels() map[string]string {
	return map[string]string{
		"instance": "192.168.0.100:9100",
		"job":      "node",
		"dev":      "test", // 配合 Alertmanager 抑制规则 equal 中的自定义标签
	}
}

// newAlert 组装单条告警（公共字段统一填充，差异部分由调用方传入）
func newAlert(now, ends time.Time, alertname, group, summary, description string) Alert {
	b := baseLabels()
	return Alert{
		Labels: map[string]string{
			"alertname": alertname,
			"group":     group,
			"instance":  b["instance"],
			"job":       b["job"],
			"severity":  "warning",
			"dev":       b["dev"],
		},
		Annotations: map[string]string{
			"summary":     summary,
			"description": fmt.Sprintf("%s（模拟数据，推送时间 %s）", description, now.Format("15:04:05")),
		},
		StartsAt:     now,
		EndsAt:       ends, // 未来时间，保证在下一次推送前告警仍为 firing
		GeneratorURL: generatorURL,
	}
}

// Alert的构造方法
// buildAlerts 构造一批测试告警（一次推送三条：CPU / 内存 / 磁盘）
// 三条告警在同一个 POST 请求中发送——v2 API 的请求体本身就是数组，天然支持批量。
func buildAlerts() []Alert {
	now := time.Now().UTC()
	ends := now.Add(alertDuration)

	return []Alert{
		newAlert(now, ends, "HighCpuUsage", "CPU",
			"测试告警：实例 CPU 使用率过高", "192.168.0.100 CPU 使用率 92%"),
		newAlert(now, ends, "HighMemoryUsage", "Memory",
			"测试告警：实例内存使用率过高", "192.168.0.100 内存使用率 87%"),
		newAlert(now, ends, "DiskSpaceLow", "Disk",
			"测试告警：磁盘空间不足", "192.168.0.100 /data 磁盘剩余 5%"),
		newAlert(now, ends, "IOSpaceLow", "IO",
			"测试告警：IO不足", "192.168.0.100 /data IO剩余 5%"),
	}
}

// sendAlerts 向 Alertmanager 批量推送告警
func sendAlerts(amURL string, alerts []Alert) error {
	body, err := json.Marshal(alerts) // json 序列化（数组整体作为请求体）
	if err != nil {
		return fmt.Errorf("序列化告警失败: %w", err) // 抛异常
	}

	// POST {amURL}/api/v2/alerts，v2 API 要求 Content-Type 为 application/json
	// Go 标准库自带 HTTP 客户端，无需引依赖
	resp, err := http.Post(amURL+apiPath, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("请求 Alertmanager 失败: %w", err)
	}
	// 延迟到函数返回前执行，专门用于释放资源
	defer resp.Body.Close() // ≈ try-with-resources 自动关闭，函数返回前才执行

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Alertmanager 返回非 200 状态码: %d", resp.StatusCode)
	}
	return nil
}

// main 程序入口：解析命令行参数后，向 Alertmanager 循环批量推送测试告警。
//
// 执行流程：
//  1. 注册并解析命令行参数（flag 包）：
//     -addr     Alertmanager 地址（默认 192.168.0.210:9093）
//     -interval 推送间隔（默认 30s，支持 "10s"/"1m30s" 等时长写法）
//     flag.String/Duration 仅注册参数定义（返回指针），flag.Parse() 才真正
//     扫描 os.Args 完成解析；未传参时使用默认值，-h 可打印参数帮助。
//  2. 每次推送构造 3 条告警（CPU/内存/磁盘）并一次性 POST，
//     立即推送一次，随后由 time.Ticker 每隔 interval 推送一次，
//     直到 Ctrl+C 中断进程。
//
// 用法示例：
//
//	go run .                                  # 默认目标与间隔
//	go run . -addr 192.168.0.211:9093         # 覆盖目标地址
//	go run . -interval 10s                    # 覆盖推送间隔
func main() {
	// 注册命令行参数（此时尚未解析，只定义了参数名/默认值/帮助文本）。
	// 返回值是指针，解析完成后需用 *amAddr 解引用取实际值。
	amAddr := flag.String("addr", "192.168.0.210:9093", "Alertmanager 地址")
	interval := flag.Duration("interval", 30*time.Second, "推送间隔")
	// 真正扫描命令行输入并回填上述变量；解析失败（如未知参数）会自动打印帮助并退出。
	flag.Parse()

	// 解引用指针拿实际值，拼出 Alertmanager v2 API 的完整基地址。
	amURL := "http://" + *amAddr
	log.Printf("开始推送告警到 %s%s，间隔 %s（Ctrl+C 退出）", amURL, apiPath, *interval)

	// send 闭包：批量推送并在日志中输出结果。
	// 捕获了 amURL 变量（≈ Java lambda 捕获的 effectively final 变量）。
	// 立即推送一次，之后每 interval 推送一次。
	send := func() {
		alerts := buildAlerts()
		if err := sendAlerts(amURL, alerts); err != nil {
			log.Printf("[ERROR] 推送失败: %v", err)
			return
		}
		for _, a := range alerts {
			log.Printf("[OK] 已推送告警 alertname=%s group=%s instance=%s severity=%s endsAt=%s",
				a.Labels["alertname"], a.Labels["group"], a.Labels["instance"],
				a.Labels["severity"], a.EndsAt.Format("15:04:05"))
		}
		log.Printf("[OK] 本轮共推送 %d 条告警", len(alerts))
	}

	send()                              // 立即执行一次
	ticker := time.NewTicker(*interval) // 定时循环
	defer ticker.Stop()
	for range ticker.C { // ticker.C 是一个"每 30s 滴答一次"的管道
		send() // 循环体
	}
}
