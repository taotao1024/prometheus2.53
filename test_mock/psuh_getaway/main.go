// push_mock 是一个向 Pushgateway 持续推送 Prometheus 指标的模拟数据脚本。
//
// 功能：
//  1. 模拟 5 个实例，持续推送 gauge / counter / histogram 等多种类型的指标；
//  2. 推送目标 Pushgateway 地址：http://192.168.0.210:9091；
//  3. 首次运行时在当前目录生成 Grafana Dashboard JSON 文件
//     （grafana-dashboard-push_mock.json），导入 Grafana 即可展示。
//
// 前置条件：Prometheus 需要配置对 Pushgateway 的抓取任务（honor_labels: true，
// 以保留推送时的 job/instance 分组标签）：
//
//	scrape_configs:
//	  - job_name: pushgateway
//	    honor_labels: true
//	    static_configs:
//	      - targets: ["192.168.0.210:9091"]
//
// 运行：go run . （在本目录下执行，Ctrl+C 退出）
package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

const (
	// pushgatewayURL 是 Pushgateway 服务地址
	pushgatewayURL = "http://192.168.0.210:9091"
	// jobName 是推送指标的 job 标签值，Grafana Dashboard 中按此查询
	jobName = "push_mock"
	// pushInterval 是推送间隔
	pushInterval = 15 * time.Second
	// instanceCount 是模拟的实例数量
	instanceCount = 5
)

// instanceState 保存每个模拟实例的持久化采集器。
// 每次推送时更新采集器的当前值后整体推送给 Pushgateway；
// counter/histogram 跨推送保持累计递增，保证 PromQL 的 rate() 可用。
type instanceState struct {
	name    string
	reg     *prometheus.Registry
	cpu     prometheus.Gauge
	mem     prometheus.Gauge
	queue   prometheus.Gauge
	reqs    prometheus.Counter
	errs    prometheus.Counter
	lastTs  prometheus.Gauge
	hist    prometheus.Histogram
	reqRate float64 // 当前模拟的请求速率，用于随机游走
}

// newInstanceState 创建一个实例的注册器与全部指标采集器。
func newInstanceState(name string) *instanceState {
	reg := prometheus.NewRegistry()

	s := &instanceState{
		name: name,
		reg:  reg,
		cpu: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "push_mock_cpu_usage_percent",
			Help: "模拟的 CPU 使用率百分比.",
		}),
		mem: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "push_mock_memory_usage_percent",
			Help: "模拟的内存使用率百分比.",
		}),
		queue: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "push_mock_queue_length",
			Help: "模拟的任务队列长度.",
		}),
		reqs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "push_mock_requests_total",
			Help: "模拟的累计请求数.",
		}),
		errs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "push_mock_errors_total",
			Help: "模拟的累计错误请求数.",
		}),
		lastTs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "push_mock_last_push_timestamp_seconds",
			Help: "最近一次成功推送的 Unix 时间戳，用于监控推送是否中断.",
		}),
		hist: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "push_mock_request_duration_seconds",
			Help:    "模拟的请求耗时分布（用于 P95/P99 延迟面板）.",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}),
	}
	// 全部注册到该实例私有注册器
	for _, c := range []prometheus.Collector{s.cpu, s.mem, s.queue, s.reqs, s.errs, s.lastTs, s.hist} {
		reg.MustRegister(c)
	}
	return s
}

// tick 更新本实例的指标模拟值（随机游走 + 计数递增）。
func (s *instanceState) tick() {
	// CPU/内存：在合理区间内随机游走，避免数值跳变过于突兀
	s.cpu.Add(float64(rand.Intn(11)-5) * 0.8)
	s.mem.Add(float64(rand.Intn(7)-3) * 0.5)
	if rand.Float64() < 0.05 { // 偶发毛刺
		s.cpu.Set(rand.Float64() * 100)
	}

	// 请求速率缓慢波动
	s.reqRate += float64(rand.Intn(21) - 10)
	if s.reqRate < 20 {
		s.reqRate = 20
	}
	if s.reqRate > 500 {
		s.reqRate = 500
	}

	// 累计计数：按当前速率在推送间隔内的期望请求数递增
	reqDelta := s.reqRate * pushInterval.Seconds() / 5
	s.reqs.Add(reqDelta)
	if rand.Float64() < 0.3 { // 约 0.5%~3% 的错误率
		s.errs.Add(reqDelta * (0.005 + rand.Float64()*0.025))
	}

	// 队列长度
	s.queue.Set(float64(rand.Intn(200)))

	// 请求耗时采样：以正态分布模拟，多数落在 50ms~800ms
	for i := 0; i < int(reqDelta); i++ {
		d := rand.NormFloat64()*0.2 + 0.3
		if d < 0.001 {
			d = 0.001
		}
		s.hist.Observe(d)
	}
}

// push 将本实例当前指标推送到 Pushgateway。
func (s *instanceState) push() error {
	s.lastTs.SetToCurrentTime()
	return push.New(pushgatewayURL, jobName).
		Client(&http.Client{Timeout: 10 * time.Second}).
		Gatherer(s.reg).
		Grouping("instance", s.name).
		Push()
}

// dashboardJSON 是 Grafana Dashboard 的 JSON 模板，
// 查询语句与上面推送的指标一一对应。
const dashboardJSON = `{
  "uid": "push-mock-dashboard",
  "title": "PushGateway Mock Metrics",
  "tags": ["pushgateway", "mock"],
  "timezone": "browser",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "30s",
  "time": { "from": "now-3h", "to": "now" },
  "templating": { "list": [] },
  "panels": [
    {
      "id": 1, "type": "timeseries", "title": "CPU 使用率 (%)",
      "gridPos": { "x": 0, "y": 0, "w": 12, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "expr": "avg by (instance) (push_mock_cpu_usage_percent{job=\"push_mock\"})", "legendFormat": "{{instance}}" }
      ],
      "fieldConfig": { "defaults": { "unit": "percent", "max": 100 }, "overrides": [] }
    },
    {
      "id": 2, "type": "timeseries", "title": "内存使用率 (%)",
      "gridPos": { "x": 12, "y": 0, "w": 12, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "expr": "avg by (instance) (push_mock_memory_usage_percent{job=\"push_mock\"})", "legendFormat": "{{instance}}" }
      ],
      "fieldConfig": { "defaults": { "unit": "percent", "max": 100 }, "overrides": [] }
    },
    {
      "id": 3, "type": "timeseries", "title": "请求速率 (req/s)",
      "gridPos": { "x": 0, "y": 8, "w": 12, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "expr": "sum by (instance) (rate(push_mock_requests_total{job=\"push_mock\"}[1m]))", "legendFormat": "{{instance}}" }
      ],
      "fieldConfig": { "defaults": { "unit": "reqps" }, "overrides": [] }
    },
    {
      "id": 4, "type": "timeseries", "title": "错误率",
      "gridPos": { "x": 12, "y": 8, "w": 12, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "expr": "sum by (instance) (rate(push_mock_errors_total{job=\"push_mock\"}[5m])) / sum by (instance) (rate(push_mock_requests_total{job=\"push_mock\"}[5m]))", "legendFormat": "{{instance}}" }
      ],
      "fieldConfig": { "defaults": { "unit": "percentunit" }, "overrides": [] }
    },
    {
      "id": 5, "type": "timeseries", "title": "请求延迟 P95 / P99",
      "gridPos": { "x": 0, "y": 16, "w": 12, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "expr": "histogram_quantile(0.95, sum by (le, instance) (rate(push_mock_request_duration_seconds_bucket{job=\"push_mock\"}[5m])))", "legendFormat": "P95 {{instance}}" },
        { "refId": "B", "expr": "histogram_quantile(0.99, sum by (le, instance) (rate(push_mock_request_duration_seconds_bucket{job=\"push_mock\"}[5m])))", "legendFormat": "P99 {{instance}}" }
      ],
      "fieldConfig": { "defaults": { "unit": "s" }, "overrides": [] }
    },
    {
      "id": 6, "type": "timeseries", "title": "队列长度",
      "gridPos": { "x": 12, "y": 16, "w": 12, "h": 8 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "expr": "push_mock_queue_length{job=\"push_mock\"}", "legendFormat": "{{instance}}" }
      ],
      "fieldConfig": { "defaults": { "unit": "short" }, "overrides": [] }
    },
    {
      "id": 7, "type": "stat", "title": "最近推送距今 (秒)",
      "gridPos": { "x": 0, "y": 24, "w": 24, "h": 4 },
      "datasource": { "type": "prometheus", "uid": "prometheus" },
      "targets": [
        { "refId": "A", "expr": "time() - max by (instance) (push_mock_last_push_timestamp_seconds{job=\"push_mock\"})", "legendFormat": "{{instance}}" }
      ],
      "fieldConfig": { "defaults": { "unit": "s", "thresholds": { "mode": "absolute", "steps": [ { "color": "green", "value": null }, { "color": "red", "value": 120 } ] } }, "overrides": [] }
    }
  ]
}`

// generateDashboard 将 Dashboard JSON 写入 main.go 所在目录（源码目录），
// 供导入 Grafana 使用。os.WriteFile 以 O_TRUNC 打开文件：
// 若文件已存在则直接覆盖写入。
func generateDashboard() (string, error) {
	// 通过 runtime.Caller 获取 main.go 源码文件的绝对路径，
	// 保证 Dashboard JSON 始终生成在与本脚本相同的目录下，
	// 不受程序启动时工作目录（os.Getwd）的影响
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("无法获取 main.go 源码路径")
	}
	path := filepath.Join(filepath.Dir(thisFile), "grafana-dashboard-push_mock.json")
	if err := os.WriteFile(path, []byte(dashboardJSON), 0o644); err != nil {
		return "", err
	}
	return path, nil
}

func main() {
	// 1. 生成 Grafana Dashboard 文件
	if path, err := generateDashboard(); err != nil {
		log.Fatalf("生成 Grafana Dashboard 失败: %v", err)
	} else {
		log.Printf("Grafana Dashboard 已生成: %s (导入 Grafana 即可展示)", path)
	}

	// 2. 创建模拟实例
	var instances []*instanceState
	for i := 1; i <= instanceCount; i++ {
		instances = append(instances, newInstanceState(fmt.Sprintf("mock-instance-%02d", i)))
	}

	// 3. Ctrl+C 优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// 4. 循环推送：每个间隔为所有实例推送一批指标
	ticker := time.NewTicker(pushInterval)
	defer ticker.Stop()

	log.Printf("开始向 Pushgateway %s 推送 job=%s 的指标，间隔 %v，实例数 %d，Ctrl+C 退出",
		pushgatewayURL, jobName, pushInterval, instanceCount)

	for {
		for _, s := range instances {
			s.tick()
			if err := s.push(); err != nil {
				log.Printf("[WARN] 推送实例 %s 失败: %v", s.name, err)
			}
		}
		log.Printf("已推送 %d 个实例的指标 @ %s", instanceCount, time.Now().Format(time.RFC3339))

		select {
		case <-ticker.C:
			// 继续下一轮推送
		case sig := <-sigCh:
			log.Printf("收到信号 %v，退出", sig)
			return
		}
	}
}
