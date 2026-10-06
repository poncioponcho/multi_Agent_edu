// Package main 提供零依赖的 HTTP 压测器。
//
// 纯标准库实现（与项目「Go 版零第三方依赖」的定位一致），用于对 Go 版
// Agent 服务做阶梯式并发压测，并同步采样服务端运行时指标。
//
// 为什么需要它而不是 ab/wrk：
//  1. 需要同步抓取服务端的 goroutine / 内存 / EventBus 积压，才能把
//     「吞吐下降」与「资源泄漏」关联起来；
//  2. 需要逐秒吞吐分桶——EventBus 的去重表随运行时间膨胀，只报单个
//     平均吞吐会把「随时间劣化」这个关键现象平均掉；
//  3. 需要控制 learner_id 基数，以区分「多 key 并行」与「单 key 争用」。
//
// 用法：
//
//	# 先启动被测服务（务必静音日志，否则测到的是日志 I/O）
//	cd golang && EDU_QUIET=1 go run cmd/main.go
//
//	# 阶梯压测
//	go run cmd/loadtest/main.go -stages 10,50,100,200,400 -duration 5s
//
//	# 单 key 争用对照（所有请求打同一个 learner）
//	go run cmd/loadtest/main.go -learners 1 -stages 100 -duration 10s
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ── 命令行参数 ──

var (
	flagBaseURL   = flag.String("url", "http://127.0.0.1:8081", "被测服务地址")
	flagEndpoint  = flag.String("endpoint", "/api/v1/submit", "压测的路径")
	flagMethod    = flag.String("method", "POST", "HTTP 方法（GET 时不发送请求体，用于对照组）")
	flagStages    = flag.String("stages", "10,50,100,200,400", "阶梯并发数，逗号分隔")
	flagDuration  = flag.Duration("duration", 5*time.Second, "每个并发级别的持续时长")
	flagWarmup    = flag.Duration("warmup", 2*time.Second, "预热时长（不计入结果）")
	flagLearners  = flag.Int("learners", 200, "learner_id 基数：1 表示所有请求打同一学习者（单 key 争用）")
	flagSampleInt = flag.Duration("sample", 200*time.Millisecond, "服务端指标采样间隔")
	flagTimeout   = flag.Duration("timeout", 30*time.Second, "单请求超时")
)

// ── 服务端指标 ──

// serverStats 对应 /api/v1/debug/stats 的响应
type serverStats struct {
	UptimeSeconds float64 `json:"uptime_seconds"`
	Goroutines    int     `json:"goroutines"`
	HeapAlloc     uint64  `json:"heap_alloc_bytes"`
	GC            uint32  `json:"gc_count"`
	Learners      int     `json:"learners"`
	EventBus      struct {
		HistoryLen int   `json:"history_len"`
		SeenLen    int   `json:"seen_len"`
		QueueLen   int   `json:"queue_len"`
		QueueCap   int   `json:"queue_cap"`
		Dropped    int64 `json:"dropped"`
	} `json:"eventbus"`
}

func fetchStats(c *http.Client, base string) (*serverStats, error) {
	resp, err := c.Get(base + "/api/v1/debug/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status=%d", resp.StatusCode)
	}
	var s serverStats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

// ── 单级压测结果 ──

type stageResult struct {
	Concurrency int
	Requests    int64
	Errors      int64
	Elapsed     time.Duration
	Latencies   []time.Duration // 已排序

	Before *serverStats
	During *serverStats // 采样峰值
	After  *serverStats
	PerSec []int64 // 逐秒完成数
}

func (r *stageResult) throughput() float64 {
	return float64(r.Requests) / r.Elapsed.Seconds()
}

func (r *stageResult) percentile(p float64) time.Duration {
	if len(r.Latencies) == 0 {
		return 0
	}
	idx := int(p / 100 * float64(len(r.Latencies)-1))
	return r.Latencies[idx]
}

// ── 压测主流程 ──

func main() {
	flag.Parse()

	base := strings.TrimRight(*flagBaseURL, "/")
	endpoint := base + *flagEndpoint

	// 压测客户端：放大连接池，避免客户端侧成为瓶颈
	client := &http.Client{
		Timeout: *flagTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        0, // 0 = 无上限
			MaxIdleConnsPerHost: 4096,
			DisableCompression:  true,
		},
	}

	// 预生成请求体，避免在热路径里做 JSON 序列化
	if *flagLearners < 1 {
		*flagLearners = 1
	}
	payloads := make([][]byte, *flagLearners)
	for i := range payloads {
		body, _ := json.Marshal(map[string]interface{}{
			"learner_id":         fmt.Sprintf("L-%d", i),
			"knowledge_id":       "kp-1",
			"is_correct":         true,
			"time_spent_seconds": 1.2,
		})
		payloads[i] = body
	}

	// 健康检查
	if s, err := fetchStats(client, base); err != nil {
		fmt.Fprintf(os.Stderr, "无法连接被测服务 %s：%v\n", base, err)
		fmt.Fprintf(os.Stderr, "请先启动：cd golang && EDU_QUIET=1 go run cmd/main.go\n")
		os.Exit(1)
	} else {
		fmt.Printf("被测服务就绪：uptime=%.1fs goroutines=%d heap=%.1fMB\n\n",
			s.UptimeSeconds, s.Goroutines, float64(s.HeapAlloc)/(1<<20))
	}

	concurrencies, err := parseStages(*flagStages)
	if err != nil {
		fmt.Fprintf(os.Stderr, "解析 -stages 失败：%v\n", err)
		os.Exit(1)
	}

	// 预热
	if *flagWarmup > 0 {
		fmt.Printf("预热 %v（并发 %d）...\n", *flagWarmup, concurrencies[0])
		runStage(client, base, endpoint, payloads, concurrencies[0], *flagWarmup, nil)
	}

	fmt.Printf("\n压测目标：%s    learner 基数：%d    每级时长：%v\n",
		endpoint, *flagLearners, *flagDuration)
	fmt.Println(strings.Repeat("─", 118))
	fmt.Printf("%-6s %10s %9s %9s %9s %9s %9s %8s %10s\n",
		"并发", "吞吐req/s", "p50", "p90", "p99", "max", "错误", "gorout", "内存MB")
	fmt.Println(strings.Repeat("─", 118))

	var results []*stageResult
	for _, c := range concurrencies {
		r := runStage(client, base, endpoint, payloads, c, *flagDuration, client)
		results = append(results, r)
		fmt.Printf("%-6d %10.0f %9s %9s %9s %9s %9d %8d %10.1f\n",
			r.Concurrency, r.throughput(),
			fmtDur(r.percentile(50)), fmtDur(r.percentile(90)),
			fmtDur(r.percentile(99)), fmtDur(r.percentile(100)),
			r.Errors, peakGoroutines(r), float64(r.After.HeapAlloc)/(1<<20))
	}
	fmt.Println(strings.Repeat("─", 118))

	// 饱和点判定
	reportSaturation(results)

	// 逐秒吞吐：暴露「随时间劣化」
	reportPerSecond(results)

	// 服务端资源增长
	reportResources(results)
}

func parseStages(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("%q 不是整数", part)
		}
		if n <= 0 {
			return nil, fmt.Errorf("并发数必须 > 0，得到 %d", n)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("未提供任何并发级别")
	}
	return out, nil
}

// runStage 跑一级并发。statsClient 非 nil 时同步采样服务端指标。
func runStage(client *http.Client, base, endpoint string, payloads [][]byte, concurrency int, d time.Duration, statsClient *http.Client) *stageResult {
	res := &stageResult{Concurrency: concurrency}
	isGET := strings.EqualFold(*flagMethod, "GET")

	if statsClient != nil {
		if s, err := fetchStats(statsClient, base); err == nil {
			res.Before = s
		}
	}

	// 逐秒分桶：多留 2 个桶避免边界越界
	buckets := make([]atomic.Int64, int(d.Seconds())+2)
	perWorkerLat := make([][]time.Duration, concurrency)
	var totalReq, totalErr atomic.Int64

	// 服务端指标采样器：记录峰值
	peak := &serverStats{}
	var peakMu sync.Mutex
	stopSampler := make(chan struct{})
	var samplerWG sync.WaitGroup
	if statsClient != nil {
		samplerWG.Add(1)
		go func() {
			defer samplerWG.Done()
			ticker := time.NewTicker(*flagSampleInt)
			defer ticker.Stop()
			for {
				select {
				case <-stopSampler:
					return
				case <-ticker.C:
					s, err := fetchStats(statsClient, base)
					if err != nil {
						continue
					}
					peakMu.Lock()
					if s.Goroutines > peak.Goroutines {
						peak.Goroutines = s.Goroutines
					}
					if s.HeapAlloc > peak.HeapAlloc {
						peak.HeapAlloc = s.HeapAlloc
					}
					if s.EventBus.QueueLen > peak.EventBus.QueueLen {
						peak.EventBus.QueueLen = s.EventBus.QueueLen
					}
					if s.EventBus.SeenLen > peak.EventBus.SeenLen {
						peak.EventBus.SeenLen = s.EventBus.SeenLen
					}
					peak.EventBus.HistoryLen = s.EventBus.HistoryLen
					peakMu.Unlock()
				}
			}
		}()
	}

	start := time.Now()
	deadline := start.Add(d)

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// 预分配：按每 worker 每秒 2000 请求估容量，避免热路径 append 扩容
			lat := make([]time.Duration, 0, int(d.Seconds())*2000)
			bodyIdx := workerID

			for {
				if time.Now().After(deadline) {
					break
				}
				body := payloads[bodyIdx%len(payloads)]
				bodyIdx++
				t0 := time.Now()
				var resp *http.Response
				var err error
				if isGET {
					resp, err = client.Get(endpoint)
				} else {
					resp, err = client.Post(endpoint, "application/json", bytes.NewReader(body))
				}
				if err != nil {
					totalErr.Add(1)
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				el := time.Since(t0)

				if resp.StatusCode != http.StatusOK {
					totalErr.Add(1)
					continue
				}
				totalReq.Add(1)
				lat = append(lat, el)

				sec := int(time.Since(start).Seconds())
				if sec >= 0 && sec < len(buckets) {
					buckets[sec].Add(1)
				}
			}
			perWorkerLat[workerID] = lat
		}(w)
	}
	wg.Wait()
	res.Elapsed = time.Since(start)

	close(stopSampler)
	samplerWG.Wait()

	// 合并各 worker 的延迟样本
	total := 0
	for _, l := range perWorkerLat {
		total += len(l)
	}
	merged := make([]time.Duration, 0, total)
	for _, l := range perWorkerLat {
		merged = append(merged, l...)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i] < merged[j] })
	res.Latencies = merged
	res.Requests = totalReq.Load()
	res.Errors = totalErr.Load()

	res.PerSec = make([]int64, len(buckets))
	for i := range buckets {
		res.PerSec[i] = buckets[i].Load()
	}

	peakMu.Lock()
	res.During = peak
	peakMu.Unlock()

	if statsClient != nil {
		if s, err := fetchStats(statsClient, base); err == nil {
			res.After = s
		}
	}
	if res.During == nil {
		res.During = &serverStats{}
	}
	if res.Before == nil {
		res.Before = &serverStats{}
	}
	if res.After == nil {
		res.After = &serverStats{}
	}
	return res
}

func peakGoroutines(r *stageResult) int {
	if r.During == nil {
		return 0
	}
	return r.During.Goroutines
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

// reportSaturation 找出吞吐不再增长的第一个并发级别，即饱和点。
func reportSaturation(results []*stageResult) {
	fmt.Println("\n【饱和点判定】")
	best := 0.0
	saturatedAt := -1
	for i, r := range results {
		tp := r.throughput()
		growth := 0.0
		if i > 0 {
			prev := results[i-1].throughput()
			if prev > 0 {
				growth = (tp - prev) / prev * 100
			}
		}
		mark := ""
		if tp > best {
			best = tp
		} else if saturatedAt < 0 {
			saturatedAt = r.Concurrency
			mark = "   ← 吞吐首次不增，饱和点"
		}
		if i == 0 {
			fmt.Printf("  并发 %-4d  吞吐 %8.0f req/s\n", r.Concurrency, tp)
		} else {
			fmt.Printf("  并发 %-4d  吞吐 %8.0f req/s  (环比 %+.1f%%)%s\n", r.Concurrency, tp, growth, mark)
		}
	}
	if saturatedAt > 0 {
		fmt.Printf("  → 饱和并发 ≈ %d，峰值吞吐 %.0f req/s\n", saturatedAt, best)
	} else {
		fmt.Printf("  → 本次压测范围内未见饱和，峰值吞吐 %.0f req/s（继续加大 -stages 探测）\n", best)
	}
}

// reportPerSecond 打印逐秒吞吐，暴露「同等负载下随时间劣化」。
func reportPerSecond(results []*stageResult) {
	fmt.Println("\n【逐秒吞吐】观察同一并发下是否随时间劣化（EventBus 去重表膨胀所致）")
	for _, r := range results {
		if len(r.PerSec) == 0 {
			continue
		}
		fmt.Printf("  并发 %-4d: ", r.Concurrency)
		for i, v := range r.PerSec {
			if int64(i) >= int64(r.Elapsed.Seconds())+1 {
				break
			}
			fmt.Printf("%d ", v)
		}
		if len(r.PerSec) >= 3 && r.PerSec[0] > 0 {
			first, last := r.PerSec[0], r.PerSec[2]
			if first > 0 {
				fmt.Printf(" | 首秒 %d → 第3秒 %d (%+.0f%%)", first, last, float64(last-first)/float64(first)*100)
			}
		}
		fmt.Println()
	}
}

// reportResources 打印服务端资源增长，关联「吞吐下降」与「资源泄漏」。
func reportResources(results []*stageResult) {
	fmt.Println("\n【服务端资源与事件总线状态】")
	fmt.Printf("%-6s %10s %10s %11s %10s %9s %8s %8s\n",
		"并发", "gorout峰值", "gorout后", "history", "seen", "队列峰值", "丢弃", "GC次数")
	for _, r := range results {
		fmt.Printf("%-6d %10d %10d %11d %10d %9d %8d %8d\n",
			r.Concurrency, r.During.Goroutines, r.After.Goroutines,
			r.After.EventBus.HistoryLen, r.After.EventBus.SeenLen,
			r.During.EventBus.QueueLen, r.After.EventBus.Dropped, r.After.GC)
	}

	first, last := results[0], results[len(results)-1]
	fmt.Printf("\n  本次压测期间：history %d → %d（+%d，只增不减）\n",
		first.Before.EventBus.HistoryLen, last.After.EventBus.HistoryLen,
		last.After.EventBus.HistoryLen-first.Before.EventBus.HistoryLen)
	fmt.Printf("  堆内存 %.1fMB → %.1fMB（+%.1fMB）\n",
		float64(first.Before.HeapAlloc)/(1<<20), float64(last.After.HeapAlloc)/(1<<20),
		(float64(last.After.HeapAlloc)-float64(first.Before.HeapAlloc))/(1<<20))
	fmt.Printf("  事件通道容量 %d，队列峰值 %d（满即阻塞 HTTP 请求）\n",
		last.After.EventBus.QueueCap, maxQueue(results))
}

func maxQueue(results []*stageResult) int {
	m := 0
	for _, r := range results {
		if r.During.EventBus.QueueLen > m {
			m = r.During.EventBus.QueueLen
		}
	}
	return m
}
