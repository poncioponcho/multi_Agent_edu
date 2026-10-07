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
	"errors"
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

	flagMode       = flag.String("mode", "rest", "压测模式：rest | ws")
	flagWSPath     = flag.String("ws-path", "/ws", "WebSocket 路径前缀（服务端为 /ws/{learner_id}）")
	flagWSSameUser = flag.Bool("ws-same-learner", false, "所有连接共用同一个 learner_id（测同一学习者的广播扇出放大）")
	flagWSIdle     = flag.Duration("ws-idle", 300*time.Millisecond, "判定一批响应结束的空闲阈值；同时是「等首个响应」的超时上限")
)

// ── 服务端指标 ──

// serverStats 对应 /api/v1/debug/stats 的响应
type serverStats struct {
	UptimeSeconds float64 `json:"uptime_seconds"`
	Goroutines    int     `json:"goroutines"`
	HeapAlloc     uint64  `json:"heap_alloc_bytes"`
	GC            uint32  `json:"gc_count"`
	Learners      int     `json:"learners"`
	// 以下两项由 Python 服务暴露（Go 服务无 WebSocket，恒为 0）
	WSConns  int `json:"ws_connections"`
	Tasks    int `json:"asyncio_tasks"`
	EventBus struct {
		HistoryLen int   `json:"history_len"`
		SeenLen    int   `json:"seen_len"`
		QueueLen   int   `json:"queue_len"`
		QueueCap   int   `json:"queue_cap"`
		Dropped    int64 `json:"dropped"`
	} `json:"eventbus"`
}

// statsSampler 在后台按固定间隔拉取服务端指标并记录峰值
type statsSampler struct {
	peak *serverStats
	mu   sync.Mutex
	stop chan struct{}
	wg   sync.WaitGroup
}

// startStatsSampler 启动采样器。client 为 nil 时返回 nil（表示本次不采样）。
func startStatsSampler(client *http.Client, base string) *statsSampler {
	if client == nil {
		return nil
	}
	s := &statsSampler{peak: &serverStats{}, stop: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(*flagSampleInt)
		defer ticker.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				st, err := fetchStats(client, base)
				if err != nil {
					continue
				}
				s.mu.Lock()
				if st.Goroutines > s.peak.Goroutines {
					s.peak.Goroutines = st.Goroutines
				}
				if st.HeapAlloc > s.peak.HeapAlloc {
					s.peak.HeapAlloc = st.HeapAlloc
				}
				if st.EventBus.QueueLen > s.peak.EventBus.QueueLen {
					s.peak.EventBus.QueueLen = st.EventBus.QueueLen
				}
				if st.EventBus.SeenLen > s.peak.EventBus.SeenLen {
					s.peak.EventBus.SeenLen = st.EventBus.SeenLen
				}
				if st.WSConns > s.peak.WSConns {
					s.peak.WSConns = st.WSConns
				}
				if st.Tasks > s.peak.Tasks {
					s.peak.Tasks = st.Tasks
				}
				s.peak.EventBus.HistoryLen = st.EventBus.HistoryLen
				s.mu.Unlock()
			}
		}
	}()
	return s
}

// stopAndPeak 停止采样并返回峰值快照（nil 安全）
func (s *statsSampler) stopAndPeak() *serverStats {
	if s == nil {
		return &serverStats{}
	}
	close(s.stop)
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak
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

	// WebSocket 模式专有
	ConnectErrs int64   // 握手失败次数
	Fanout      float64 // 平均每个请求收到多少条广播消息
	MsgsPerSec  float64 // 服务端实际推送的消息速率（广播量，才是服务端真实工作量）

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

	isWS := strings.EqualFold(*flagMode, "ws")
	wsBase := ""
	if isWS {
		wsBase = strings.Replace(base, "https://", "wss://", 1)
		wsBase = strings.Replace(wsBase, "http://", "ws://", 1)
		wsBase = strings.TrimRight(wsBase, "/") + *flagWSPath
		if strings.HasPrefix(wsBase, "wss://") {
			fmt.Fprintln(os.Stderr, "提示：ws 模式暂不支持 wss://（未实现 TLS），请用 ws:// 直连后端")
		}
	}

	// 预热
	if *flagWarmup > 0 {
		fmt.Printf("预热 %v（并发 %d）...\n", *flagWarmup, concurrencies[0])
		if isWS {
			runWSStage(wsBase, *flagLearners, concurrencies[0], *flagWarmup, nil, base)
		} else {
			runStage(client, base, endpoint, payloads, concurrencies[0], *flagWarmup, nil)
		}
	}

	if isWS {
		shared := "每连接独立 learner"
		if *flagWSSameUser {
			shared = "全部连接共用 1 个 learner（测广播扇出）"
		}
		fmt.Printf("\n压测目标：%s/{learner_id}    %s    每级时长：%v\n",
			wsBase, shared, *flagDuration)
	} else {
		fmt.Printf("\n压测目标：%s    learner 基数：%d    每级时长：%v\n",
			endpoint, *flagLearners, *flagDuration)
	}
	fmt.Println(strings.Repeat("─", 118))
	if isWS {
		fmt.Printf("%-6s %10s %10s %9s %9s %8s %8s %9s\n",
			"连接数", "完整往返/s", "消息/s", "p50", "p99", "错误", "握手失败", "扇出/请求")
	} else {
		fmt.Printf("%-6s %10s %9s %9s %9s %9s %9s %8s %10s\n",
			"并发", "吞吐req/s", "p50", "p90", "p99", "max", "错误", "gorout", "内存MB")
	}
	fmt.Println(strings.Repeat("─", 118))

	var results []*stageResult
	for _, c := range concurrencies {
		var r *stageResult
		if isWS {
			r = runWSStage(wsBase, *flagLearners, c, *flagDuration, client, base)
		} else {
			r = runStage(client, base, endpoint, payloads, c, *flagDuration, client)
		}
		results = append(results, r)
		if isWS {
			fmt.Printf("%-6d %10.0f %10.0f %9s %9s %8d %8d %9.1f\n",
				r.Concurrency, r.throughput(), r.MsgsPerSec,
				fmtDur(r.percentile(50)), fmtDur(r.percentile(99)),
				r.Errors, r.ConnectErrs, r.Fanout)
		} else {
			fmt.Printf("%-6d %10.0f %9s %9s %9s %9s %9d %8d %10.1f\n",
				r.Concurrency, r.throughput(),
				fmtDur(r.percentile(50)), fmtDur(r.percentile(90)),
				fmtDur(r.percentile(99)), fmtDur(r.percentile(100)),
				r.Errors, peakGoroutines(r), float64(r.After.HeapAlloc)/(1<<20))
		}
	}
	fmt.Println(strings.Repeat("─", 118))

	// WebSocket 模式额外汇报连接容量与广播规模
	if isWS {
		reportWS(results)
	}

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
	sampler := startStatsSampler(statsClient, base)

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

	res.During = sampler.stopAndPeak()

	if statsClient != nil {
		if s, err := fetchStats(statsClient, base); err == nil {
			res.After = s
		}
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

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket 模式
// ─────────────────────────────────────────────────────────────────────────────

// runWSStage 跑一级 WebSocket 并发（每个 worker = 一条长连接）。
//
// 每轮流程：发一条 submit → 读到首个响应记 RTT → 继续读到空闲为止，排空这
// 一批广播（服务端对每个事件单独发一条消息，并会广播给该 learner 的全部连接）。
//
// ⚠️ 测量口径：为界定一批响应的边界需要等待空闲，连接会有少量空转，因此本
// 模式测的是**连接容量、广播扇出规模与消息往返延迟**，不是峰值吞吐
// （峰值吞吐请用 rest 模式）。
func runWSStage(wsBase string, learners, connections int, d time.Duration, statsClient *http.Client, base string) *stageResult {
	res := &stageResult{Concurrency: connections}

	if statsClient != nil {
		if s, err := fetchStats(statsClient, base); err == nil {
			res.Before = s
		}
	}

	buckets := make([]atomic.Int64, int(d.Seconds())+2)
	perWorkerLat := make([][]time.Duration, connections)
	var totalReq, totalErr, totalMsgs, totalConnErr atomic.Int64

	sampler := startStatsSampler(statsClient, base)

	start := time.Now()
	deadline := start.Add(d)

	var wg sync.WaitGroup
	for w := 0; w < connections; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			learnerIdx := workerID
			if *flagWSSameUser {
				learnerIdx = 0
			}
			learnerID := "L-" + strconv.Itoa(learnerIdx%learners)
			target := wsBase + "/" + learnerID

			conn, err := wsDial(target, *flagTimeout)
			if err != nil {
				totalConnErr.Add(1)
				return
			}
			defer conn.close()

			msg := []byte(`{"action":"submit","knowledge_id":"kp-1","is_correct":true,"time_spent_seconds":1.2}`)
			lat := make([]time.Duration, 0, 64)
			var myMsgs int64

		workerLoop:
			for {
				if time.Now().After(deadline) {
					break
				}
				t0 := time.Now()
				if err := conn.writeText(msg); err != nil {
					totalErr.Add(1)
					break
				}

				var firstRTT time.Duration
				var got int64
				for {
					idle := time.Now().Add(*flagWSIdle)
					if idle.After(deadline) {
						idle = deadline
					}
					if _, err := conn.readMessage(idle); err != nil {
						if errors.Is(err, ErrWSClosed) {
							totalErr.Add(1)
							break workerLoop
						}
						break // 读超时：这一批响应结束
					}
					got++
					if got == 1 {
						firstRTT = time.Since(t0)
					}
					myMsgs++
				}

				if got == 0 {
					// 若是因整体时长到期而读超时，不算错误，直接结束
					if time.Now().After(deadline) {
						break
					}
					totalErr.Add(1)
					continue
				}
				lat = append(lat, firstRTT)
				totalReq.Add(1)
				// 逐秒分桶只统计「完成的请求」，与 rest 模式口径一致；
				// 广播消息量由 Fanout 单独表达，混在一起会让该列失去意义
				if sec := int(time.Since(start).Seconds()); sec >= 0 && sec < len(buckets) {
					buckets[sec].Add(1)
				}
			}

			perWorkerLat[workerID] = lat
			totalMsgs.Add(myMsgs)
		}(w)
	}
	wg.Wait()
	res.Elapsed = time.Since(start)

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
	res.ConnectErrs = totalConnErr.Load()
	if rq := totalReq.Load(); rq > 0 {
		res.Fanout = float64(totalMsgs.Load()) / float64(rq)
	}
	if res.Elapsed > 0 {
		res.MsgsPerSec = float64(totalMsgs.Load()) / res.Elapsed.Seconds()
	}

	res.PerSec = make([]int64, len(buckets))
	for i := range buckets {
		res.PerSec[i] = buckets[i].Load()
	}

	res.During = sampler.stopAndPeak()
	if statsClient != nil {
		if s, err := fetchStats(statsClient, base); err == nil {
			res.After = s
		}
	}
	if res.Before == nil {
		res.Before = &serverStats{}
	}
	if res.After == nil {
		res.After = &serverStats{}
	}
	return res
}

// reportWS 汇报 WebSocket 专有结论：连接容量与广播扇出
//
// 口径说明：「完整往返/s」指一个 submit 及其整批广播都被读完的轮次，
// 受 -ws-idle（判定批次结束的空闲阈值）约束，因此它衡量的是**交互节奏**，
// 不是服务端峰值吞吐。「消息/s」才是服务端实际推送量，更能反映真实工作量。
func reportWS(results []*stageResult) {
	fmt.Println("\n【WebSocket 连接容量与广播扇出】")
	fmt.Printf("%-8s %12s %10s %12s %12s %14s\n",
		"连接数", "完整往返/s", "消息/s", "握手失败", "扇出/请求", "服务端连接峰值")
	for _, r := range results {
		fmt.Printf("%-8d %12.0f %10.0f %12d %12.1f %14d\n",
			r.Concurrency, r.throughput(), r.MsgsPerSec,
			r.ConnectErrs, r.Fanout, r.During.WSConns)
	}

	last := results[len(results)-1]
	if last.ConnectErrs > 0 {
		fmt.Printf("\n  ⚠️ 有 %d 次握手失败：并发 %d 时服务端已无法接受全部连接\n",
			last.ConnectErrs, last.Concurrency)
	} else {
		fmt.Printf("\n  ✅ 并发 %d 条连接全部握手成功\n", last.Concurrency)
	}
	if last.Fanout > 1 {
		fmt.Printf("  每个请求平均收到 %.1f 条广播消息（服务端对每个事件单独发一条）\n", last.Fanout)
	}
	if last.After.EventBus.HistoryLen > 0 {
		fmt.Printf("  服务端事件历史累积到 %d 条（只增不减）\n", last.After.EventBus.HistoryLen)
	}
	if last.During.Tasks > 0 {
		fmt.Printf("  服务端 asyncio 任务峰值 %d\n", last.During.Tasks)
	}
}
