package eventbus

import (
	"fmt"
	"io"
	"log"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMain 丢弃日志输出。
//
// EventBus 在 Publish 与 dispatch 里各打一条 log.Printf，压测/基准下这些
// 日志会淹没测试输出、并且 I/O 本身会污染测量结果（实测日志是本系统
// 最重的单点开销，见 docs/压测报告.md）。这里统一静音，只测总线本身。
func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	m.Run()
}

// ─────────────────────────────────────────────────────────────────────────────
// 基准测试：EventBus 吞吐与读写放大
//
// 运行：
//   go test ./internal/eventbus/ -bench . -benchmem -benchtime=20000x
//   go test ./internal/eventbus/ -bench BenchmarkPublish$ -benchtime=1000x   # 观察曲线
//   go test ./internal/eventbus/ -bench BenchmarkPublish$ -benchtime=100000x
// ─────────────────────────────────────────────────────────────────────────────

func benchEvent(i int) Event {
	return Event{
		Type:      StudentSubmission,
		Source:    "bench",
		LearnerID: fmt.Sprintf("learner-%d", i%100),
		Data:      map[string]interface{}{"knowledge_id": "kp-1", "is_correct": true},
	}
}

// BenchmarkPublish 无订阅者的顺序发布：测量纯入队成本（含去重表写入）。
//
// 历史背景：改造前 b.N 次发布会让去重表增长到 b.N 条，一旦超过
// MaxSeenEntries(5000)，cleanSeenLocked 就会在**每次**发布时全表遍历一次，
// 于是用不同 -benchtime 跑这个基准，ns/op 会在 5000 处出现台阶式跃升
// （5000→50000 事件，吞吐降 163 倍）。
//
// 现在去重表是固定容量环形索引，判重与淘汰均为 O(1)、无清理步骤，
// ns/op 应随 b.N 保持平稳——本基准因此同时是「悬崖不复发」的哨兵。
func BenchmarkPublish(b *testing.B) {
	bus := New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(benchEvent(i))
	}
}

// BenchmarkPublishWithSubscribers 模拟真实系统：5 个订阅者各订阅一类事件。
// 每发布一个事件，dispatch 会为每个 handler 各起一个 goroutine，
// 因此这里测到的是「发布成本 + goroutine 创建成本」。
func BenchmarkPublishWithSubscribers(b *testing.B) {
	bus := New()
	for i := 0; i < 5; i++ {
		bus.Subscribe(StudentSubmission, func(Event) {})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(benchEvent(i))
	}
}

// BenchmarkPublishParallel 并发发布，测量写锁（含去重表清理）的争用程度。
func BenchmarkPublishParallel(b *testing.B) {
	bus := New()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			bus.Publish(benchEvent(i))
			i++
		}
	})
}

// prefillHistory 通过真实发布路径把 history 填充到 n 条，用于测量读接口
// 随 history 增长的劣化。填充时间不计入基准计时。
func prefillHistory(n int) *EventBus {
	bus := New()
	for i := 0; i < n; i++ {
		bus.Publish(benchEvent(i))
	}
	return bus
}

// BenchmarkGetHistory history 越长，GetHistory 需要全量扫描并持读锁越久。
// 子基准按 history 规模分组，直接给出 O(n) 劣化的斜率。
func BenchmarkGetHistory(b *testing.B) {
	for _, n := range []int{1000, 5000, 20000} {
		b.Run(fmt.Sprintf("history=%d", n), func(b *testing.B) {
			bus := prefillHistory(n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bus.GetHistory("learner-0", 20)
			}
		})
	}
}

// BenchmarkGetTrace 全链路回放接口，同样是对 history 的全量扫描。
func BenchmarkGetTrace(b *testing.B) {
	for _, n := range []int{1000, 5000, 20000} {
		b.Run(fmt.Sprintf("history=%d", n), func(b *testing.B) {
			bus := prefillHistory(n)
			// 取一条真实存在的 correlation id，让扫描走到最后才命中。
			// 注意 history 已是环形缓冲：最旧的有效元素在 histStart 处，
			// 未满时 histStart 恒为 0，满时指向真正的起点。
			bus.mu.RLock()
			target := bus.history[bus.histStart].CorrelationID
			bus.mu.RUnlock()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bus.GetTrace(target)
			}
		})
	}
}

// BenchmarkPublishSeenTableCost 隔离去重表规模对发布成本的影响。
//
// prefilled 表示发布前去重表里已有多少条记录，覆盖三种形态：
//   - 1000  → 远未达上限，纯 map 写入
//   - 20000 → 未达上限（默认 50000）
//   - 60000 → 已超上限，每次写入都触发一次环形淘汰
//
// 改造前这三个档位的 ns/op 差异巨大（阈值以上每次发布全表遍历）；
// 改造后三者应基本持平。若再次出现随表规模上升，说明又引入了
// 随表大小劣化的清理逻辑——本基准是它的回归哨兵。
func BenchmarkPublishSeenTableCost(b *testing.B) {
	for _, prefilled := range []int{1000, 20000, 60000} {
		b.Run(fmt.Sprintf("seen=%d", prefilled), func(b *testing.B) {
			bus := New()
			for i := 0; i < prefilled; i++ {
				bus.Publish(benchEvent(i))
			}
			// 去重表有容量上限：填满后 SeenLen 停在 cap，不再等于 prefilled
			want := prefilled
			if lim := bus.SeenCap(); want > lim {
				want = lim
			}
			if got := bus.SeenLen(); got != want {
				b.Fatalf("prefill 未生效: seen=%d want=%d (cap=%d)", got, want, bus.SeenCap())
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bus.Publish(benchEvent(i))
			}
		})
	}
}

// BenchmarkPublishWithStore 测量启用持久化（EDU_EVENT_LOG）后发布路径的代价。
//
// 与 BenchmarkPublish 的差值 = 每次发布多出的「JSON 序列化 + 一次 write(2)」成本。
// 这个数字是"持久化值不值"的核心依据，也是面试时要能报出来的取舍。
func BenchmarkPublishWithStore(b *testing.B) {
	store, err := OpenStore(filepath.Join(b.TempDir(), "bench-events.jsonl"))
	if err != nil {
		b.Fatalf("OpenStore: %v", err)
	}
	defer store.Close()

	bus := New(WithStore(store))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(benchEvent(i))
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 观测量测试：不是断言正确性，而是把「看不见的资源增长」变成可记录的数字。
// 这些用例永远通过，数据用 -v 查看，用于生成 docs/压测报告.md。
// ─────────────────────────────────────────────────────────────────────────────

// TestMeasureHistoryBoundedGrowth 观测 history 的容量上限是否真正生效。
//
// 改造前这里是「只增不减」的观测：20000 事件 = 12.3 MB 且永不释放，
// 外推 1000 万事件约 6.15 GB。改造后 history 是固定容量环形缓冲，
// 因此本用例改为验证：无论发布多少，HistoryLen 都不超过 HistoryCap。
func TestMeasureHistoryBoundedGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过内存观测（-short）")
	}

	const events = 20000
	bus := New()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	start := time.Now()
	for i := 0; i < events; i++ {
		bus.Publish(benchEvent(i))
	}
	elapsed := time.Since(start)

	runtime.GC()
	runtime.ReadMemStats(&after)

	heapDelta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("发布 %d 个事件：history=%d/%d, seen=%d/%d, 耗时=%v (%.0f events/s)",
		events, bus.HistoryLen(), bus.HistoryCap(), bus.SeenLen(), bus.SeenCap(),
		elapsed, float64(events)/elapsed.Seconds())
	t.Logf("HeapAlloc 增量：%.2f MB（≈ %.0f bytes/事件）",
		float64(heapDelta)/(1<<20), float64(heapDelta)/float64(events))
	t.Logf("内存上限：history 满 %d 条约 %.1f MB（不再随运行时间增长）",
		bus.HistoryCap(), 645.0*float64(bus.HistoryCap())/(1<<20))

	want := events
	if lim := bus.HistoryCap(); want > lim {
		want = lim
	}
	if bus.HistoryLen() != want {
		t.Fatalf("history 条数不符: got %d want %d (cap=%d)", bus.HistoryLen(), want, bus.HistoryCap())
	}
}

// TestMeasureGoroutinePeak 观测 dispatch 为每个 handler 各起一个 goroutine
// 带来的并发度膨胀。
//
// 做法：让 5 个 handler 各阻塞 50ms，然后一次性灌入一批事件；用采样器
// 每 2ms 记录一次 runtime.NumGoroutine()，取峰值。
// 峰值应接近 events × handlers，即并发度与流量成正比、无任何上限。
func TestMeasureGoroutinePeak(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过 goroutine 观测（-short）")
	}

	const (
		events      = 3000
		subscribers = 5
		handlerHold = 50 * time.Millisecond
	)

	bus := New()
	for i := 0; i < subscribers; i++ {
		bus.Subscribe(StudentSubmission, func(Event) { time.Sleep(handlerHold) })
	}

	baseline := runtime.NumGoroutine()

	var peak atomic.Int64
	peak.Store(int64(baseline))
	stop := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Add(1)
	go func() {
		defer sampler.Done()
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				n := int64(runtime.NumGoroutine())
				for {
					cur := peak.Load()
					if n <= cur || peak.CompareAndSwap(cur, n) {
						break
					}
				}
			}
		}
	}()

	for i := 0; i < events; i++ {
		bus.Publish(benchEvent(i))
	}
	time.Sleep(2 * handlerHold) // 等 handler 全部结束
	close(stop)
	sampler.Wait()

	observedPeak := peak.Load()
	t.Logf("基线 goroutine=%d，灌入 %d 事件 × %d handler 后峰值=%d",
		baseline, events, subscribers, observedPeak)
	t.Logf("峰值/基线 = %.1f 倍；理论上限 events×handlers = %d",
		float64(observedPeak)/float64(baseline), events*subscribers)
	t.Logf("外推：10 万事件 × 5 handler 将瞬时创建约 50 万个 goroutine（按 8KB 栈 ≈ 4GB 虚拟内存）")

	if observedPeak <= int64(baseline) {
		t.Fatalf("未观测到 goroutine 增长：baseline=%d peak=%d", baseline, observedPeak)
	}
}
