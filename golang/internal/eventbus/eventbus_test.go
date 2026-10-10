package eventbus

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestLivelockProtection 防活锁：两个handler互相触发事件，
// 若不设跳数上限将无限循环，MaxHops 应截断事件链
func TestLivelockProtection(t *testing.T) {
	bus := New()

	var aCount, bCount atomic.Int64
	bus.Subscribe("test.a", func(e Event) {
		aCount.Add(1)
		if e.Hops < MaxHops {
			bus.PublishChild(e, Event{Type: "test.b", Source: "A"})
		}
	})
	bus.Subscribe("test.b", func(e Event) {
		bCount.Add(1)
		if e.Hops < MaxHops {
			bus.PublishChild(e, Event{Type: "test.a", Source: "B"})
		}
	})

	bus.Publish(Event{Type: "test.a", Source: "root"})

	// 等待事件链传播结束
	time.Sleep(500 * time.Millisecond)

	if dropped := bus.DroppedCount(); dropped == 0 {
		t.Fatalf("expected dropped events > 0 for livelock loop, got 0")
	}
	// 每条链最多 MaxHops 跳，两个类型交替，各自计数 <= MaxHops
	if aCount.Load() > MaxHops || bCount.Load() > MaxHops {
		t.Fatalf("livelock not bounded: a=%d b=%d (max=%d)", aCount.Load(), bCount.Load(), MaxHops)
	}
}

// TestDedup 事件去重：同一 CorrelationID 下相同类型只处理一次
func TestDedup(t *testing.T) {
	bus := New()
	var count atomic.Int64
	bus.Subscribe("test.dup", func(e Event) { count.Add(1) })

	bus.Publish(Event{Type: "test.dup", Source: "s1"})
	bus.Publish(Event{Type: "test.dup", Source: "s1"}) // 新 correlation → 允许
	time.Sleep(200 * time.Millisecond)
	if count.Load() != 2 {
		t.Fatalf("expected 2 distinct events processed, got %d", count.Load())
	}
}

// TestTrace 全链路追踪：子事件继承 CorrelationID，可完整回放
func TestTrace(t *testing.T) {
	bus := New()
	bus.Subscribe("test.t1", func(e Event) {
		bus.PublishChild(e, Event{Type: "test.t2", Source: "child"})
	})

	root := bus.Publish(Event{Type: "test.t1", Source: "root", LearnerID: "l1"})
	time.Sleep(200 * time.Millisecond)

	trace := bus.GetTrace(root.CorrelationID)
	if len(trace) != 2 {
		t.Fatalf("expected trace of 2 events, got %d", len(trace))
	}
	for _, e := range trace {
		if e.CorrelationID != root.CorrelationID {
			t.Fatalf("correlation id broken: got %s want %s", e.CorrelationID, root.CorrelationID)
		}
	}
	// 跳数递增：root=1, child=2
	if trace[0].Hops != 1 || trace[1].Hops != 2 {
		t.Fatalf("hops not increasing: %d %d", trace[0].Hops, trace[1].Hops)
	}
}

// TestHandlerPanicIsolation handler panic 不影响其他handler
func TestHandlerPanicIsolation(t *testing.T) {
	bus := New()
	var count atomic.Int64
	bus.Subscribe("test.panic", func(e Event) { panic("boom") })
	bus.Subscribe("test.panic", func(e Event) { count.Add(1) })

	bus.Publish(Event{Type: "test.panic", Source: "s"})
	time.Sleep(200 * time.Millisecond)

	if count.Load() != 1 {
		t.Fatalf("expected healthy handler to run, got %d", count.Load())
	}
}

// TestDedupSameChain 同一 correlationID 下同类型事件只处理一次（第二条被丢弃）。
//
// 这是把「去重键粒度」这一已知设计缺陷固化成可观测断言：
// 当前键是 correlationID|type，因此同一条链里业务上合法的第二次同类型事件
// 会被当成重复丢掉。将来若把键改为 event.ID，本用例会失败——
// 那时应当同步更新本用例与手册 §Q3.5。
func TestDedupSameChain(t *testing.T) {
	bus := New()
	var count atomic.Int64
	bus.Subscribe("test.dup2", func(Event) { count.Add(1) })

	bus.Publish(Event{Type: "test.dup2", Source: "s", CorrelationID: "chain-x"})
	bus.Publish(Event{Type: "test.dup2", Source: "s", CorrelationID: "chain-x"}) // 同链同类型 → 丢弃
	time.Sleep(200 * time.Millisecond)

	if got := count.Load(); got != 1 {
		t.Fatalf("同链同类型未被去重: got %d want 1", got)
	}
	if got := bus.DroppedCount(); got != 1 {
		t.Fatalf("dropped 计数不符: got %d want 1", got)
	}
}

// TestHistoryRingBuffer history 环形缓冲：超出容量后只保留最新 cap 条，
// GetHistory 仍按时间顺序（旧→新）返回，被淘汰的事件查不到。
func TestHistoryRingBuffer(t *testing.T) {
	t.Setenv("EDU_HISTORY_CAP", "5")
	bus := New()
	if got := bus.HistoryCap(); got != 5 {
		t.Fatalf("HistoryCap 未受环境变量控制: got %d want 5", got)
	}

	for i := 0; i < 8; i++ {
		bus.Publish(Event{
			Type:      StudentSubmission,
			Source:    "test",
			LearnerID: fmt.Sprintf("l-%d", i),
		})
	}

	if got := bus.HistoryLen(); got != 5 {
		t.Fatalf("history 未受容量限制: got %d want 5", got)
	}

	// 只应保留最后 5 条：l-3 .. l-7，且顺序为旧→新
	hist := bus.GetHistory("", 0)
	if len(hist) != 5 {
		t.Fatalf("GetHistory 条数: got %d want 5", len(hist))
	}
	for i, e := range hist {
		want := fmt.Sprintf("l-%d", i+3)
		if e.LearnerID != want {
			t.Fatalf("第 %d 条 learner 不符: got %s want %s（淘汰或排序有误）", i, e.LearnerID, want)
		}
	}

	// 被淘汰的最旧一条不应再能查到
	if got := bus.GetHistory("l-0", 0); len(got) != 0 {
		t.Fatalf("被淘汰的事件仍可查到: %d 条", len(got))
	}
}

// TestHistoryRingKeepsTrace 环形缓冲绕回后，仍在窗口内的链路依旧可完整回放；
// 一旦被挤出窗口，链路就只能查到残余部分——这正是"窗口有限"的语义。
//
// 注意：本用例用 PublishChild 手动串联，不走订阅 + 异步 handler——
// 否则"订阅时机"会让事件顺序变得不确定（订阅前发布的事件也会被 dispatch 到）。
func TestHistoryRingKeepsTrace(t *testing.T) {
	t.Setenv("EDU_HISTORY_CAP", "4")
	bus := New()

	// 先灌入 3 条无关事件，把缓冲推到接近满
	for i := 0; i < 3; i++ {
		bus.Publish(Event{Type: StudentSubmission, Source: "noise", LearnerID: "other"})
	}

	// 再发一条链的两条事件（同步串联，顺序确定）
	root := bus.Publish(Event{Type: StudentSubmission, Source: "root", LearnerID: "l1"})
	bus.PublishChild(root, Event{Type: AssessmentComplete, Source: "child", LearnerID: "l1"})

	trace := bus.GetTrace(root.CorrelationID)
	if len(trace) != 2 {
		t.Fatalf("环形缓冲内链路应完整: got %d want 2", len(trace))
	}
	if trace[0].Hops != 1 || trace[1].Hops != 2 {
		t.Fatalf("链路顺序/跳数有误: %d %d", trace[0].Hops, trace[1].Hops)
	}

	// 再灌 3 条（容量 4），把 root 挤出窗口 → 链路只剩后半段 child
	for i := 0; i < 3; i++ {
		bus.Publish(Event{Type: StudentSubmission, Source: "noise", LearnerID: "other"})
	}
	if got := len(bus.GetTrace(root.CorrelationID)); got != 1 {
		t.Fatalf("root 应已被挤出窗口、仅剩 child: got %d want 1", got)
	}
}

// TestSeenRingEviction 去重表环形淘汰：容量满后淘汰最旧的 key，
// 于是更早的 correlationID 可以重新通过——去重窗口是有限窗口，而非永久。
func TestSeenRingEviction(t *testing.T) {
	t.Setenv("EDU_SEEN_CAP", "3")
	bus := New()
	if got := bus.SeenCap(); got != 3 {
		t.Fatalf("SeenCap 未受环境变量控制: got %d want 3", got)
	}

	var count atomic.Int64
	bus.Subscribe(StudentSubmission, func(Event) { count.Add(1) })

	bus.Publish(Event{Type: StudentSubmission, Source: "s", CorrelationID: "c1"})
	// 再发 3 条不同的链，把 c1 挤出容量为 3 的去重窗口
	for i := 2; i <= 4; i++ {
		bus.Publish(Event{Type: StudentSubmission, Source: "s", CorrelationID: fmt.Sprintf("c%d", i)})
	}

	if got := bus.SeenLen(); got != 3 {
		t.Fatalf("seen 未受容量限制: got %d want 3", got)
	}

	// dispatch 是异步的：先等前 4 条处理完再取基线，否则读到的是 0
	time.Sleep(200 * time.Millisecond)
	before := count.Load()
	if before != 4 {
		t.Fatalf("前 4 条应各处理一次: got %d want 4", before)
	}

	// c1 已被淘汰 → 同链同类型事件应能再次通过
	bus.Publish(Event{Type: StudentSubmission, Source: "s", CorrelationID: "c1"})
	time.Sleep(200 * time.Millisecond)
	if got := count.Load(); got != before+1 {
		t.Fatalf("淘汰后的 key 未能重新通过: got %d want %d", got, before+1)
	}
}

// TestPublishThroughputStable 哨兵：发布成本不随去重表规模增长。
//
// 改造前，去重表超过 5000 条后每次发布都要全表遍历，导致吞吐悬崖。
// 这里用「前 200 次发布」与「第 3000~3200 次发布」两段的平均耗时对比，
// 若后者显著慢于前者（超过 5 倍），说明又引入了随规模劣化的清理逻辑。
func TestPublishThroughputStable(t *testing.T) {
	if testing.Short() {
		t.Skip("跳过吞吐哨兵（-short）")
	}
	bus := New()
	const probe = 200

	measure := func(start int) time.Duration {
		t0 := time.Now()
		for i := start; i < start+probe; i++ {
			bus.Publish(Event{
				Type:      StudentSubmission,
				Source:    "bench",
				LearnerID: fmt.Sprintf("learner-%d", i%100),
			})
		}
		return time.Since(t0) / probe
	}

	early := measure(0)
	for i := probe; i < 3000; i++ { // 把去重表推过旧的 5000 阈值量级
		bus.Publish(Event{Type: StudentSubmission, Source: "fill", LearnerID: "x"})
	}
	late := measure(3000)

	t.Logf("每事件平均耗时：前 %d 次=%v，第 3000+ 次=%v（seen=%d）",
		probe, early, late, bus.SeenLen())

	if late > early*5 {
		t.Fatalf("发布成本随规模劣化：early=%v late=%v（疑似重新引入全表清理）", early, late)
	}
}

// TestSubscribeAllAndCancel 全量订阅与取消。
//
// 取消必须真正生效：SSE 每条连接都会 SubscribeAll 一次，连接断开若不取消，
// 订阅数会随连接数单调累积（内存泄漏 + 每条事件被重复处理）。
func TestSubscribeAllAndCancel(t *testing.T) {
	bus := New()
	before := bus.SubscriberCount()

	var count atomic.Int64
	cancel := bus.SubscribeAll(func(Event) { count.Add(1) })
	if got := bus.SubscriberCount(); got != before+1 {
		t.Fatalf("SubscribeAll 未登记: %d -> %d", before, got)
	}

	bus.Publish(Event{Type: StudentSubmission, Source: "s", LearnerID: "L1"})
	time.Sleep(200 * time.Millisecond)
	if count.Load() == 0 {
		t.Fatal("全量订阅应收到事件")
	}

	cancel()
	if got := bus.SubscriberCount(); got != before {
		t.Fatalf("取消后订阅数应回落: got %d want %d", got, before)
	}

	n := count.Load()
	bus.Publish(Event{Type: StudentSubmission, Source: "s", LearnerID: "L1"})
	time.Sleep(200 * time.Millisecond)
	if got := count.Load(); got != n {
		t.Fatalf("取消后不应再收到事件: %d -> %d", n, got)
	}
}

// TestSubscribeCancelOnlyRemovesTarget 取消一个订阅不应影响同类型的其他订阅者。
func TestSubscribeCancelOnlyRemovesTarget(t *testing.T) {
	bus := New()
	var a, b atomic.Int64
	cancelA := bus.Subscribe("test.multi", func(Event) { a.Add(1) })
	bus.Subscribe("test.multi", func(Event) { b.Add(1) })

	cancelA()
	bus.Publish(Event{Type: "test.multi", Source: "s"})
	time.Sleep(200 * time.Millisecond)

	if a.Load() != 0 {
		t.Fatalf("已取消的订阅不应再收到: %d", a.Load())
	}
	if b.Load() != 1 {
		t.Fatalf("同类型其他订阅者应正常收到: %d", b.Load())
	}
}
