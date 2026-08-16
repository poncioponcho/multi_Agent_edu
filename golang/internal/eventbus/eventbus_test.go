package eventbus

import (
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
