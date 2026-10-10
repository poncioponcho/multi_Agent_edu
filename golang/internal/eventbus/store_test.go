package eventbus

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestStoreAppendAndLoad 追加与回放往返（含 Data map 的完整往返）。
func TestStoreAppendAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer s.Close()

	want := []Event{
		{ID: "e1", CorrelationID: "c1", Type: StudentSubmission, Source: "test", LearnerID: "L1"},
		{ID: "e2", CorrelationID: "c1", Type: AssessmentComplete, Source: "test", LearnerID: "L1",
			Data: map[string]interface{}{"mastery": 0.42}},
	}
	for _, e := range want {
		if err := s.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	got, err := s.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("回放条数: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Type != want[i].Type {
			t.Fatalf("第 %d 条不符: got %+v want %+v", i, got[i], want[i])
		}
	}
	if m, _ := got[1].Data["mastery"].(float64); m != 0.42 {
		t.Fatalf("Data 未正确往返: %v", got[1].Data)
	}
}

// TestStoreSkipsMalformedLine 进程被 kill 时最后一行很可能只写了一半，
// 回放必须跳过它、而不是整个文件报错导致服务起不来。
func TestStoreSkipsMalformedLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s, _ := OpenStore(path)
	if err := s.Append(Event{ID: "ok1", Type: StudentSubmission, Source: "t"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	s.Close()

	// 手工追加一行"写了一半"的 JSON
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("打开日志追加半行: %v", err)
	}
	if _, err := f.WriteString(`{"id":"trunc","type":"student.sub`); err != nil {
		t.Fatalf("写半行: %v", err)
	}
	f.Close()

	s2, _ := OpenStore(path)
	defer s2.Close()
	got, err := s2.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll 不应因半行而报错: %v", err)
	}
	if len(got) != 1 || got[0].ID != "ok1" {
		t.Fatalf("应只回放出完整的那条: got %d 条", len(got))
	}
}

// TestStoreLoadEmpty 空日志应回放 0 条且不报错。
func TestStoreLoadEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer s.Close()

	got, err := s.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("空文件应回放 0 条: got %d", len(got))
	}
}

// TestStoreCloseThenAppend 关闭后追加应返回错误而不是 panic。
func TestStoreCloseThenAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s, _ := OpenStore(path)
	s.Close()
	if err := s.Append(Event{ID: "x"}); err == nil {
		t.Fatal("关闭后 Append 应返回错误")
	}
}

// ★ TestReplayDoesNotDispatch 回放只填 history，绝不触发 handler。
//
// 这是持久化最容易搞错的地方：如果回放走的是 Publish 路径，
// 进程每重启一次历史事件就会被重新处理一遍——学生会收到重复回复、
// 掌握度被反复更新。那不是"持久化"，是"重放风暴"。
func TestReplayDoesNotDispatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")

	// ── 第一个"进程"：写入两条事件 ──
	store1, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	bus1 := New(WithStore(store1))
	if !bus1.PersistEnabled() {
		t.Fatal("WithStore 未生效")
	}
	bus1.Publish(Event{Type: StudentSubmission, Source: "p1", LearnerID: "L1"})
	bus1.Publish(Event{Type: AssessmentComplete, Source: "p1", LearnerID: "L1"})
	time.Sleep(100 * time.Millisecond)
	store1.Close()

	// ── 第二个"进程"：重启并回放 ──
	store2, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer store2.Close()

	var dispatched atomic.Int64
	bus2 := New(WithStore(store2))
	// 订阅发生在 New 之后（回放已在 New 内同步完成），因此能验证回放期间无投递
	bus2.Subscribe(StudentSubmission, func(Event) { dispatched.Add(1) })
	bus2.Subscribe(AssessmentComplete, func(Event) { dispatched.Add(1) })
	time.Sleep(200 * time.Millisecond)

	if got := bus2.HistoryLen(); got != 2 {
		t.Fatalf("回放后 history 应有 2 条: got %d", got)
	}
	if got := dispatched.Load(); got != 0 {
		t.Fatalf("★ 回放不应触发 handler: got %d 次投递", got)
	}
	if got := len(bus2.GetHistory("L1", 0)); got != 2 {
		t.Fatalf("回放的历史应可查: got %d", got)
	}
}

// TestPersistenceSurvivesRestart 端到端：写入 → 关停 → 重启 → 同一条链仍完整。
//
// 这正是 verify_persistence.sh 在进程级别验证的行为，这里用两个 EventBus
// 实例在单进程内模拟（"重启"的本质就是内存状态被丢弃）。
func TestPersistenceSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")

	store1, _ := OpenStore(path)
	bus1 := New(WithStore(store1))
	root := bus1.Publish(Event{Type: StudentSubmission, Source: "api", LearnerID: "L-RESTART"})
	bus1.PublishChild(root, Event{Type: AssessmentComplete, Source: "agent", LearnerID: "L-RESTART"})
	time.Sleep(100 * time.Millisecond)
	store1.Close()

	store2, _ := OpenStore(path)
	defer store2.Close()
	bus2 := New(WithStore(store2))

	trace := bus2.GetTrace(root.CorrelationID)
	if len(trace) != 2 {
		t.Fatalf("重启后同一条链应完整: got %d want 2", len(trace))
	}
}

// TestNoStoreKeepsInMemoryBehavior 不传 WithStore 时行为与改造前一致（默认纯内存）。
func TestNoStoreKeepsInMemoryBehavior(t *testing.T) {
	bus := New()
	if bus.PersistEnabled() {
		t.Fatal("默认不应启用持久化")
	}
	bus.Publish(Event{Type: StudentSubmission, Source: "s", LearnerID: "L1"})
	if got := bus.HistoryLen(); got != 1 {
		t.Fatalf("history 应记录: got %d", got)
	}
}
