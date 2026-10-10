package eventbus

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

// EventType 事件类型枚举
type EventType string

const (
	StudentSubmission  EventType = "student.submission"
	StudentQuestion    EventType = "student.question"
	StudentMessage     EventType = "student.message"
	AssessmentComplete EventType = "assessment.complete"
	MasteryUpdated     EventType = "assessment.mastery_updated"
	WeaknessDetected   EventType = "assessment.weakness_detected"
	TeachingResponse   EventType = "tutor.teaching_response"
	HintNeeded         EventType = "tutor.hint_needed"
	DifficultyAdjusted EventType = "tutor.difficulty_adjusted"
	PathUpdated        EventType = "curriculum.path_updated"
	ReviewScheduled    EventType = "curriculum.review_scheduled"
	NextTopic          EventType = "curriculum.next_topic"
	HintResponse       EventType = "hint.response"
	EngagementAlert    EventType = "engagement.alert"
	Encouragement      EventType = "engagement.encouragement"
	PaceAdjustment     EventType = "engagement.pace_adjustment"
)

// MaxHops 事件链最大跳数 -- 防活锁：超过即丢弃
const MaxHops = 8

// 环形缓冲容量：默认值与可调开关。
//
// 背景（改造前）：history 与 seen 都是"只追加、从不淘汰"的无界结构，
// 且 seen 的清理策略是"达到阈值后每次 Publish 都全表遍历（且持写锁）"，
// 导致两个后果——发布吞吐在 5000 条处出现 163 倍悬崖，内存永不释放。
// 详见 docs/压测报告.md §5 与手册附录 §B.3。
const (
	// DefaultHistoryCap history 环形缓冲默认容量（条）。
	// 实测 645 B/事件（含 Data map，见压测报告 §5.3），10 万条约 64 MB 常驻上限；
	// 满负载（~8000 事件/秒）下覆盖约 12 秒可回溯窗口。
	DefaultHistoryCap = 100000

	// DefaultSeenCap 去重表环形缓冲默认容量（条）。
	// 约 100 B/条，5 万条约 5 MB；满负载下覆盖约 6 秒去重窗口，
	// 足以覆盖任意一条事件链（链生命周期为毫秒级）。
	DefaultSeenCap = 50000
)

// envInt 读取正整数环境变量，缺省或非法时返回 def。
// 用途：让 history/seen 的容量上限可现场调整（EDU_HISTORY_CAP / EDU_SEEN_CAP），
// 而不必重新编译——容量是"内存占用"与"可回溯窗口"之间的权衡。
func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// Event 事件数据结构
type Event struct {
	ID            string                 `json:"id"`
	CorrelationID string                 `json:"correlation_id"` // 全链路追踪ID：同一事件链共享
	Hops          int                    `json:"hops"`           // 当前传播跳数
	Type          EventType              `json:"type"`
	Source        string                 `json:"source"`
	LearnerID     string                 `json:"learner_id"`
	Timestamp     time.Time              `json:"timestamp"`
	Data          map[string]interface{} `json:"data"`
}

// Handler 事件处理函数
type Handler func(Event)

// subscription 一次订阅登记：id 用于取消，handler 是回调。
type subscription struct {
	id      int64
	handler Handler
}

// EventBus 事件总线 -- Go版使用channel实现
//
// 生产级增强：
//  1. 全链路追踪：CorrelationID 在事件链中透传，支持按链路回放（GetTrace）
//  2. 防活锁：Hops 超过 MaxHops 的事件被丢弃，防止 Agent 互相触发形成无限循环
//  3. 事件去重：同一 CorrelationID 下相同类型的事件只处理一次，防止事件风暴
//  4. 有界内存：history 与 seen 均为固定容量环形缓冲，写入成本与内存占用恒为 O(1)，
//     不再随运行时间增长（改造前实测：5000 条处吞吐降 163 倍、645 B/事件永不释放）
//
// 边界说明（面试高频）：环形缓冲解决的是"内存与吞吐随时间劣化"，
// **不解决投递可靠性**。history 仍是进程内内存态、且满时覆盖最旧，因此
// 既活不过进程重启，也无法保证长期可回溯——投递语义依然是 at-most-once。
// 持久化与"一定送达"的完整方案见手册 §Q3.5。
type EventBus struct {
	subscribers map[EventType][]subscription
	allSubs     []subscription // SubscribeAll 登记的全量订阅（SSE 等转发场景）
	nextSubID   int64
	eventChan   chan Event

	// history 事件历史环形缓冲。
	// 未满时按普通切片追加；达到 histCap 后转为环形、覆盖最旧一条。
	// 因此恒有 len(history) <= histCap，最旧的有效元素位于 histStart。
	history   []Event
	histStart int
	histCap   int

	// seen 去重索引：map 负责 O(1) 判重，ring 负责保序淘汰。
	// 两者必须同步更新——ring 满时淘汰最旧的 key，并同步从 map 中删除，
	// 以此替代原先"阈值触发 + 写锁内全表遍历"的清理方式（O(n) → O(1)）。
	seen      map[string]struct{}
	seenRing  []string
	seenStart int
	seenCap   int

	dropped int64 // 被丢弃的事件数（防活锁 + 去重）

	// store 可选的持久化后端。为 nil 时行为与无持久化版本完全一致。
	// 启用后：New 时回放历史填充 history；Publish 在入队前追加落盘。
	store *Store

	mu sync.RWMutex
}

// Option 事件总线配置项。
type Option func(*EventBus)

// WithStore 启用事件持久化：
//   - New 时从 store 回放历史（只填 history，**不触发 handler**）
//   - Publish 时在入队前把事件追加落盘
//
// 不传该选项时，EventBus 行为与无持久化版本完全一致（默认纯内存）。
func WithStore(s *Store) Option {
	return func(b *EventBus) { b.store = s }
}

// New 创建EventBus实例。
//
// 可选传入 WithStore 启用持久化；不传则纯内存运行（默认，保持零配置可跑）。
func New(opts ...Option) *EventBus {
	bus := &EventBus{
		subscribers: make(map[EventType][]subscription),
		eventChan:   make(chan Event, 1000),

		// 惰性增长：初始只分配小容量，随事件增长到 histCap 后转为环形覆盖。
		// 这样短生命周期进程（如单测/评测）不会一上来就占用几十 MB。
		history: make([]Event, 0, 1024),
		histCap: envInt("EDU_HISTORY_CAP", DefaultHistoryCap),

		seen:     make(map[string]struct{}),
		seenRing: make([]string, 0, 1024),
		seenCap:  envInt("EDU_SEEN_CAP", DefaultSeenCap),
	}
	for _, opt := range opts {
		opt(bus)
	}
	// 回放必须在 dispatch 启动之前完成，否则回放进来的历史会被误当成新事件投递
	if bus.store != nil {
		bus.replayFromStore()
	}
	go bus.dispatch()
	return bus
}

// replayFromStore 从持久化日志回放历史到内存 history。
//
// ★ 关键语义：回放**只填充 history，不触发任何 handler**。
//
// 否则进程每重启一次，历史事件就会被重新处理一遍——那不是"持久化"，
// 是"重放风暴"：学生会收到重复的教学回复，掌握度被反复更新。
// 真正需要"重新消费"的场景，要靠消费位点（offset）区分已处理/未处理，
// 见手册 §Q3.5 第 2 步。
func (b *EventBus) replayFromStore() {
	events, err := b.store.LoadAll()
	if err != nil {
		log.Printf("[EventBus] replay from %s failed: %v", b.store.Path(), err)
		return
	}
	b.mu.Lock()
	for _, e := range events {
		b.appendHistoryLocked(e)
	}
	b.mu.Unlock()
	log.Printf("[EventBus] replayed %d event(s) from %s (history only, handlers NOT triggered)",
		len(events), b.store.Path())
}

// Subscribe 订阅某类事件，返回取消订阅的函数。
//
// 兼容旧用法：忽略返回值即表示"永久订阅"（进程生命周期内不取消）。
// 需要取消的场景（如 SSE 连接断开）才需要接住返回值。
func (b *EventBus) Subscribe(eventType EventType, handler Handler) func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.nextSubID
	b.nextSubID++
	b.subscribers[eventType] = append(b.subscribers[eventType],
		subscription{id: id, handler: handler})
	return func() { b.unsubscribe(eventType, id) }
}

// SubscribeAll 订阅**所有**事件类型，返回取消订阅的函数。
//
// 用途：SSE 这类"把某个 learner 的全部事件实时转发出去"的场景——
// 否则调用方要为 16 个事件类型各订阅一次。
func (b *EventBus) SubscribeAll(handler Handler) func() {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := b.nextSubID
	b.nextSubID++
	b.allSubs = append(b.allSubs, subscription{id: id, handler: handler})
	return func() { b.unsubscribeAll(id) }
}

// unsubscribe 移除某类型下指定 id 的订阅。
//
// 注意：这里**重建切片**而不是原地 append 删除——原地删除会改写底层数组，
// 而 dispatch 可能正在遍历旧切片，会读到错位元素（并发 bug 的经典来源）。
func (b *EventBus) unsubscribe(eventType EventType, id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	subs := b.subscribers[eventType]
	for i, s := range subs {
		if s.id == id {
			next := make([]subscription, 0, len(subs)-1)
			next = append(next, subs[:i]...)
			next = append(next, subs[i+1:]...)
			b.subscribers[eventType] = next
			return
		}
	}
}

// unsubscribeAll 移除 SubscribeAll 登记的指定 id 订阅。
func (b *EventBus) unsubscribeAll(id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, s := range b.allSubs {
		if s.id == id {
			next := make([]subscription, 0, len(b.allSubs)-1)
			next = append(next, b.allSubs[:i]...)
			next = append(next, b.allSubs[i+1:]...)
			b.allSubs = next
			return
		}
	}
}

// SubscriberCount 返回当前订阅总数（含全量订阅），用于观测与测试。
func (b *EventBus) SubscriberCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	n := len(b.allSubs)
	for _, subs := range b.subscribers {
		n += len(subs)
	}
	return n
}

// Publish 发布事件（非阻塞，写入channel）
// 返回值是注入追踪字段后的事件，便于调用方读取生成的 CorrelationID
func (b *EventBus) Publish(event Event) Event {
	if event.ID == "" {
		event.ID = newID()
	}
	if event.CorrelationID == "" {
		event.CorrelationID = event.ID
	}
	event.Hops++
	event.Timestamp = time.Now()

	// ── 防活锁：跳数上限 ──
	if event.Hops > MaxHops {
		b.mu.Lock()
		b.dropped++
		b.mu.Unlock()
		log.Printf("[EventBus] DROP livelock: correlation=%s hops=%d type=%s",
			event.CorrelationID, event.Hops, event.Type)
		return event
	}

	// ── 事件去重：同一事件链内同类型只处理一次 ──
	// 判重与记账均为 O(1)（map 查 + ring 写），无全表遍历、无清理步骤。
	dedupKey := event.CorrelationID + "|" + string(event.Type)
	b.mu.Lock()
	if _, dup := b.seen[dedupKey]; dup {
		b.dropped++
		b.mu.Unlock()
		log.Printf("[EventBus] DROP duplicate: correlation=%s type=%s", event.CorrelationID, event.Type)
		return event
	}
	b.rememberSeenLocked(dedupKey)
	b.appendHistoryLocked(event)
	b.mu.Unlock()

	// ── 持久化：先落盘，再投递 ──
	// 顺序很重要：日志写成功后才入队。这样即使紧接着进程崩溃，
	// 事件也已经在日志里，重启后可回放出来。
	// 落盘失败只记日志、不中断——持久化是"增强"，不该让教学流程停摆。
	if b.store != nil {
		if err := b.store.Append(event); err != nil {
			log.Printf("[EventBus] persist failed (corr=%s type=%s): %v",
				event.CorrelationID, event.Type, err)
		}
	}

	b.eventChan <- event
	return event
}

// appendHistoryLocked 把事件写入 history（调用方需持有写锁）。
//
// 未达容量上限时是普通追加；已满则覆盖最旧一条并推进起点。
// 两条路径都是 O(1)，因此写入成本不随运行时间增长。
func (b *EventBus) appendHistoryLocked(e Event) {
	if len(b.history) < b.histCap {
		b.history = append(b.history, e)
		return
	}
	// 已满：环形覆盖最旧一条（被覆盖事件的 Data map 随之可被 GC）
	b.history[b.histStart] = e
	b.histStart = (b.histStart + 1) % b.histCap
}

// rememberSeenLocked 记录去重键（调用方需持有写锁）。
//
// map 提供 O(1) 判重，ring 提供保序淘汰：满时淘汰最旧的键并同步清理 map，
// 使内存有确定上限，且不再需要"全表遍历找过期项"的清理步骤。
func (b *EventBus) rememberSeenLocked(key string) {
	if len(b.seenRing) < b.seenCap {
		b.seenRing = append(b.seenRing, key)
		b.seen[key] = struct{}{}
		return
	}
	// 已满：淘汰最旧的键，再写入新键
	old := b.seenRing[b.seenStart]
	delete(b.seen, old)
	b.seenRing[b.seenStart] = key
	b.seenStart = (b.seenStart + 1) % b.seenCap
	b.seen[key] = struct{}{}
}

// forEachHistoryLocked 按时间顺序（最旧 → 最新）遍历 history（调用方需持有读锁）。
// fn 返回 false 可提前终止遍历。
func (b *EventBus) forEachHistoryLocked(fn func(Event) bool) {
	n := len(b.history)
	for i := 0; i < n; i++ {
		idx := i
		if n == b.histCap {
			// 已转环形：从起点开始按模绕回，恢复时间顺序
			idx = (b.histStart + i) % b.histCap
		}
		if !fn(b.history[idx]) {
			return
		}
	}
}

// PublishChild 由 Agent 内部转发事件时使用：继承父事件的 CorrelationID 与跳数，
// 使整条事件链可被追踪，同时受 MaxHops 保护
func (b *EventBus) PublishChild(parent Event, child Event) Event {
	child.CorrelationID = parent.CorrelationID
	child.Hops = parent.Hops
	return b.Publish(child)
}

// DroppedCount 返回因防活锁/去重丢弃的事件数
func (b *EventBus) DroppedCount() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.dropped
}

// HistoryLen 返回 history 当前条数。
//
// 有界：恒 <= HistoryCap()。改造前该值随进程运行时间无上限增长
// （实测 6 秒内涨到 106,447 条、堆 119.7 MB），现被容量上限钉住。
func (b *EventBus) HistoryLen() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.history)
}

// HistoryCap 返回 history 环形缓冲的容量上限（条）。
// 默认 DefaultHistoryCap，可用环境变量 EDU_HISTORY_CAP 覆盖。
func (b *EventBus) HistoryCap() int {
	return b.histCap
}

// SeenLen 返回去重表当前条目数。
//
// 有界：恒 <= SeenCap()。改造前该表在"只删 5 分钟前记录"的策略下
// 于压测期间只增不减，且每次 Publish 都要全表遍历一次。
func (b *EventBus) SeenLen() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.seen)
}

// SeenCap 返回去重表的容量上限（条）。
// 默认 DefaultSeenCap，可用环境变量 EDU_SEEN_CAP 覆盖。
func (b *EventBus) SeenCap() int {
	return b.seenCap
}

// PersistEnabled 返回是否启用了持久化（即是否传入了 WithStore）。
// 供 /debug/stats 与验收脚本判断当前运行形态。
func (b *EventBus) PersistEnabled() bool {
	return b.store != nil
}

// QueueLen 返回事件通道当前积压长度，用于观测消费是否跟得上生产。
func (b *EventBus) QueueLen() int {
	return len(b.eventChan)
}

// QueueCap 返回事件通道容量。
func (b *EventBus) QueueCap() int {
	return cap(b.eventChan)
}

// dispatch 事件分发goroutine
func (b *EventBus) dispatch() {
	for event := range b.eventChan {
		b.mu.RLock()
		handlers := b.subscribers[event.Type]
		all := b.allSubs
		b.mu.RUnlock()

		log.Printf("[EventBus] %s -> %s (learner=%s, corr=%s, hops=%d)",
			event.Source, event.Type, event.LearnerID, shortID(event.CorrelationID), event.Hops)

		// 类型订阅与全量订阅（SSE 等）各起一个 goroutine
		for _, s := range handlers {
			go b.runHandler(s.handler, event)
		}
		for _, s := range all {
			go b.runHandler(s.handler, event)
		}
	}
}

// runHandler 在独立 goroutine 中执行 handler，并隔离 panic——
// 单个 handler 崩掉不应影响其他订阅者，也不应拖垮 dispatch。
func (b *EventBus) runHandler(h Handler, e Event) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[EventBus] Handler panic: %v", r)
		}
	}()
	h(e)
}

// GetHistory 获取事件历史
func (b *EventBus) GetHistory(learnerID string, limit int) []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var filtered []Event
	b.forEachHistoryLocked(func(e Event) bool {
		if learnerID == "" || e.LearnerID == learnerID {
			filtered = append(filtered, e)
		}
		return true
	})
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[len(filtered)-limit:]
	}
	return filtered
}

// GetTrace 按 CorrelationID 回放完整事件链 -- 全链路追踪的核心能力
//
// 注意：只覆盖仍在 history 环形缓冲内的链路。被容量淘汰的旧链路、
// 以及进程重启前的链路都查不到——这是 at-most-once 语义的直接体现。
func (b *EventBus) GetTrace(correlationID string) []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var trace []Event
	b.forEachHistoryLocked(func(e Event) bool {
		if e.CorrelationID == correlationID {
			trace = append(trace, e)
		}
		return true
	})
	return trace
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

// String 便于日志输出
func (e Event) String() string {
	return fmt.Sprintf("%s[%s]", e.Type, e.Source)
}
