package eventbus

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
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

// MaxSeenEntries seen 表容量上限，超过触发过期清理
const MaxSeenEntries = 5000

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

// EventBus 事件总线 -- Go版使用channel实现
//
// 生产级增强（本次迭代）：
//  1. 全链路追踪：CorrelationID 在事件链中透传，支持按链路回放（GetTrace）
//  2. 防活锁：Hops 超过 MaxHops 的事件被丢弃，防止 Agent 互相触发形成无限循环
//  3. 事件去重：同一 CorrelationID 下相同类型的事件只处理一次，防止事件风暴
type EventBus struct {
	subscribers map[EventType][]Handler
	eventChan   chan Event
	history     []Event
	seen        map[string]time.Time // dedupKey(correlationID|type) -> 首次时间
	dropped     int64                // 被丢弃的事件数（防活锁+去重）
	mu          sync.RWMutex
}

// New 创建EventBus实例
func New() *EventBus {
	bus := &EventBus{
		subscribers: make(map[EventType][]Handler),
		eventChan:   make(chan Event, 1000),
		history:     make([]Event, 0),
		seen:        make(map[string]time.Time),
	}
	go bus.dispatch()
	return bus
}

// Subscribe 订阅事件
func (b *EventBus) Subscribe(eventType EventType, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[eventType] = append(b.subscribers[eventType], handler)
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
	dedupKey := event.CorrelationID + "|" + string(event.Type)
	b.mu.Lock()
	if _, dup := b.seen[dedupKey]; dup {
		b.dropped++
		b.mu.Unlock()
		log.Printf("[EventBus] DROP duplicate: correlation=%s type=%s", event.CorrelationID, event.Type)
		return event
	}
	b.seen[dedupKey] = time.Now()
	b.cleanSeenLocked()
	b.history = append(b.history, event)
	b.mu.Unlock()

	b.eventChan <- event
	return event
}

// PublishChild 由 Agent 内部转发事件时使用：继承父事件的 CorrelationID 与跳数，
// 使整条事件链可被追踪，同时受 MaxHops 保护
func (b *EventBus) PublishChild(parent Event, child Event) Event {
	child.CorrelationID = parent.CorrelationID
	child.Hops = parent.Hops
	return b.Publish(child)
}

// cleanSeenLocked 清理超过5分钟的去重记录（调用方需持有写锁）
func (b *EventBus) cleanSeenLocked() {
	if len(b.seen) < MaxSeenEntries {
		return
	}
	cutoff := time.Now().Add(-5 * time.Minute)
	for k, t := range b.seen {
		if t.Before(cutoff) {
			delete(b.seen, k)
		}
	}
}

// DroppedCount 返回因防活锁/去重丢弃的事件数
func (b *EventBus) DroppedCount() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.dropped
}

// HistoryLen 返回事件历史长度。
//
// 注意：history 目前只追加、从不淘汰，因此该值等于进程生命周期内
// 处理过的全部事件数。压测用它观测内存增长（见 docs/压测报告.md）。
func (b *EventBus) HistoryLen() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.history)
}

// SeenLen 返回去重表当前条目数。
//
// 去重表仅在超过 MaxSeenEntries(5000) 时清理一次，且只删除 5 分钟前的记录，
// 高吞吐下该表会持续增长；压测用它观测去重表的膨胀与清理开销。
func (b *EventBus) SeenLen() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.seen)
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
		b.mu.RUnlock()

		log.Printf("[EventBus] %s -> %s (learner=%s, corr=%s, hops=%d)",
			event.Source, event.Type, event.LearnerID, shortID(event.CorrelationID), event.Hops)

		for _, handler := range handlers {
			// 每个handler在独立goroutine中执行
			go func(h Handler, e Event) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[EventBus] Handler panic: %v", r)
					}
				}()
				h(e)
			}(handler, event)
		}
	}
}

// GetHistory 获取事件历史
func (b *EventBus) GetHistory(learnerID string, limit int) []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var filtered []Event
	for _, e := range b.history {
		if learnerID == "" || e.LearnerID == learnerID {
			filtered = append(filtered, e)
		}
	}
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[len(filtered)-limit:]
	}
	return filtered
}

// GetTrace 按 CorrelationID 回放完整事件链 -- 全链路追踪的核心能力
func (b *EventBus) GetTrace(correlationID string) []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var trace []Event
	for _, e := range b.history {
		if e.CorrelationID == correlationID {
			trace = append(trace, e)
		}
	}
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
