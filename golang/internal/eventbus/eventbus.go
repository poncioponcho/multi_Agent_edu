package eventbus

import (
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"time"
)

// EventType 事件类型枚举
type EventType string

const (
	StudentSubmission EventType = "student.submission"
	StudentQuestion   EventType = "student.question"
	StudentMessage    EventType = "student.message"
	AssessmentComplete EventType = "assessment.complete"
	MasteryUpdated    EventType = "assessment.mastery_updated"
	WeaknessDetected  EventType = "assessment.weakness_detected"
	TeachingResponse  EventType = "tutor.teaching_response"
	HintNeeded        EventType = "tutor.hint_needed"
	DifficultyAdjusted EventType = "tutor.difficulty_adjusted"
	PathUpdated       EventType = "curriculum.path_updated"
	ReviewScheduled   EventType = "curriculum.review_scheduled"
	NextTopic         EventType = "curriculum.next_topic"
	HintResponse      EventType = "hint.response"
	EngagementAlert   EventType = "engagement.alert"
	Encouragement     EventType = "engagement.encouragement"
	PaceAdjustment    EventType = "engagement.pace_adjustment"
)

// Event 事件数据结构
type Event struct {
	ID        string                 `json:"id"`
	Type      EventType              `json:"type"`
	Source    string                 `json:"source"`
	LearnerID string                `json:"learner_id"`
	Timestamp time.Time             `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

// Handler 事件处理函数
type Handler func(Event)

// EventBus 事件总线 -- Go版使用channel实现
//
// 面试要点：
// - Go channel vs Python asyncio: channel是CSP模型，asyncio是协程模型
// - channel天然支持多生产者多消费者
// - select语句可以同时监听多个channel
type EventBus struct {
	subscribers map[EventType][]Handler
	eventChan   chan Event
	history     []Event
	mu          sync.RWMutex
}

// New 创建EventBus实例
func New() *EventBus {
	bus := &EventBus{
		subscribers: make(map[EventType][]Handler),
		eventChan:   make(chan Event, 1000), // 缓冲channel，防止阻塞
		history:     make([]Event, 0),
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
func (b *EventBus) Publish(event Event) {
	if event.ID == "" {
		b := make([]byte, 8)
		rand.Read(b)
		event.ID = fmt.Sprintf("%x", b)
	}
	event.Timestamp = time.Now()

	b.mu.Lock()
	b.history = append(b.history, event)
	b.mu.Unlock()

	b.eventChan <- event
}

// dispatch 事件分发goroutine
func (b *EventBus) dispatch() {
	for event := range b.eventChan {
		b.mu.RLock()
		handlers := b.subscribers[event.Type]
		b.mu.RUnlock()

		log.Printf("[EventBus] %s -> %s (learner=%s)", event.Source, event.Type, event.LearnerID)

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
