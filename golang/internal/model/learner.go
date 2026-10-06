package model

import (
	"math"
	"sync"
	"time"
)

// KnowledgeState 单个知识点的掌握状态
type KnowledgeState struct {
	KnowledgeID  string    `json:"knowledge_id"`
	Mastery      float64   `json:"mastery"`
	Attempts     int       `json:"attempts"`
	CorrectCount int       `json:"correct_count"`
	Streak       int       `json:"streak"`
	LastAttempt  time.Time `json:"last_attempt"`
}

// Level 返回掌握等级
func (s *KnowledgeState) Level() string {
	if s.Attempts == 0 {
		return "not_started"
	}
	switch {
	case s.Mastery < 0.3:
		return "beginner"
	case s.Mastery < 0.6:
		return "developing"
	case s.Mastery < 0.85:
		return "proficient"
	default:
		return "mastered"
	}
}

// BKT 参数
const (
	pInit    = 0.1
	pTransit = 0.15
	pGuess   = 0.2
	pSlip    = 0.1
)

// LearnerModel 学习者模型 -- BKT实现
type LearnerModel struct {
	LearnerID string
	states    map[string]*KnowledgeState
	mu        sync.RWMutex
}

// NewLearnerModel 创建学习者模型
func NewLearnerModel(learnerID string) *LearnerModel {
	return &LearnerModel{
		LearnerID: learnerID,
		states:    make(map[string]*KnowledgeState),
	}
}

// getStateLocked 返回知识点状态指针（不存在则创建）。
//
// 调用方必须已持有 m.mu 写锁。之所以需要这个内部版本：sync.RWMutex 不可重入，
// UpdateMastery 已持写锁时若再调用 GetState（其内部要取锁）会自死锁。
func (m *LearnerModel) getStateLocked(knowledgeID string) *KnowledgeState {
	s, ok := m.states[knowledgeID]
	if !ok {
		s = &KnowledgeState{KnowledgeID: knowledgeID, Mastery: pInit}
		m.states[knowledgeID] = s
	}
	return s
}

// GetState 获取知识点状态的**快照副本**（不存在则先创建）。
//
// 返回副本而非内部指针：内部指针会在调用方读取期间被 UpdateMastery 并发写入，
// 把指针交出去等于把数据竞争暴露给调用方（handler 层就曾因此触发 -race）。
func (m *LearnerModel) GetState(knowledgeID string) KnowledgeState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return *m.getStateLocked(knowledgeID)
}

// Mastery 只读地查询某知识点的掌握度。
//
// 与 GetState 的两点区别：① 只持读锁；② 不创建不存在的条目。
// API 层的只读接口（如 /next-topics）应使用它——用 GetState 会导致每个
// GET 请求都为全部知识点创建条目并反复取写锁，属于读接口写放大。
func (m *LearnerModel) Mastery(knowledgeID string) float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.states[knowledgeID]; ok {
		return s.Mastery
	}
	return pInit
}

// UpdateMastery BKT核心算法
//
// 并发安全：整个「读 Mastery → 贝叶斯更新 → 写回 Mastery/Attempts/Streak/...」
// 必须在同一把写锁内完成，并且返回值是快照副本。
//
// 修复前该 RMW 在锁外执行（GetState 只在取指针时持锁），同一 learner 的
// 并发提交会读到同一个旧 Mastery 并互相覆盖——经典的 lost update，静默丢数据。
// 回归测试见 learner_concurrency_test.go::TestUpdateMasteryConcurrentLostUpdate。
func (m *LearnerModel) UpdateMastery(knowledgeID string, isCorrect bool) KnowledgeState {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.getStateLocked(knowledgeID)
	pL := state.Mastery

	var pObsGivenL, pObsGivenNotL float64
	if isCorrect {
		pObsGivenL = 1 - pSlip
		pObsGivenNotL = pGuess
		state.CorrectCount++
		state.Streak++
	} else {
		pObsGivenL = pSlip
		pObsGivenNotL = 1 - pGuess
		state.Streak = 0
	}

	pObs := pL*pObsGivenL + (1-pL)*pObsGivenNotL
	pLGivenObs := pL
	if pObs > 0 {
		pLGivenObs = (pL * pObsGivenL) / pObs
	}
	pLNew := pLGivenObs + (1-pLGivenObs)*pTransit

	state.Mastery = math.Max(0, math.Min(1, pLNew))
	state.Attempts++
	state.LastAttempt = time.Now()
	return *state
}

// GetWeakPoints 获取薄弱知识点（返回快照副本，不暴露内部指针）
func (m *LearnerModel) GetWeakPoints(threshold float64) []KnowledgeState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var weak []KnowledgeState
	for _, s := range m.states {
		if s.Mastery < threshold && s.Attempts > 0 {
			weak = append(weak, *s)
		}
	}
	return weak
}

// ReviewItem SM-2复习条目
type ReviewItem struct {
	KnowledgeID    string    `json:"knowledge_id"`
	EasinessFactor float64   `json:"easiness_factor"`
	IntervalDays   float64   `json:"interval_days"`
	Repetition     int       `json:"repetition"`
	NextReview     time.Time `json:"next_review"`
}

// NewReviewItem 创建复习条目
func NewReviewItem(knowledgeID string) *ReviewItem {
	return &ReviewItem{
		KnowledgeID:    knowledgeID,
		EasinessFactor: 2.5,
		NextReview:     time.Now(),
	}
}

// SM2Review SM-2算法核心
func SM2Review(item *ReviewItem, quality int) {
	if quality < 0 {
		quality = 0
	}
	if quality > 5 {
		quality = 5
	}

	newEF := item.EasinessFactor + (0.1 - float64(5-quality)*(0.08+float64(5-quality)*0.02))
	item.EasinessFactor = math.Max(1.3, math.Min(2.5, newEF))

	if quality < 3 {
		item.Repetition = 0
		item.IntervalDays = 1
	} else {
		switch item.Repetition {
		case 0:
			item.IntervalDays = 1
		case 1:
			item.IntervalDays = 6
		default:
			item.IntervalDays = item.IntervalDays * item.EasinessFactor
		}
		item.Repetition++
	}

	item.NextReview = time.Now().Add(time.Duration(item.IntervalDays*24) * time.Hour)
}
