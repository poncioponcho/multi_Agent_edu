package agent

import (
	"fmt"
	"log"
	"sync"

	"github.com/multi-agent-education/golang/internal/eventbus"
	"github.com/multi-agent-education/golang/internal/model"
)

// ─── Assessment Agent ───

type AssessmentAgent struct {
	bus     *eventbus.EventBus
	Models  map[string]*model.LearnerModel
	mu      sync.RWMutex
}

func NewAssessmentAgent(bus *eventbus.EventBus) *AssessmentAgent {
	return &AssessmentAgent{bus: bus, Models: make(map[string]*model.LearnerModel)}
}

func (a *AssessmentAgent) Start() {
	a.bus.Subscribe(eventbus.StudentSubmission, a.handleSubmission)
}

func (a *AssessmentAgent) GetModel(learnerID string) *model.LearnerModel {
	a.mu.Lock()
	defer a.mu.Unlock()
	m, ok := a.Models[learnerID]
	if !ok {
		m = model.NewLearnerModel(learnerID)
		a.Models[learnerID] = m
	}
	return m
}

func (a *AssessmentAgent) handleSubmission(event eventbus.Event) {
	knowledgeID, _ := event.Data["knowledge_id"].(string)
	isCorrect, _ := event.Data["is_correct"].(bool)

	m := a.GetModel(event.LearnerID)
	state := m.UpdateMastery(knowledgeID, isCorrect)

	log.Printf("[Assessment] learner=%s, kp=%s, correct=%v, mastery=%.3f (%s)",
		event.LearnerID, knowledgeID, isCorrect, state.Mastery, state.Level())

	a.bus.Publish(eventbus.Event{
		Type: eventbus.MasteryUpdated, Source: "AssessmentAgent",
		LearnerID: event.LearnerID,
		Data: map[string]interface{}{
			"knowledge_id": knowledgeID, "mastery": state.Mastery,
			"level": state.Level(), "is_correct": isCorrect, "attempts": state.Attempts,
		},
	})

	if state.Mastery < 0.3 && state.Attempts >= 3 {
		a.bus.Publish(eventbus.Event{
			Type: eventbus.WeaknessDetected, Source: "AssessmentAgent",
			LearnerID: event.LearnerID,
			Data: map[string]interface{}{"knowledge_id": knowledgeID, "mastery": state.Mastery},
		})
	}

	a.bus.Publish(eventbus.Event{
		Type: eventbus.AssessmentComplete, Source: "AssessmentAgent",
		LearnerID: event.LearnerID,
		Data: map[string]interface{}{
			"knowledge_id": knowledgeID, "mastery": state.Mastery,
			"level": state.Level(), "is_correct": isCorrect,
		},
	})
}

// ─── Tutor Agent ───

type TutorAgent struct {
	bus      *eventbus.EventBus
	attempts map[string]int
	mu       sync.Mutex
}

func NewTutorAgent(bus *eventbus.EventBus) *TutorAgent {
	return &TutorAgent{bus: bus, attempts: make(map[string]int)}
}

func (t *TutorAgent) Start() {
	t.bus.Subscribe(eventbus.AssessmentComplete, t.handleAssessment)
	t.bus.Subscribe(eventbus.EngagementAlert, t.handleEngagement)
}

func (t *TutorAgent) handleAssessment(event eventbus.Event) {
	knowledgeID, _ := event.Data["knowledge_id"].(string)
	level, _ := event.Data["level"].(string)
	isCorrect, _ := event.Data["is_correct"].(bool)

	if !isCorrect {
		key := event.LearnerID + ":" + knowledgeID
		t.mu.Lock()
		t.attempts[key]++
		attempts := t.attempts[key]
		t.mu.Unlock()
		if attempts >= 2 {
			t.bus.Publish(eventbus.Event{
				Type: eventbus.HintNeeded, Source: "TutorAgent",
				LearnerID: event.LearnerID,
				Data: map[string]interface{}{
					"knowledge_id": knowledgeID, "attempts": attempts,
					"mastery": event.Data["mastery"],
				},
			})
			return
		}
	}

	var response string
	if isCorrect {
		response = fmt.Sprintf("很好！你在「%s」表现不错。你能用自己的话解释一下吗？", knowledgeID)
	} else {
		response = fmt.Sprintf("没关系，让我们分析「%s」。你觉得卡在了哪一步？", knowledgeID)
	}

	t.bus.Publish(eventbus.Event{
		Type: eventbus.TeachingResponse, Source: "TutorAgent",
		LearnerID: event.LearnerID,
		Data: map[string]interface{}{
			"knowledge_id": knowledgeID, "response": response,
			"teaching_style": "socratic", "difficulty_level": level,
		},
	})
}

func (t *TutorAgent) handleEngagement(event eventbus.Event) {
	alertType, _ := event.Data["alert_type"].(string)
	data := map[string]interface{}{}
	if alertType == "frustration" {
		data["action"] = "decrease"
		data["message"] = "让我们换一个角度，从更简单的地方开始。"
	} else if alertType == "boredom" {
		data["action"] = "increase"
		data["message"] = "让我给你一个更有挑战性的问题！"
	}
	if len(data) > 0 {
		t.bus.Publish(eventbus.Event{
			Type: eventbus.DifficultyAdjusted, Source: "TutorAgent",
			LearnerID: event.LearnerID, Data: data,
		})
	}
}

// ─── Curriculum Agent ───

type CurriculumAgent struct {
	bus         *eventbus.EventBus
	reviewItems map[string]map[string]*model.ReviewItem
	mu          sync.Mutex
}

func NewCurriculumAgent(bus *eventbus.EventBus) *CurriculumAgent {
	return &CurriculumAgent{bus: bus, reviewItems: make(map[string]map[string]*model.ReviewItem)}
}

func (c *CurriculumAgent) Start() {
	c.bus.Subscribe(eventbus.MasteryUpdated, c.handleMasteryUpdate)
	c.bus.Subscribe(eventbus.WeaknessDetected, c.handleWeakness)
}

func (c *CurriculumAgent) handleMasteryUpdate(event eventbus.Event) {
	knowledgeID, _ := event.Data["knowledge_id"].(string)
	mastery, _ := event.Data["mastery"].(float64)

	c.mu.Lock()
	if _, ok := c.reviewItems[event.LearnerID]; !ok {
		c.reviewItems[event.LearnerID] = make(map[string]*model.ReviewItem)
	}
	item, ok := c.reviewItems[event.LearnerID][knowledgeID]
	if !ok {
		item = model.NewReviewItem(knowledgeID)
		c.reviewItems[event.LearnerID][knowledgeID] = item
	}
	c.mu.Unlock()

	quality := masteryToQuality(mastery)
	model.SM2Review(item, quality)

	log.Printf("[Curriculum] learner=%s, kp=%s, EF=%.2f, interval=%.1fd",
		event.LearnerID, knowledgeID, item.EasinessFactor, item.IntervalDays)
}

func (c *CurriculumAgent) handleWeakness(event eventbus.Event) {
	c.bus.Publish(eventbus.Event{
		Type: eventbus.PathUpdated, Source: "CurriculumAgent",
		LearnerID: event.LearnerID,
		Data: map[string]interface{}{
			"reason":            "weakness_detected",
			"weak_knowledge_id": event.Data["knowledge_id"],
			"message":           "检测到薄弱知识点，建议先复习前置知识",
		},
	})
}

func masteryToQuality(mastery float64) int {
	switch {
	case mastery >= 0.9:
		return 5
	case mastery >= 0.75:
		return 4
	case mastery >= 0.6:
		return 3
	case mastery >= 0.4:
		return 2
	case mastery >= 0.2:
		return 1
	default:
		return 0
	}
}

// ─── Hint Agent ───

type HintAgent struct {
	bus     *eventbus.EventBus
	history map[string]int
	mu      sync.Mutex
}

func NewHintAgent(bus *eventbus.EventBus) *HintAgent {
	return &HintAgent{bus: bus, history: make(map[string]int)}
}

func (h *HintAgent) Start() {
	h.bus.Subscribe(eventbus.HintNeeded, h.handleHintNeeded)
}

func (h *HintAgent) handleHintNeeded(event eventbus.Event) {
	knowledgeID, _ := event.Data["knowledge_id"].(string)
	mastery, _ := event.Data["mastery"].(float64)

	key := event.LearnerID + ":" + knowledgeID
	h.mu.Lock()
	h.history[key]++
	hintCount := h.history[key]
	h.mu.Unlock()

	level := 1
	if mastery < 0.15 && hintCount >= 3 {
		level = 3
	} else if hintCount > 3 {
		level = 3
	} else if hintCount > 1 {
		level = 2
	}

	var hintText, levelName string
	switch level {
	case 1:
		levelName = "metacognitive"
		hintText = fmt.Sprintf("💡 关于「%s」：想一想，题目里有哪些关键信息？", knowledgeID)
	case 2:
		levelName = "scaffolding"
		hintText = fmt.Sprintf("📝 关于「%s」：试着回忆相关的公式，然后一步步来。", knowledgeID)
	default:
		levelName = "targeted"
		hintText = fmt.Sprintf("📖 关于「%s」：让我帮你梳理解题思路。", knowledgeID)
	}

	log.Printf("[Hint] learner=%s, kp=%s, level=%s", event.LearnerID, knowledgeID, levelName)

	h.bus.Publish(eventbus.Event{
		Type: eventbus.HintResponse, Source: "HintAgent",
		LearnerID: event.LearnerID,
		Data: map[string]interface{}{
			"knowledge_id": knowledgeID, "hint_level": level,
			"hint_level_name": levelName, "hint_text": hintText,
		},
	})
}

// ─── Engagement Agent ───

type EngagementAgent struct {
	bus         *eventbus.EventBus
	engagements map[string]*learnerEngagement
	mu          sync.Mutex
}

type learnerEngagement struct {
	consecutiveErrors  int
	consecutiveCorrect int
	recentResults      []bool
}

func NewEngagementAgent(bus *eventbus.EventBus) *EngagementAgent {
	return &EngagementAgent{bus: bus, engagements: make(map[string]*learnerEngagement)}
}

func (e *EngagementAgent) Start() {
	e.bus.Subscribe(eventbus.StudentSubmission, e.trackSubmission)
	e.bus.Subscribe(eventbus.AssessmentComplete, e.analyze)
}

func (e *EngagementAgent) trackSubmission(event eventbus.Event) {
	eng := e.getEngagement(event.LearnerID)
	isCorrect, _ := event.Data["is_correct"].(bool)

	e.mu.Lock()
	eng.recentResults = append(eng.recentResults, isCorrect)
	if len(eng.recentResults) > 20 {
		eng.recentResults = eng.recentResults[len(eng.recentResults)-20:]
	}
	if isCorrect {
		eng.consecutiveCorrect++
		eng.consecutiveErrors = 0
	} else {
		eng.consecutiveErrors++
		eng.consecutiveCorrect = 0
	}
	e.mu.Unlock()
}

func (e *EngagementAgent) analyze(event eventbus.Event) {
	eng := e.getEngagement(event.LearnerID)

	if eng.consecutiveErrors >= 3 {
		e.bus.Publish(eventbus.Event{
			Type: eventbus.EngagementAlert, Source: "EngagementAgent",
			LearnerID: event.LearnerID,
			Data: map[string]interface{}{
				"alert_type": "frustration",
				"message":    "别灰心！犯错是学习的一部分。",
			},
		})
	} else if eng.consecutiveCorrect >= 5 {
		correct := 0
		for _, r := range eng.recentResults {
			if r {
				correct++
			}
		}
		accuracy := float64(correct) / float64(len(eng.recentResults))
		if accuracy > 0.9 {
			e.bus.Publish(eventbus.Event{
				Type: eventbus.EngagementAlert, Source: "EngagementAgent",
				LearnerID: event.LearnerID,
				Data: map[string]interface{}{
					"alert_type": "boredom",
					"message":    "你表现非常棒！让我们挑战更难的内容！",
				},
			})
		}
	} else if eng.consecutiveCorrect >= 3 {
		e.bus.Publish(eventbus.Event{
			Type: eventbus.Encouragement, Source: "EngagementAgent",
			LearnerID: event.LearnerID,
			Data: map[string]interface{}{
				"message": fmt.Sprintf("连续%d题全对！继续保持！", eng.consecutiveCorrect),
			},
		})
	}
}

func (e *EngagementAgent) getEngagement(learnerID string) *learnerEngagement {
	e.mu.Lock()
	defer e.mu.Unlock()
	eng, ok := e.engagements[learnerID]
	if !ok {
		eng = &learnerEngagement{}
		e.engagements[learnerID] = eng
	}
	return eng
}
