package agent

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/multi-agent-education/golang/internal/eventbus"
	"github.com/multi-agent-education/golang/internal/llm"
	"github.com/multi-agent-education/golang/internal/model"
	"github.com/multi-agent-education/golang/internal/rag"
)

// ─── Assessment Agent ───

type AssessmentAgent struct {
	bus    *eventbus.EventBus
	Models map[string]*model.LearnerModel
	mu     sync.RWMutex
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

	// PublishChild：继承父事件的 CorrelationID，保证整条事件链可追踪
	a.bus.PublishChild(event, eventbus.Event{
		Type: eventbus.MasteryUpdated, Source: "AssessmentAgent",
		LearnerID: event.LearnerID,
		Data: map[string]interface{}{
			"knowledge_id": knowledgeID, "mastery": state.Mastery,
			"level": state.Level(), "is_correct": isCorrect, "attempts": state.Attempts,
		},
	})

	if state.Mastery < 0.3 && state.Attempts >= 3 {
		a.bus.PublishChild(event, eventbus.Event{
			Type: eventbus.WeaknessDetected, Source: "AssessmentAgent",
			LearnerID: event.LearnerID,
			Data:      map[string]interface{}{"knowledge_id": knowledgeID, "mastery": state.Mastery},
		})
	}

	a.bus.PublishChild(event, eventbus.Event{
		Type: eventbus.AssessmentComplete, Source: "AssessmentAgent",
		LearnerID: event.LearnerID,
		Data: map[string]interface{}{
			"knowledge_id": knowledgeID, "mastery": state.Mastery,
			"level": state.Level(), "is_correct": isCorrect,
		},
	})
}

// ─── Tutor Agent（教学 + RAG检索增强 + 可选LLM） ───

type TutorOption func(*TutorAgent)

// WithRetriever 注入RAG检索器：生成回复前检索教材片段并标注引用
func WithRetriever(r *rag.BM25) TutorOption {
	return func(t *TutorAgent) { t.retriever = r }
}

// WithLLM 注入LLM客户端：有Key时用LLM生成苏格拉底式回复，失败自动降级模板
func WithLLM(c *llm.Client) TutorOption {
	return func(t *TutorAgent) { t.llm = c }
}

type TutorAgent struct {
	bus       *eventbus.EventBus
	attempts  map[string]int
	retriever *rag.BM25
	llm       *llm.Client
	mu        sync.Mutex
}

func NewTutorAgent(bus *eventbus.EventBus, opts ...TutorOption) *TutorAgent {
	t := &TutorAgent{bus: bus, attempts: make(map[string]int)}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *TutorAgent) Start() {
	t.bus.Subscribe(eventbus.AssessmentComplete, t.handleAssessment)
	t.bus.Subscribe(eventbus.EngagementAlert, t.handleEngagement)
	t.bus.Subscribe(eventbus.StudentQuestion, t.handleQuestion)
}

// handleQuestion 响应学生自由提问（POST /api/v1/question 入口）
func (t *TutorAgent) handleQuestion(event eventbus.Event) {
	knowledgeID, _ := event.Data["knowledge_id"].(string)
	question, _ := event.Data["question"].(string)

	response := t.answerQuestion(knowledgeID, question)

	t.bus.PublishChild(event, eventbus.Event{
		Type: eventbus.TeachingResponse, Source: "TutorAgent",
		LearnerID: event.LearnerID,
		Data: map[string]interface{}{
			"knowledge_id": knowledgeID, "response": response,
			"teaching_style": "socratic",
		},
	})
}

func (t *TutorAgent) handleAssessment(event eventbus.Event) {
	knowledgeID, _ := event.Data["knowledge_id"].(string)
	level, _ := event.Data["level"].(string)
	isCorrect, _ := event.Data["is_correct"].(bool)
	mastery, _ := event.Data["mastery"].(float64)

	if !isCorrect {
		key := event.LearnerID + ":" + knowledgeID
		t.mu.Lock()
		t.attempts[key]++
		attempts := t.attempts[key]
		t.mu.Unlock()
		if attempts >= 2 {
			t.bus.PublishChild(event, eventbus.Event{
				Type: eventbus.HintNeeded, Source: "TutorAgent",
				LearnerID: event.LearnerID,
				Data: map[string]interface{}{
					"knowledge_id": knowledgeID, "attempts": attempts,
					"mastery": mastery,
				},
			})
			return
		}
	}

	response := t.buildResponse(knowledgeID, isCorrect, mastery)

	t.bus.PublishChild(event, eventbus.Event{
		Type: eventbus.TeachingResponse, Source: "TutorAgent",
		LearnerID: event.LearnerID,
		Data: map[string]interface{}{
			"knowledge_id": knowledgeID, "response": response,
			"teaching_style": "socratic", "difficulty_level": level,
		},
	})
}

// buildResponse 生成教学回复：
//  1. RAG：按知识点名称检索教材片段，注入引用（citation）
//  2. LLM：有 Key 时基于检索片段生成苏格拉底式回复（低温度，稳定）
//  3. 降级：LLM 不可用/失败时使用模板回复，保证服务可用
func (t *TutorAgent) buildResponse(knowledgeID string, isCorrect bool, mastery float64) string {
	citation := ""
	if t.retriever != nil {
		if name, ok := t.knowledgeName(knowledgeID); ok {
			hits := t.retriever.Search(name, 1)
			if len(hits) > 0 && hits[0].Score > 0 {
				citation = fmt.Sprintf("（📖 参考教材《%s》：%s…）",
					hits[0].Doc.Title, snippet(hits[0].Doc.Content, 55))
			}
		}
	}

	if t.llm != nil {
		resp, err := t.generateSocratic(knowledgeID, isCorrect, mastery, citation)
		if err == nil {
			return resp
		}
		log.Printf("[Tutor] LLM failed, fallback to template: %v", err)
	}

	if isCorrect {
		return fmt.Sprintf("很好！你在「%s」表现不错。你能用自己的话解释一下吗？%s", knowledgeID, citation)
	}
	return fmt.Sprintf("没关系，让我们分析「%s」。你觉得卡在了哪一步？%s", knowledgeID, citation)
}

// generateSocratic 基于教材片段生成苏格拉底式回复
func (t *TutorAgent) generateSocratic(knowledgeID string, isCorrect bool, mastery float64, citation string) (string, error) {
	name := knowledgeID
	if n, ok := t.knowledgeName(knowledgeID); ok {
		name = n
	}
	system := "你是苏格拉底式教学助教。规则：绝不直接给出答案，只能通过提问引导学生自己思考。" +
		"回复必须简短（不超过2句话），且以提问结尾。"
	user := fmt.Sprintf("学生正在学习「%s」，当前掌握度 %.0f%%。学生%s。%s请给出你的引导式回复。",
		name, mastery*100, map[bool]string{true: "刚刚答对了题目", false: "刚刚答错了题目"}[isCorrect], citation)

	// 教学场景用低温度保证稳定
	return t.llm.Chat([]llm.Message{
		{Role: "system", Content: system},
		{Role: "user", Content: user},
	}, 0.4)
}

func (t *TutorAgent) knowledgeName(knowledgeID string) (string, bool) {
	n, ok := model.GetKnowledge(knowledgeID)
	if !ok {
		return knowledgeID, false
	}
	return n.Name, true
}

// answerQuestion 生成对自由提问的苏格拉底式回复：
//  1. RAG：按知识点名称检索教材片段，注入引用（citation）
//  2. LLM：有 Key 时基于检索片段生成引导式回复（低温度，稳定）
//  3. 降级：LLM 不可用/失败时使用模板回复，保证服务可用
func (t *TutorAgent) answerQuestion(knowledgeID, question string) string {
	name := knowledgeID
	if n, ok := t.knowledgeName(knowledgeID); ok {
		name = n
	}

	citation := ""
	if t.retriever != nil {
		hits := t.retriever.Search(name, 1)
		if len(hits) > 0 && hits[0].Score > 0 {
			citation = fmt.Sprintf("（📖 参考教材《%s》：%s…）",
				hits[0].Doc.Title, snippet(hits[0].Doc.Content, 55))
		}
	}

	if t.llm != nil {
		system := "你是苏格拉底式教学助教。规则：绝不直接给出答案，只能通过提问引导学生自己思考。" +
			"回复必须简短（不超过2句话），且以提问结尾。"
		user := fmt.Sprintf("学生正在学习「%s」，提问：%s。%s请给出你的引导式回复。",
			name, question, citation)
		resp, err := t.llm.Chat([]llm.Message{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		}, 0.4)
		if err == nil {
			return resp
		}
		log.Printf("[Tutor] LLM failed, fallback to template: %v", err)
	}

	return fmt.Sprintf(
		"好的，关于「%s」，你的问题是：%s\n在我回答之前，让我先问你：\n你对这个知识点已经了解了哪些内容？试着说说你的理解，我们一起看看对不对。%s",
		name, question, citation)
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
		t.bus.PublishChild(event, eventbus.Event{
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
	c.bus.PublishChild(event, eventbus.Event{
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

	h.bus.PublishChild(event, eventbus.Event{
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
		e.bus.PublishChild(event, eventbus.Event{
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
			e.bus.PublishChild(event, eventbus.Event{
				Type: eventbus.EngagementAlert, Source: "EngagementAgent",
				LearnerID: event.LearnerID,
				Data: map[string]interface{}{
					"alert_type": "boredom",
					"message":    "你表现非常棒！让我们挑战更难的内容！",
				},
			})
		}
	} else if eng.consecutiveCorrect >= 3 {
		e.bus.PublishChild(event, eventbus.Event{
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

// snippet 截取文档片段（按rune，避免截断中文）
func snippet(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n])
}

// ContainsGuidedQuestion 判断回复是否为引导式提问（供评测与前端使用）：
// 以问号结尾或包含引导性措辞，且不含直接给答案的表述
func ContainsGuidedQuestion(response string) bool {
	// 直接给答案的标记（否决项）：教材公式中的 "=" 不算，需带上下文
	directMarkers := []string{"答案是", "答案为", "结果等于", "结果为", "顶点是", "的解是", "正确答案是"}
	for _, m := range directMarkers {
		if strings.Contains(response, m) {
			return false
		}
	}
	guideMarkers := []string{"？", "?", "你觉得", "想一想", "试着", "卡在", "回忆", "解释一下", "能不能"}
	for _, m := range guideMarkers {
		if strings.Contains(response, m) {
			return true
		}
	}
	return false
}
