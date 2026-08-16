// Package eval 实现 Agent 评测框架（Eval）：
// 黄金用例集 + 指标统计，全链路（事件总线）驱动，输出结构化评测报告。
//
// 设计要点（面试向）：
//   - 用例即断言：每个用例声明期望行为（引导式回复/提示级别/干预触发），
//     Prompt 或策略迭代后重跑即可发现回归
//   - 多轮序列：Hint 升级、挫败干预等跨轮行为用 Sequence 模拟连续作答，
//     观测结果按轮次归档（Rounds），断言精确到轮
//   - 指标可辩护：引导率/提示准确率/平均对话轮数均为实测值
package eval

import (
	"fmt"
	"time"

	"github.com/multi-agent-education/golang/internal/agent"
	"github.com/multi-agent-education/golang/internal/eventbus"
	"github.com/multi-agent-education/golang/internal/rag"
)

// Case 黄金评测用例
type Case struct {
	Name        string // 用例名（可读）
	LearnerID   string
	KnowledgeID string
	// 作答序列：非空时按序列逐轮作答（true=对 false=错）；为空时使用 [IsCorrect]
	Sequence []bool
	// IsCorrect 单轮用例的作答结果（Sequence为空时生效）
	IsCorrect bool
	// ExpectHintLvAt 轮次(1-based) -> 期望提示级别（用于多轮Hint升级断言）
	ExpectHintLvAt map[int]int
	// ExpectAlertAt 干预类型 -> 期望触发轮次
	ExpectAlertAt map[string]int
	// ExpectEncourageAt 期望触发鼓励的轮次（>0 时校验）
	ExpectEncourageAt int
}

// 用例集：覆盖 5 个 Agent 的核心行为、跨轮策略与知识图谱覆盖
var Cases = []Case{
	// ── Tutor：答对 → 引导式追问 ──
	{Name: "tutor_first_correct", LearnerID: "l1", KnowledgeID: "linear_eq_1", IsCorrect: true},
	{Name: "tutor_quadratic_correct", LearnerID: "l1", KnowledgeID: "quadratic_eq", IsCorrect: true},
	{Name: "tutor_pythagorean_correct", LearnerID: "l1", KnowledgeID: "pythagorean", IsCorrect: true},

	// ── Tutor：首次答错 → 引导分析（不触发Hint） ──
	{Name: "tutor_first_wrong_no_hint", LearnerID: "l2", KnowledgeID: "factoring", IsCorrect: false},

	// ── Hint：连续答错，提示级别随尝试次数升级（第2次错才触发提示） ──
	{Name: "hint_progression", LearnerID: "l3", KnowledgeID: "linear_eq_1",
		Sequence: []bool{false, false, false, false, false},
		ExpectHintLvAt: map[int]int{
			2: 1, // 第2次错 → 首次提示 level1（暗示）
			3: 2, // 第3次错 → 升级 level2（引导）
			4: 2, // 第4次错 → 保持 level2
			5: 3, // 第5次错 → 直接讲解 level3
		}},

	// ── Engagement：连续3错 → 挫败干预，Tutor降低难度 ──
	{Name: "engagement_frustration", LearnerID: "l4", KnowledgeID: "quadratic_func",
		Sequence:      []bool{false, false, false},
		ExpectAlertAt: map[string]int{"frustration": 3}},

	// ── Engagement：连续5对且准确率>90% → 挑战更高难度 ──
	{Name: "engagement_boredom", LearnerID: "l4b", KnowledgeID: "linear_eq_1",
		Sequence:      []bool{true, true, true, true, true, true},
		ExpectAlertAt: map[string]int{"boredom": 5}},

	// ── Engagement：连续3对 → 鼓励 ──
	{Name: "engagement_encouragement", LearnerID: "l4c", KnowledgeID: "pythagorean",
		Sequence:          []bool{true, true, true},
		ExpectEncourageAt: 3},

	// ── 各知识点答题（验证知识图谱覆盖与引导回复） ──
	{Name: "kp_negative_numbers", LearnerID: "l5", KnowledgeID: "negative_numbers", IsCorrect: true},
	{Name: "kp_linear_func", LearnerID: "l5", KnowledgeID: "linear_func", IsCorrect: false},
	{Name: "kp_similar_triangle", LearnerID: "l5", KnowledgeID: "similar_triangle", IsCorrect: true},
	{Name: "kp_trig_basic", LearnerID: "l5", KnowledgeID: "trig_basic", IsCorrect: false},
	{Name: "kp_probability", LearnerID: "l5", KnowledgeID: "probability", IsCorrect: true},
	{Name: "kp_statistics", LearnerID: "l5", KnowledgeID: "statistics", IsCorrect: false},
	{Name: "kp_inequality", LearnerID: "l5", KnowledgeID: "inequality", IsCorrect: true},
	{Name: "kp_sequence", LearnerID: "l5", KnowledgeID: "sequence", IsCorrect: false},
	{Name: "kp_coordinate", LearnerID: "l5", KnowledgeID: "coordinate", IsCorrect: true},
	{Name: "kp_logic", LearnerID: "l5", KnowledgeID: "logic", IsCorrect: false},
}

// DialogCase 完整对话场景：用于统计"到达掌握所需的对话轮数"
type DialogCase struct {
	Name        string
	LearnerID   string
	KnowledgeID string
	Answers     []bool // 依次作答，直到 mastery>=0.6
}

var DialogCases = []DialogCase{
	{Name: "dialog_linear_eq_mastery", LearnerID: "d1", KnowledgeID: "linear_eq_1",
		Answers: []bool{false, true, true, true, true}},
	{Name: "dialog_factoring_mastery", LearnerID: "d2", KnowledgeID: "factoring",
		Answers: []bool{false, false, true, true, true, true}},
	{Name: "dialog_pythagorean_mastery", LearnerID: "d3", KnowledgeID: "pythagorean",
		Answers: []bool{true, true, true, false, true, true}},
}

// RoundObs 单轮观测结果
type RoundObs struct {
	Response   string `json:"response,omitempty"`
	HintLevel  int    `json:"hint_level,omitempty"` // 0=未触发
	AlertType  string `json:"alert_type,omitempty"`
	Encouraged bool   `json:"encouraged,omitempty"`
}

// Report 评测报告
type Report struct {
	TotalCases      int            `json:"total_cases"`
	GuidedResponses int            `json:"guided_responses"` // 引导式回复数
	GuidanceRate    float64        `json:"guidance_rate"`    // 引导率 %
	HintChecks      int            `json:"hint_checks"`
	HintCorrect     int            `json:"hint_correct"`
	HintAccuracy    float64        `json:"hint_accuracy"` // 提示级别准确率 %
	AlertChecks     int            `json:"alert_checks"`
	AlertCorrect    int            `json:"alert_correct"`
	AlertAccuracy   float64        `json:"alert_accuracy"` // 干预准确率 %
	Dialogs         int            `json:"dialogs"`
	AvgTurns        float64        `json:"avg_turns"` // 平均对话轮数（到达掌握）
	Passed          int            `json:"passed"`
	Failed          int            `json:"failed"`
	Details         []CaseDetail   `json:"details"`
	DialogDetails   []DialogDetail `json:"dialog_details"`
}

// CaseDetail 单个用例明细
type CaseDetail struct {
	Name   string    `json:"name"`
	Passed bool      `json:"passed"`
	Rounds []RoundObs `json:"rounds"`
	Reason string    `json:"reason,omitempty"`
}

// DialogDetail 对话场景明细
type DialogDetail struct {
	Name    string  `json:"name"`
	Turns   int     `json:"turns"`
	Mastery float64 `json:"mastery"`
}

const collectTimeout = 500 * time.Millisecond

// Run 运行全部评测并汇总指标
func Run() *Report {
	rep := &Report{}

	for _, c := range Cases {
		d := runCase(c)
		rep.Details = append(rep.Details, d)
		rep.TotalCases++

		if d.Passed {
			rep.Passed++
		} else {
			rep.Failed++
		}
		for _, r := range d.Rounds {
			if r.Response != "" && agent.ContainsGuidedQuestion(r.Response) {
				rep.GuidedResponses++
				break
			}
		}
		// 提示级别断言统计
		for wantRound, wantLv := range c.ExpectHintLvAt {
			rep.HintChecks++
			if roundHintLevel(d, wantRound) == wantLv {
				rep.HintCorrect++
			}
		}
		// 干预断言统计
		for alertType, wantRound := range c.ExpectAlertAt {
			rep.AlertChecks++
			if roundAlert(d, alertType) == wantRound {
				rep.AlertCorrect++
			}
		}
	}

	totalTurns := 0
	for _, dc := range DialogCases {
		dd := runDialog(dc)
		rep.DialogDetails = append(rep.DialogDetails, dd)
		rep.Dialogs++
		totalTurns += dd.Turns
	}
	if rep.Dialogs > 0 {
		rep.AvgTurns = round2(float64(totalTurns) / float64(rep.Dialogs))
	}

	rep.GuidanceRate = pct(rep.GuidedResponses, rep.TotalCases)
	rep.HintAccuracy = pct(rep.HintCorrect, rep.HintChecks)
	rep.AlertAccuracy = pct(rep.AlertCorrect, rep.AlertChecks)
	return rep
}

// runCase 运行单个用例：构造独立总线与Agent，逐轮发布作答，观测按轮归档
func runCase(c Case) CaseDetail {
	bus := eventbus.New()
	tutor := agent.NewTutorAgent(bus, agent.WithRetriever(rag.NewRetriever()))
	hint := agent.NewHintAgent(bus)
	engagement := agent.NewEngagementAgent(bus)
	assessment := agent.NewAssessmentAgent(bus)

	// 同步注册订阅：Start 仅做 Subscribe（纯内存操作，不阻塞）。
	// 注意：不能用 go 异步启动，否则存在竞态——事件发布时 Agent 可能尚未订阅完成
	assessment.Start()
	tutor.Start()
	hint.Start()
	engagement.Start()

	obsCh := make(chan observed, 32)
	bus.Subscribe(eventbus.TeachingResponse, func(e eventbus.Event) {
		r, _ := e.Data["response"].(string)
		obsCh <- observed{kind: "response", response: r}
	})
	bus.Subscribe(eventbus.HintResponse, func(e eventbus.Event) {
		lv := 0
		switch v := e.Data["hint_level"].(type) {
		case float64:
			lv = int(v)
		case int:
			lv = v
		}
		obsCh <- observed{kind: "hint", hintLevel: lv}
	})
	bus.Subscribe(eventbus.EngagementAlert, func(e eventbus.Event) {
		at, _ := e.Data["alert_type"].(string)
		obsCh <- observed{kind: "alert", alertType: at}
	})
	bus.Subscribe(eventbus.Encouragement, func(e eventbus.Event) {
		obsCh <- observed{kind: "encourage"}
	})

	seq := c.Sequence
	if len(seq) == 0 {
		seq = []bool{c.IsCorrect}
	}

	d := CaseDetail{Name: c.Name}
	for _, correct := range seq {
		drain(obsCh) // 清空上一轮残留

		bus.Publish(eventbus.Event{
			Type: eventbus.StudentSubmission, Source: "eval",
			LearnerID: c.LearnerID,
			Data: map[string]interface{}{
				"knowledge_id": c.KnowledgeID, "is_correct": correct,
			},
		})

		ro := RoundObs{}
		timeout := time.After(collectTimeout)
	collecting:
		for {
			select {
			case obs := <-obsCh:
				switch obs.kind {
				case "response":
					ro.Response = obs.response
				case "hint":
					ro.HintLevel = obs.hintLevel
				case "alert":
					ro.AlertType = obs.alertType
				case "encourage":
					ro.Encouraged = true
				}
			case <-timeout:
				break collecting
			}
		}
		d.Rounds = append(d.Rounds, ro)
	}

	d.Passed, d.Reason = checkCase(c, d)
	return d
}

// observed 事件观测
type observed struct {
	kind      string // "response" | "hint" | "alert" | "encourage"
	response  string
	hintLevel int
	alertType string
}

// checkCase 规则判定：校验期望行为是否满足
func checkCase(c Case, d CaseDetail) (bool, string) {
	var failures []string

	// 引导式回复校验：用例必须有至少一轮收到引导式教学回复
	hasGuided := false
	for _, r := range d.Rounds {
		if r.Response != "" && agent.ContainsGuidedQuestion(r.Response) {
			hasGuided = true
			break
		}
	}
	if !hasGuided {
		if len(d.Rounds) == 0 || d.Rounds[0].Response == "" {
			failures = append(failures, "no teaching response")
		} else {
			failures = append(failures, "no guided response: "+truncate(d.Rounds[0].Response, 50))
		}
	}

	// 提示级别按轮次校验
	for wantRound, wantLv := range c.ExpectHintLvAt {
		gotLv := roundHintLevel(d, wantRound)
		if gotLv != wantLv {
			failures = append(failures, fmt.Sprintf("round %d hint level=%d want=%d", wantRound, gotLv, wantLv))
		}
	}

	// 干预按类型+轮次校验
	for alertType, wantRound := range c.ExpectAlertAt {
		gotRound := roundAlert(d, alertType)
		if gotRound != wantRound {
			failures = append(failures, fmt.Sprintf("alert %q at round=%d want=%d", alertType, gotRound, wantRound))
		}
	}

	if c.ExpectEncourageAt > 0 {
		found := false
		for idx, r := range d.Rounds {
			if r.Encouraged && idx+1 == c.ExpectEncourageAt {
				found = true
				break
			}
		}
		if !found {
			failures = append(failures, fmt.Sprintf("encouragement expected at round %d", c.ExpectEncourageAt))
		}
	}

	if len(failures) == 0 {
		return true, ""
	}
	return false, joinReasons(failures)
}

// roundHintLevel 第round轮（1-based）触发的提示级别
func roundHintLevel(d CaseDetail, round int) int {
	if round >= 1 && round <= len(d.Rounds) {
		return d.Rounds[round-1].HintLevel
	}
	return 0
}

// roundAlert 某干预类型触发的轮次（未触发返回0）
func roundAlert(d CaseDetail, alertType string) int {
	for i, r := range d.Rounds {
		if r.AlertType == alertType {
			return i + 1
		}
	}
	return 0
}

// drain 清空channel中残留消息
func drain(ch chan observed) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// runDialog 完整对话：依次作答直到 mastery>=0.6，统计轮数
func runDialog(dc DialogCase) DialogDetail {
	bus := eventbus.New()
	assessment := agent.NewAssessmentAgent(bus)
	_ = agent.NewTutorAgent(bus, agent.WithRetriever(rag.NewRetriever()))
	_ = agent.NewHintAgent(bus)
	_ = agent.NewEngagementAgent(bus)

	// 同步注册订阅（Start 仅 Subscribe，不阻塞；避免订阅未完成事件即被发布）
	assessment.Start()

	masteryCh := make(chan float64, 1)
	bus.Subscribe(eventbus.MasteryUpdated, func(e eventbus.Event) {
		if e.LearnerID != dc.LearnerID {
			return
		}
		if m, ok := e.Data["mastery"].(float64); ok {
			select {
			case masteryCh <- m:
			default:
			}
		}
	})

	turns := 0
	mastery := 0.0
	for _, correct := range dc.Answers {
		turns++
		bus.Publish(eventbus.Event{
			Type: eventbus.StudentSubmission, Source: "eval",
			LearnerID: dc.LearnerID,
			Data: map[string]interface{}{
				"knowledge_id": dc.KnowledgeID, "is_correct": correct,
			},
		})
		select {
		case m := <-masteryCh:
			mastery = m
		case <-time.After(collectTimeout):
		}
		if mastery >= 0.6 {
			break
		}
	}
	return DialogDetail{Name: dc.Name, Turns: turns, Mastery: round2(mastery)}
}

func pct(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return round2(float64(a) / float64(b) * 100)
}

func round2(v float64) float64 {
	return float64(int(v*100+0.5)) / 100
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}

func joinReasons(fs []string) string {
	out := ""
	for i, f := range fs {
		if i > 0 {
			out += "; "
		}
		out += f
	}
	return out
}
