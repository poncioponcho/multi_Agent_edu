package eval

import (
	"testing"

	"github.com/multi-agent-education/golang/internal/agent"
)

// TestRunAllCases 全量评测：所有用例必须通过，且指标为合理值
func TestRunAllCases(t *testing.T) {
	rep := Run()

	if rep.Failed != 0 {
		for _, d := range rep.Details {
			if !d.Passed {
				t.Logf("failed: %s -> %s", d.Name, d.Reason)
			}
		}
		t.Fatalf("expected all cases passed, got %d failed / %d total", rep.Failed, rep.TotalCases)
	}
	if rep.GuidanceRate <= 0 || rep.GuidanceRate > 100 {
		t.Fatalf("guidance rate out of range: %.1f%%", rep.GuidanceRate)
	}
	if rep.HintAccuracy != 100 {
		t.Fatalf("hint accuracy should be 100%%, got %.1f%%", rep.HintAccuracy)
	}
	if rep.AlertAccuracy != 100 {
		t.Fatalf("alert accuracy should be 100%%, got %.1f%%", rep.AlertAccuracy)
	}
	if rep.AvgTurns <= 0 {
		t.Fatalf("avg turns should be > 0, got %.1f", rep.AvgTurns)
	}
	t.Logf("guidance=%.1f%% hint=%.1f%% alert=%.1f%% avgTurns=%.1f",
		rep.GuidanceRate, rep.HintAccuracy, rep.AlertAccuracy, rep.AvgTurns)
}

// TestGuidanceClassifier 引导式判定器：识别直接给答案 vs 引导提问
func TestGuidanceClassifier(t *testing.T) {
	guided := []string{
		"你觉得卡在了哪一步？",
		"你能用自己的话解释一下吗？",
		"想一想，题目里有哪些关键信息？",
	}
	for _, g := range guided {
		if !agent.ContainsGuidedQuestion(g) {
			t.Errorf("should be guided: %q", g)
		}
	}
	direct := []string{
		"答案是 4",
		"这个方程的解是 x = 2",
		"顶点是 (-1, 0)",
		"结果等于 12",
	}
	for _, d := range direct {
		if agent.ContainsGuidedQuestion(d) {
			t.Errorf("should NOT be guided: %q", d)
		}
	}
}
