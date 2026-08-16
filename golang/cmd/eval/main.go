// Eval CLI：运行 Agent 评测并输出结构化报告
//
// 用法：
//   go run cmd/eval/main.go            # 输出完整评测报告（JSON + 摘要）
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/multi-agent-education/golang/internal/eval"
)

func main() {
	rep := eval.Run()

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		fmt.Fprintln(os.Stderr, "encode report:", err)
		os.Exit(1)
	}

	fmt.Println("\n=== 评测摘要 ===")
	fmt.Printf("用例通过率   : %d/%d (%.1f%%)\n", rep.Passed, rep.TotalCases, pct(rep.Passed, rep.TotalCases))
	fmt.Printf("引导率       : %.1f%% (%d/%d 回复为引导式提问)\n",
		rep.GuidanceRate, rep.GuidedResponses, rep.TotalCases)
	fmt.Printf("提示级别准确率: %.1f%% (%d/%d)\n", rep.HintAccuracy, rep.HintCorrect, rep.HintChecks)
	fmt.Printf("干预准确率   : %.1f%% (%d/%d)\n", rep.AlertAccuracy, rep.AlertCorrect, rep.AlertChecks)
	fmt.Printf("平均对话轮数 : %.1f 轮到达掌握（%d 个完整对话场景）\n", rep.AvgTurns, rep.Dialogs)

	if rep.Failed > 0 {
		fmt.Println("\n=== 失败用例 ===")
		for _, d := range rep.Details {
			if !d.Passed {
				fmt.Printf("  ✗ %-32s %s\n", d.Name, d.Reason)
			}
		}
		os.Exit(1)
	}
	fmt.Println("\n全部用例通过 ✅")
}

func pct(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b) * 100
}
