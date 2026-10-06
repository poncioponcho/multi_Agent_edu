package main

import (
	"io"
	"log"
	"net/http"
	"os"

	"github.com/multi-agent-education/golang/internal/agent"
	"github.com/multi-agent-education/golang/internal/api"
	"github.com/multi-agent-education/golang/internal/eventbus"
	"github.com/multi-agent-education/golang/internal/llm"
	"github.com/multi-agent-education/golang/internal/rag"
)

// 多Agent智能教育系统 - Go版入口
//
// Go版使用 goroutine + channel 实现事件驱动，
// 天然适合高并发Agent并行处理。
func main() {
	// EDU_QUIET=1 关闭日志输出。
	//
	// EventBus 在 Publish 与 dispatch 各打一条 log.Printf，属于同步写 stderr，
	// 是单请求路径上最重的开销之一。压测时必须静音，否则测到的是日志 I/O
	// 而不是总线本身（见 docs/压测报告.md）。
	if os.Getenv("EDU_QUIET") == "1" {
		log.SetOutput(io.Discard)
	}

	bus := eventbus.New()

	// 初始化RAG检索器与LLM客户端（无API Key时LLM为nil，Tutor自动降级模板回复）
	retriever := rag.NewRetriever()
	llmClient := llm.NewClientFromEnv()
	if llmClient == nil {
		log.Println("[LLM] no API key, Tutor will use template responses")
	} else {
		log.Println("[LLM] API key detected, Tutor will use LLM + RAG")
	}

	// 初始化5个Agent，每个Agent在独立的goroutine中运行
	assessmentAgent := agent.NewAssessmentAgent(bus)
	tutorAgent := agent.NewTutorAgent(bus, agent.WithRetriever(retriever), agent.WithLLM(llmClient))
	curriculumAgent := agent.NewCurriculumAgent(bus)
	hintAgent := agent.NewHintAgent(bus)
	engagementAgent := agent.NewEngagementAgent(bus)

	// 注册所有Agent的事件订阅（Start仅Subscribe，纯内存操作，同步执行避免订阅竞态）
	assessmentAgent.Start()
	tutorAgent.Start()
	curriculumAgent.Start()
	hintAgent.Start()
	engagementAgent.Start()

	// 启动HTTP服务
	router := api.SetupRouter(bus, assessmentAgent, retriever)
	log.Println("Go Agent Education Server starting on :8081")
	if err := http.ListenAndServe(":8081", router); err != nil {
		log.Fatal(err)
	}
}
