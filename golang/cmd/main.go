package main

import (
	"log"
	"net/http"

	"github.com/multi-agent-education/golang/internal/agent"
	"github.com/multi-agent-education/golang/internal/api"
	"github.com/multi-agent-education/golang/internal/eventbus"
)

// 多Agent智能教育系统 - Go版入口
//
// Go版使用 goroutine + channel 实现事件驱动，
// 天然适合高并发Agent并行处理。
func main() {
	bus := eventbus.New()

	// 初始化5个Agent，每个Agent在独立的goroutine中运行
	assessmentAgent := agent.NewAssessmentAgent(bus)
	tutorAgent := agent.NewTutorAgent(bus)
	curriculumAgent := agent.NewCurriculumAgent(bus)
	hintAgent := agent.NewHintAgent(bus)
	engagementAgent := agent.NewEngagementAgent(bus)

	// 启动所有Agent的事件监听
	go assessmentAgent.Start()
	go tutorAgent.Start()
	go curriculumAgent.Start()
	go hintAgent.Start()
	go engagementAgent.Start()

	// 启动HTTP服务
	router := api.SetupRouter(bus, assessmentAgent)
	log.Println("Go Agent Education Server starting on :8081")
	if err := http.ListenAndServe(":8081", router); err != nil {
		log.Fatal(err)
	}
}
