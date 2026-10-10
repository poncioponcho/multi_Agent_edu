package api

import (
	"bufio"
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/multi-agent-education/golang/internal/agent"
	"github.com/multi-agent-education/golang/internal/eventbus"
)

// TestMain 丢弃日志输出：EventBus 每次 Publish/dispatch 都会 log.Printf，
// 会淹没测试输出（与 internal/eventbus 包的 TestMain 同一考虑）。
func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	m.Run()
}

// newTestServer 起一个挂真实路由的测试服务。
func newTestServer(t *testing.T) (*httptest.Server, *eventbus.EventBus) {
	t.Helper()
	bus := eventbus.New()
	assessment := agent.NewAssessmentAgent(bus)
	assessment.Start()
	srv := httptest.NewServer(SetupRouter(bus, assessment, nil))
	t.Cleanup(srv.Close)
	return srv, bus
}

// waitForSubscriber 等 SSE handler 把订阅建立起来。
// 用轮询而不是固定 sleep，避免不同机器上的时序 flake。
func waitForSubscriber(t *testing.T, bus *eventbus.EventBus) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if bus.SubscriberCount() > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("SSE 订阅未建立（SubscriberCount 一直为 0）")
}

// TestSSEStreamReceivesLearnerEvents SSE 应把该 learner 的事件实时推送，
// 且**不推送其他 learner 的事件**（这是 SSE 最容易写错的地方）。
func TestSSEStreamReceivesLearnerEvents(t *testing.T) {
	srv, bus := newTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/stream/L1", nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("连接 SSE 失败: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type 应为 text/event-stream, got %q", ct)
	}

	waitForSubscriber(t, bus)

	// 发两条：一条属于 L1，一条属于 OTHER
	bus.Publish(eventbus.Event{
		Type: eventbus.TeachingResponse, Source: "tutor", LearnerID: "L1",
		Data: map[string]interface{}{"response": "socratic-question"},
	})
	bus.Publish(eventbus.Event{
		Type: eventbus.TeachingResponse, Source: "tutor", LearnerID: "OTHER",
		Data: map[string]interface{}{"response": "should-not-arrive"},
	})

	// 读第一个 data: 帧，必须只属于 L1
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

	var got string
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") {
			got = line
			break
		}
	}
	if got == "" {
		t.Fatal("未收到任何 SSE data 帧")
	}
	if !strings.Contains(got, "socratic-question") {
		t.Fatalf("收到的不是 L1 的事件: %s", got)
	}
	if strings.Contains(got, "should-not-arrive") {
		t.Fatalf("★ 不应收到其他 learner 的事件: %s", got)
	}
}

// TestSSEStreamReceivesTerminalEvents 终端事件（无 Agent 订阅）也能通过 SSE 送出——
// 这正是加 SSE 的目的：给 teaching_response / hint.response / encouragement
// 一条真正的推送通道，替代前端轮询内存 history。
func TestSSEStreamReceivesTerminalEvents(t *testing.T) {
	srv, bus := newTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/stream/L2", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("连接 SSE 失败: %v", err)
	}
	defer resp.Body.Close()

	waitForSubscriber(t, bus)

	// 三类终端事件，生产路径上都没有 Agent 订阅者
	for _, et := range []eventbus.EventType{
		eventbus.TeachingResponse, eventbus.HintResponse, eventbus.Encouragement,
	} {
		bus.Publish(eventbus.Event{Type: et, Source: "agent", LearnerID: "L2"})
	}

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)

	seen := map[string]bool{}
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "event: ") {
			seen[strings.TrimPrefix(line, "event: ")] = true
		}
		if len(seen) == 3 {
			break
		}
	}

	for _, want := range []string{"tutor.teaching_response", "hint.response", "engagement.encouragement"} {
		if !seen[want] {
			t.Fatalf("SSE 未送出终端事件 %s（已收到：%v）", want, seen)
		}
	}
}
