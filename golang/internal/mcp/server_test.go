package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/multi-agent-education/golang/internal/model"
	"github.com/multi-agent-education/golang/internal/rag"
)

func newTestServer() *Server {
	return NewServer(model.KnowledgeGraph, rag.NewRetriever())
}

// call 发送JSON-RPC请求并解析响应
func call(t *testing.T, s *Server, method string, id int, params interface{}) map[string]interface{} {
	t.Helper()
	req := map[string]interface{}{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		req["params"] = params
	}
	raw, _ := json.Marshal(req)

	resp := s.HandleJSON(raw)
	if resp == nil {
		t.Fatalf("method %s: no response", method)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(resp, &out); err != nil {
		t.Fatalf("method %s: bad response json: %v", method, err)
	}
	return out
}

func TestInitialize(t *testing.T) {
	s := newTestServer()
	out := call(t, s, "initialize", 1, map[string]interface{}{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]interface{}{},
		"clientInfo":      map[string]interface{}{"name": "test", "version": "0.0.1"},
	})
	result, ok := out["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("initialize: no result, got %v", out)
	}
	if result["protocolVersion"] != "2024-11-05" {
		t.Fatalf("unexpected protocolVersion: %v", result["protocolVersion"])
	}
}

func TestToolsList(t *testing.T) {
	s := newTestServer()
	out := call(t, s, "tools/list", 2, nil)
	result := out["result"].(map[string]interface{})
	tools := result["tools"].([]interface{})
	if len(tools) != 3 {
		t.Fatalf("expected 3 tools, got %d", len(tools))
	}
	names := map[string]bool{}
	for _, tl := range tools {
		names[tl.(map[string]interface{})["name"].(string)] = true
	}
	for _, want := range []string{"query_knowledge_graph", "calculate", "retrieve_textbook"} {
		if !names[want] {
			t.Fatalf("missing tool: %s (have %v)", want, names)
		}
	}
}

func TestCallQueryKnowledgeGraph(t *testing.T) {
	s := newTestServer()
	out := call(t, s, "tools/call", 3, map[string]interface{}{
		"name":      "query_knowledge_graph",
		"arguments": map[string]interface{}{"id": "quadratic_eq"},
	})
	result := out["result"].(map[string]interface{})
	text := result["content"].([]interface{})[0].(map[string]interface{})["text"].(string)
	if !strings.Contains(text, "一元二次方程") {
		t.Fatalf("knowledge graph result missing name: %s", text)
	}
	if !strings.Contains(text, "因式分解") {
		t.Fatalf("knowledge graph result missing prerequisite: %s", text)
	}
}

func TestCallCalculate(t *testing.T) {
	s := newTestServer()
	cases := []struct {
		expr string
		want string
	}{
		{"(2+3)*4", "20"},
		{"sqrt(16)", "4"},
		{"2^10", "1024"},
		{"abs(-5)+min(3,7)", "8"},
	}
	for _, c := range cases {
		out := call(t, s, "tools/call", 4, map[string]interface{}{
			"name":      "calculate",
			"arguments": map[string]interface{}{"expression": c.expr},
		})
		result := out["result"].(map[string]interface{})
		text := result["content"].([]interface{})[0].(map[string]interface{})["text"].(string)
		if !strings.Contains(text, c.want) {
			t.Fatalf("calc %q: want %s in %s", c.expr, c.want, text)
		}
	}
}

func TestCallCalculateInjectionSafety(t *testing.T) {
	s := newTestServer()
	// 危险输入不应 panic，且不应执行任意代码
	dangerous := []string{
		"1; rm -rf /",
		"$(whoami)",
		"__import__('os').system('id')",
		"1 + )(",
		"",
		"1/0",
	}
	for _, expr := range dangerous {
		out := call(t, s, "tools/call", 5, map[string]interface{}{
			"name":      "calculate",
			"arguments": map[string]interface{}{"expression": expr},
		})
		if _, hasErr := out["error"]; !hasErr {
			t.Fatalf("dangerous expr %q should return error, got %v", expr, out["result"])
		}
	}
}

func TestCallRetrieveTextbook(t *testing.T) {
	s := newTestServer()
	out := call(t, s, "tools/call", 6, map[string]interface{}{
		"name":      "retrieve_textbook",
		"arguments": map[string]interface{}{"knowledge_id": "pythagorean", "top_k": 1},
	})
	result := out["result"].(map[string]interface{})
	text := result["content"].([]interface{})[0].(map[string]interface{})["text"].(string)
	if !strings.Contains(text, "勾股定理") {
		t.Fatalf("retrieve result missing title: %s", text)
	}
}

func TestUnknownMethod(t *testing.T) {
	s := newTestServer()
	out := call(t, s, "unknown/method", 7, nil)
	if _, hasErr := out["error"]; !hasErr {
		t.Fatalf("unknown method should return error")
	}
}
