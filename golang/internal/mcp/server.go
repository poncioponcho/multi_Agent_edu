// Package mcp 实现 MCP (Model Context Protocol) Server 的核心：
// stdio 传输 + JSON-RPC 2.0 消息处理，纯标准库实现，零第三方依赖。
//
// 支持的方法（MCP 规范子集）：
//   - initialize：握手
//   - notifications/initialized：初始化通知（忽略）
//   - tools/list：列出可用工具
//   - tools/call：调用工具
//
// 内置工具：
//   - query_knowledge_graph：查询知识图谱节点（前置依赖/难度）
//   - calculate：安全数学表达式求值（自研递归下降解析器，不依赖 eval）
//   - retrieve_textbook：RAG 教材检索（与 Tutor Agent 共用检索器）
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/multi-agent-education/golang/internal/model"
	"github.com/multi-agent-education/golang/internal/rag"
)

// Tool MCP工具定义
type Tool struct {
	Name        string                 `json:"-"`
	Description string                 `json:"-"`
	InputSchema map[string]interface{} `json:"-"`
	Handler     func(map[string]interface{}) (interface{}, error)
}

// toolInfo 对外暴露的工具描述（剥离Handler，避免序列化失败）
type toolInfo struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}

// Server MCP服务器
type Server struct {
	serverInfo map[string]interface{}
	tools      map[string]Tool
}

// NewServer 创建MCP服务器，注入知识图谱与RAG检索器
func NewServer(kg []model.KnowledgeNode, retriever *rag.BM25) *Server {
	s := &Server{
		serverInfo: map[string]interface{}{
			"name":    "multi-agent-edu-go",
			"version": "1.0.0",
		},
		tools: make(map[string]Tool),
	}

	s.registerTool(Tool{
		Name:        "query_knowledge_graph",
		Description: "查询知识图谱中的知识点：返回名称、难度、前置知识点",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"id": map[string]interface{}{"type": "string", "description": "知识点ID，如 quadratic_eq"},
			},
			"required": []string{"id"},
		},
		Handler: func(params map[string]interface{}) (interface{}, error) {
			id, _ := params["id"].(string)
			node, ok := model.GetKnowledge(id)
			if !ok {
				return nil, fmt.Errorf("knowledge point not found: %s", id)
			}
			var prereqs []string
			for _, p := range model.GetPrerequisites(id) {
				prereqs = append(prereqs, p.Name)
			}
			return map[string]interface{}{
				"id": node.ID, "name": node.Name,
				"difficulty": node.Difficulty, "prerequisites": prereqs,
			}, nil
		},
	})

	s.registerTool(Tool{
		Name:        "calculate",
		Description: "安全计算数学表达式，支持 + - * / ^ 括号、sqrt abs 函数。例: (2+3)*4、sqrt(16)、2^10",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"expression": map[string]interface{}{"type": "string", "description": "数学表达式"},
			},
			"required": []string{"expression"},
		},
		Handler: func(params map[string]interface{}) (interface{}, error) {
			expr, _ := params["expression"].(string)
			if expr == "" {
				return nil, fmt.Errorf("expression is required")
			}
			val, err := evalExpr(expr)
			if err != nil {
				return nil, err
			}
			return map[string]interface{}{"expression": expr, "result": val}, nil
		},
	})

	s.registerTool(Tool{
		Name:        "retrieve_textbook",
		Description: "检索教材内容：按知识点名称返回最相关的教材片段（RAG检索）",
		InputSchema: map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"knowledge_id": map[string]interface{}{"type": "string", "description": "知识点ID"},
				"top_k":        map[string]interface{}{"type": "integer", "description": "返回条数，默认1"},
			},
			"required": []string{"knowledge_id"},
		},
		Handler: func(params map[string]interface{}) (interface{}, error) {
			if retriever == nil {
				return nil, fmt.Errorf("retriever not configured")
			}
			id, _ := params["knowledge_id"].(string)
			topK := 1
			if v, ok := params["top_k"].(float64); ok && int(v) > 0 {
				topK = int(v)
			}
			query := id
			if node, ok := model.GetKnowledge(id); ok {
				query = node.Name
			}
			hits := retriever.Search(query, topK)
			if len(hits) == 0 {
				return map[string]interface{}{"hits": []interface{}{}}, nil
			}
			out := make([]map[string]interface{}, 0, len(hits))
			for _, h := range hits {
				out = append(out, map[string]interface{}{
					"knowledge_id": h.Doc.ID, "title": h.Doc.Title,
					"score":   round(h.Score, 4),
					"excerpt": snippet(h.Doc.Content, 120),
				})
			}
			return map[string]interface{}{"hits": out}, nil
		},
	})

	return s
}

func (s *Server) registerTool(t Tool) {
	s.tools[t.Name] = t
}

// HandleJSON 处理一条 JSON-RPC 消息，返回响应（通知类返回nil）
func (s *Server) HandleJSON(raw []byte) []byte {
	var req struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		return s.errorResponse(nil, -32700, "parse error")
	}

	switch req.Method {
	case "initialize":
		return s.response(req.ID, map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]interface{}{"tools": map[string]interface{}{}},
			"serverInfo":      s.serverInfo,
		})
	case "notifications/initialized":
		return nil
	case "tools/list":
		tools := make([]toolInfo, 0, len(s.tools))
		for _, t := range s.tools {
			tools = append(tools, toolInfo{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema})
		}
		return s.response(req.ID, map[string]interface{}{"tools": tools})
	case "tools/call":
		var params struct {
			Name      string                 `json:"name"`
			Arguments map[string]interface{} `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return s.errorResponse(req.ID, -32602, "invalid params")
		}
		tool, ok := s.tools[params.Name]
		if !ok {
			return s.errorResponse(req.ID, -32601, "tool not found: "+params.Name)
		}
		result, err := tool.Handler(params.Arguments)
		if err != nil {
			return s.errorResponse(req.ID, -32000, err.Error())
		}
		// MCP 规范：结果以 content 列表返回
		return s.response(req.ID, map[string]interface{}{
			"content": []map[string]interface{}{
				{"type": "text", "text": toJSON(result)},
			},
			"isError": false,
		})
	default:
		return s.errorResponse(req.ID, -32601, "method not found: "+req.Method)
	}
}

// Run 启动 stdio 传输循环：从 stdin 读 JSON 行，响应写 stdout（按 MCP 规范以换行分隔）
func (s *Server) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	dec := json.NewDecoder(in)
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		resp := s.HandleJSON(raw)
		if resp == nil {
			continue
		}
		if _, err := out.Write(append(resp, '\n')); err != nil {
			return err
		}
	}
}

func (s *Server) response(id json.RawMessage, result interface{}) []byte {
	out, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "id": id, "result": result,
	})
	return out
}

func (s *Server) errorResponse(id json.RawMessage, code int, message string) []byte {
	out, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "id": id,
		"error": map[string]interface{}{"code": code, "message": message},
	})
	return out
}

func toJSON(v interface{}) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

func snippet(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return string(r)
	}
	return string(r[:n]) + "…"
}

func round(v float64, n int) float64 {
	p := math.Pow(10, float64(n))
	return math.Round(v*p) / p
}

// ─── 安全表达式求值（递归下降解析器） ───
// 支持：数字、+ - * / ^、括号、一元负号、sqrt() abs() min() max()
// 安全性：完全自研解析器，不调用任何 eval 类机制，杜绝注入

type exprParser struct {
	tokens []string
	pos    int
}

func evalExpr(input string) (float64, error) {
	p := &exprParser{tokens: tokenizeExpr(input)}
	v, err := p.parseExpr()
	if err != nil {
		return 0, err
	}
	if p.pos != len(p.tokens) {
		return 0, fmt.Errorf("unexpected token: %s", p.tokens[p.pos])
	}
	return v, nil
}

func tokenizeExpr(s string) []string {
	var out []string
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n':
			continue
		case c >= '0' && c <= '9' || c == '.':
			j := i
			for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.') {
				j++
			}
			out = append(out, s[i:j])
			i = j - 1
		case strings.ContainsRune("+-*/^()", rune(c)):
			out = append(out, string(c))
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_':
			j := i
			for j < len(s) && (s[j] >= 'a' && s[j] <= 'z' || s[j] >= 'A' && s[j] <= 'Z' || s[j] == '_') {
				j++
			}
			out = append(out, s[i:j])
			i = j - 1
		default:
			out = append(out, string(c))
		}
	}
	return out
}

func (p *exprParser) peek() string {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return ""
}

func (p *exprParser) next() string {
	t := p.peek()
	p.pos++
	return t
}

func (p *exprParser) parseExpr() (float64, error) {
	return p.parseAddSub()
}

func (p *exprParser) parseAddSub() (float64, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return 0, err
	}
	for {
		switch p.peek() {
		case "+":
			p.next()
			r, err := p.parseMulDiv()
			if err != nil {
				return 0, err
			}
			left += r
		case "-":
			p.next()
			r, err := p.parseMulDiv()
			if err != nil {
				return 0, err
			}
			left -= r
		default:
			return left, nil
		}
	}
}

func (p *exprParser) parseMulDiv() (float64, error) {
	left, err := p.parsePow()
	if err != nil {
		return 0, err
	}
	for {
		switch p.peek() {
		case "*":
			p.next()
			r, err := p.parsePow()
			if err != nil {
				return 0, err
			}
			left *= r
		case "/":
			p.next()
			r, err := p.parsePow()
			if err != nil {
				return 0, err
			}
			if r == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			left /= r
		default:
			return left, nil
		}
	}
}

func (p *exprParser) parsePow() (float64, error) {
	left, err := p.parseUnary()
	if err != nil {
		return 0, err
	}
	if p.peek() == "^" {
		p.next()
		right, err := p.parseUnary()
		if err != nil {
			return 0, err
		}
		return math.Pow(left, right), nil
	}
	return left, nil
}

func (p *exprParser) parseUnary() (float64, error) {
	if p.peek() == "-" {
		p.next()
		v, err := p.parseUnary()
		return -v, err
	}
	if p.peek() == "+" {
		p.next()
		return p.parseUnary()
	}
	return p.parsePrimary()
}

func (p *exprParser) parsePrimary() (float64, error) {
	t := p.next()
	switch {
	case t == "(":
		v, err := p.parseExpr()
		if err != nil {
			return 0, err
		}
		if p.next() != ")" {
			return 0, fmt.Errorf("expected )")
		}
		return v, nil
	case isNumber(t):
		return strconv.ParseFloat(t, 64)
	case isFuncName(t):
		args, err := p.parseArgs()
		if err != nil {
			return 0, err
		}
		switch t {
		case "sqrt":
			if len(args) != 1 || args[0] < 0 {
				return 0, fmt.Errorf("sqrt requires non-negative single argument")
			}
			return math.Sqrt(args[0]), nil
		case "abs":
			if len(args) != 1 {
				return 0, fmt.Errorf("abs requires single argument")
			}
			return math.Abs(args[0]), nil
		case "min":
			if len(args) == 0 {
				return 0, fmt.Errorf("min requires arguments")
			}
			m := args[0]
			for _, a := range args[1:] {
				if a < m {
					m = a
				}
			}
			return m, nil
		case "max":
			if len(args) == 0 {
				return 0, fmt.Errorf("max requires arguments")
			}
			m := args[0]
			for _, a := range args[1:] {
				if a > m {
					m = a
				}
			}
			return m, nil
		}
		return 0, fmt.Errorf("unknown function: %s", t)
	default:
		return 0, fmt.Errorf("unexpected token: %s", t)
	}
}

func (p *exprParser) parseArgs() ([]float64, error) {
	if p.next() != "(" {
		return nil, fmt.Errorf("expected ( after function name")
	}
	var args []float64
	if p.peek() == ")" {
		p.next()
		return args, nil
	}
	for {
		v, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, v)
		switch p.next() {
		case ",":
			continue
		case ")":
			return args, nil
		default:
			return nil, fmt.Errorf("expected , or )")
		}
	}
}

func isNumber(s string) bool {
	if s == "" {
		return false
	}
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func isFuncName(s string) bool {
	switch s {
	case "sqrt", "abs", "min", "max":
		return true
	}
	return false
}
