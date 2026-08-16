// Package llm 提供轻量 OpenAI 兼容 LLM 客户端（纯标准库实现，零第三方依赖）。
//
// 设计要点（面试向）：
//   - 与 Agent 解耦：Tutor Agent 通过该抽象层调用模型，无 API Key 时自动降级模板回复
//   - 模型可替换：通过环境变量切换 provider/baseURL/model
//   - 超时与重试：单次请求超时 30s，失败自动重试 1 次（指数退避）
package llm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// Message 对话消息
type Message struct {
	Role    string `json:"role"` // system / user / assistant
	Content string `json:"content"`
}

// Client LLM客户端
type Client struct {
	apiKey    string
	baseURL   string
	model     string
	http      *http.Client
}

// NewClientFromEnv 从环境变量创建客户端：
//   - OPENAI_API_KEY：必填，缺失返回 nil（调用方降级为模板）
//   - OPENAI_BASE_URL：可选，默认 https://api.openai.com/v1
//   - OPENAI_MODEL：可选，默认 gpt-4o
func NewClientFromEnv() *Client {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" || apiKey == "your-openai-api-key-here" {
		return nil
	}
	baseURL := os.Getenv("OPENAI_BASE_URL")
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	model := os.Getenv("OPENAI_MODEL")
	if model == "" {
		model = "gpt-4o"
	}
	return &Client{
		apiKey:  apiKey,
		baseURL: baseURL,
		model:   model,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Chat 发送对话请求，返回助手回复
func (c *Client) Chat(messages []Message, temperature float64) (string, error) {
	if c == nil {
		return "", errors.New("llm client not configured")
	}

	payload := map[string]interface{}{
		"model":       c.model,
		"messages":    messages,
		"temperature": temperature,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		req, err := http.NewRequest(http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("llm api status=%d body=%s", resp.StatusCode, truncate(string(raw), 200))
			continue
		}

		var result struct {
			Choices []struct {
				Message Message `json:"message"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(raw, &result); err != nil {
			lastErr = err
			continue
		}
		if len(result.Choices) == 0 {
			lastErr = errors.New("llm api returned no choices")
			continue
		}
		return result.Choices[0].Message.Content, nil
	}
	return "", lastErr
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
