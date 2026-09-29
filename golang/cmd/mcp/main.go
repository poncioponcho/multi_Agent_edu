// MCP Server：以 stdio 协议暴露知识图谱查询、数学计算、教材检索能力。
//
// 用法：
//
//	go run cmd/mcp/main.go            # 启动 stdio 模式的 MCP Server
//
// 支持 MCP 方法：initialize / tools/list / tools/call
// 支持工具：query_knowledge_graph / calculate / retrieve_textbook
package main

import (
	"context"
	"log"
	"os"

	"github.com/multi-agent-education/golang/internal/mcp"
	"github.com/multi-agent-education/golang/internal/model"
	"github.com/multi-agent-education/golang/internal/rag"
)

func main() {
	srv := mcp.NewServer(model.KnowledgeGraph, rag.NewRetriever())
	log.Println("[MCP] server starting (stdio)")
	if err := srv.Run(context.Background(), os.Stdin, os.Stdout); err != nil {
		log.Fatal(err)
	}
}
