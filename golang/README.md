# Go 版 -- 多Agent智能教育系统

> 基于 Go 标准库 + goroutine + channel 的实现，**零第三方依赖**

## 技术栈

- **HTTP**: Go 1.22 标准库 `net/http`（新路由语法）
- **Agent通信**: channel（CSP模型）
- **并发**: goroutine（每个Agent独立goroutine）
- **数据安全**: sync.RWMutex + ConcurrentMap
- **检索增强**: 纯标准库实现的 BM25 检索器（手写公式，无外部依赖）
- **MCP**: 纯标准库实现的 MCP Server（JSON-RPC 2.0 + stdio）

## 生产级增强（v2.0）

### 1. 全链路追踪 + 防活锁（EventBus）

- **correlation_id 透传**：Agent 间转发事件继承父事件追踪ID（`PublishChild`），可用 `GET /api/v1/trace/{id}` 回放完整事件链
- **防活锁**：事件跳数（Hops）超过上限（8）自动丢弃，防止 Agent 互相触发形成无限循环
- **事件去重**：同一事件链内相同类型只处理一次，防止事件风暴
- 覆盖测试：活锁循环截断、去重、链路回放、handler panic 隔离

### 2. Agent 评测框架（Eval）

`internal/eval` + `cmd/eval`：18 个黄金用例（含多轮序列）+ 全链路驱动（走真实 EventBus），指标实测：

```
用例通过率    : 18/18 (100%)
引导率        : 100.0% (18/18 回复为引导式提问)
提示级别准确率 : 100.0% (4/4，含 level1→2→2→3 升级链路)
干预准确率    : 100.0% (2/2，挫败降难度 + 厌倦提难度)
平均对话轮数  : 3.0 轮到达掌握（3 个完整对话场景）
```

运行：`go run cmd/eval/main.go`，Prompt/策略迭代后重跑即可防回归。

### 3. RAG 检索增强（Tutor Agent）

- 纯标准库 BM25 检索器（unigram+bigram 中文切词，无分词器依赖）
- 内置 8 篇教材文档，按知识点名称检索
- Tutor 生成回复前注入教材引用（citation），回复可溯源
- API：`GET /api/v1/retrieve?q=二次函数`；MCP 工具：`retrieve_textbook`
- 可选 LLM：配置 `OPENAI_API_KEY` 后 Tutor 自动切换为"检索+LLM生成"，失败降级模板

### 4. MCP Server（Model Context Protocol）

`cmd/mcp`：纯标准库实现的 MCP Server（stdio + JSON-RPC 2.0），支持任意 MCP 客户端接入：

```
go run cmd/mcp/main.go
```

| 工具 | 说明 |
|------|------|
| `query_knowledge_graph` | 查询知识点：名称/难度/前置依赖 |
| `calculate` | 安全数学表达式求值（自研递归下降解析器，杜绝注入） |
| `retrieve_textbook` | RAG 教材检索 |

## 快速开始

```bash
cd golang
go run cmd/main.go          # 启动 HTTP 服务 :8081
go run cmd/eval/main.go     # 运行 Agent 评测
go run cmd/mcp/main.go      # 启动 MCP Server (stdio)
go test ./...               # 全部测试（事件总线/BM25/MCP/Eval）
```

## 目录结构

```
golang/
├── cmd/
│   ├── main.go              # HTTP 服务入口
│   ├── eval/main.go         # 评测 CLI
│   └── mcp/main.go          # MCP Server (stdio)
└── internal/
    ├── agent/agents.go      # 5个Agent实现（Tutor含RAG/LLM）
    ├── eventbus/eventbus.go # channel事件总线（trace+防活锁+去重）
    ├── model/learner.go     # BKT + SM-2
    ├── model/knowledge.go   # 知识图谱（20节点DAG）
    ├── rag/                 # BM25检索器 + 教材文档集
    ├── llm/client.go        # OpenAI兼容客户端（可选，标准库实现）
    ├── eval/                # 评测框架（黄金用例+指标）
    ├── mcp/server.go        # MCP Server（JSON-RPC 2.0）
    └── api/handler.go       # HTTP路由（含 trace/retrieve/next-topics）
```
