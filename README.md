# 🎓 多Agent智能教育与个性化学习系统

> **Go + Python 双语言实现 | 企业级 Mesh + 事件驱动架构 | 零依赖 Go 版**

[![Go](https://img.shields.io/badge/Go-1.22+-cyan.svg)](golang/)
[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](python/)
[![React](https://img.shields.io/badge/React-18+-purple.svg)](frontend/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📖 项目简介

这是一个**多Agent智能教育系统**，用5个AI Agent协作完成个性化教学：

- **个性化评估**：贝叶斯知识追踪（BKT）实时估计学生对每个知识点的掌握度
- **引导式教学**：苏格拉底式提问引导学生自己思考，而非直接给答案
- **自适应复习**：基于间隔重复算法（SM-2）动态排期，在前置知识达标后再推进

核心设计目标：**从"千人一面的网课"到"根据每个学生的薄弱点实时调整的教学系统"**。

**Go 版亮点**：全部基于标准库实现（BM25 检索器、MCP Server、LLM 客户端均为手写，零第三方依赖），并具备**全链路追踪、防活锁、Agent 评测框架、RAG 检索增强、MCP 协议接入**五项生产级能力，实测指标见下文。

---

## 🏗️ 系统架构

```
                    ┌──────────────────────────┐
                    │   React 前端 (WebSocket)   │
                    └────────────┬─────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │   API Gateway / EventBus   │
                    └────────────┬─────────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        │                        │                        │
   ┌────▼─────┐           ┌─────▼─────┐           ┌──────▼──────┐
   │Assessment │◄─────────►│  Tutor    │◄─────────►│  Hint       │
   │  Agent    │           │  Agent    │           │  Agent      │
   │ (评估)    │           │ (教学)    │           │ (提示)      │
   └────┬──────┘           └───────────┘           └─────────────┘
        │                        ▲
        │                        │
   ┌────▼──────┐           ┌─────┴─────┐
   │Curriculum  │◄─────────►│Engagement │
   │  Agent     │           │  Agent    │
   │ (课程)     │           │ (互动)    │
   └────────────┘           └───────────┘
        │                        │
        └────────┬───────────────┘
                 │
        ┌────────▼────────┐
        │  共享学习者状态   │
        │ PostgreSQL+Redis │
        └─────────────────┘
```

### 5个Agent职责说明

| Agent | 职责 | 核心算法/技术 |
|-------|------|---------------|
| **Assessment Agent (评估)** | 知识点掌握度评估、学习路径诊断 | 贝叶斯知识追踪(BKT)、Beta分布 |
| **Tutor Agent (教学)** | 苏格拉底式提问教学，动态调整难度 | RAG检索增强、Prompt Engineering、难度自适应 |
| **Curriculum Agent (课程)** | 动态生成学习路径，间隔重复排期 | SM-2算法、知识图谱拓扑排序 |
| **Hint Agent (提示)** | 分级提示：暗示→引导→直接答案 | 三级提示策略、尝试次数分析 |
| **Engagement Agent (互动)** | 监测学习状态，适时鼓励、调整节奏 | 情感分析、响应时间分析 |

### 为什么用 Mesh + 事件驱动？

| 编排模式 | 特点 | 适用场景 |
|----------|------|----------|
| **Supervisor (监督者)** | 中心化调度，单点瓶颈 | 简单串行任务流 |
| **Pipeline (管道)** | 线性流转，灵活性低 | 数据处理流水线 |
| **Mesh + 事件驱动** ✅ | Agent双向通信，松耦合 | **教育场景：需要实时双向交互** |

选择 Mesh 的原因：

- 教学过程中，Tutor 需要随时请求 Hint，Assessment 需要通知 Curriculum 调整路径
- Agent 之间是**双向、异步、事件驱动**的，不是简单的串行调用
- 新增 Agent 只需订阅事件，无需修改现有代码（**开闭原则**）

---

## ⚡ Go 版生产级能力（v2.0）

### 全链路追踪 + 防活锁

EventBus 增加 `correlation_id` 透传（Agent 间转发自动继承）、事件最大跳数限制（防活锁）与去重（防事件风暴）：

```
GET /api/v1/trace/{correlationID}   # 回放完整事件链
```

覆盖测试：活锁循环截断 ✅ 事件去重 ✅ 链路回放 ✅ handler panic 隔离 ✅

### Agent 评测框架（实测数据）

18 个黄金用例（含多轮序列：提示级别升级、挫败/厌倦干预）走真实 EventBus 全链路评测：

| 指标 | 实测值 |
|------|--------|
| 用例通过率 | 18/18 (100%) |
| 引导率（非直接给答案） | 100% |
| 提示级别准确率（level1→2→2→3） | 100% (4/4) |
| 干预准确率（挫败降难度/厌倦提难度） | 100% (2/2) |
| 平均对话轮数（到达掌握） | 3.0 轮 |

### RAG 检索增强 + 可选 LLM

- 纯标准库 BM25 检索器（中文 unigram+bigram 切词），内置 8 篇教材文档
- Tutor 回复自动注入教材引用（📖 可溯源）；配置 `OPENAI_API_KEY` 后自动切换"检索 + LLM 生成"，失败降级模板

### MCP Server（零依赖实现）

纯标准库实现 MCP 协议（JSON-RPC 2.0 + stdio），3 个工具：`query_knowledge_graph`（知识图谱查询）、`calculate`（安全数学求值，自研解析器）、`retrieve_textbook`（RAG 检索）。

---

## 🚀 快速开始

### Go 版（推荐）

```bash
cd golang
go run cmd/main.go          # HTTP 服务 :8081（无第三方依赖，直接可跑）
go run cmd/eval/main.go     # 运行 Agent 评测（18 用例）
go run cmd/mcp/main.go      # 启动 MCP Server (stdio)
go test ./...               # 全部测试
```

### Docker 一键启动（Python 版 + 前端 + 数据库）

```bash
docker-compose up -d
# 前端：http://localhost:3000
# Python API：http://localhost:8000/docs
```

### Python 版

```bash
cd python
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp ../.env.example ../.env   # 填入 LLM API Key
python -m api.main           # http://localhost:8000/docs
```

---

## 🧠 核心算法

### 1. SM-2 间隔重复算法（Curriculum Agent）

```
复习间隔：I(1)=1天, I(2)=6天, I(n)=I(n-1)×EF
难度因子：EF' = EF - 0.8 + 0.28×q - 0.02×q²   （q 为回答质量 0-5 分）
```

代码位置：[golang/internal/model/learner.go](golang/internal/model/learner.go)

### 2. 贝叶斯知识追踪 BKT（Assessment Agent）

```
参数：P(L₀)先验、P(T)学习转移、P(G)猜测、P(S)失误
答对: P(Lₙ|correct) = P(Lₙ₋₁)×(1-P(S)) / P(correct)
答错: P(Lₙ|wrong)   = P(Lₙ₋₁)×P(S) / P(wrong)
```

代码位置：[golang/internal/model/learner.go](golang/internal/model/learner.go)

### 3. 苏格拉底式教学（Tutor Agent）

不直接给答案，通过提问引导学生自己发现：

```
学生："二次函数 y = x² + 2x + 1 的顶点在哪里？"
✅ 第1轮："你知道二次函数的顶点公式吗？或者，你能把这个式子配方吗？"
  第2轮："很好！你配成了 y = (x+1)²，那 (x+1)² 最小值是多少？"
  第3轮："对了！所以顶点坐标是？"
```

代码位置：[golang/internal/agent/agents.go](golang/internal/agent/agents.go)

---

## 🔧 双语言对比

| 维度 | Go（零依赖） | Python |
|------|--------------|--------|
| **HTTP** | 标准库 net/http | FastAPI |
| **Agent编排** | goroutine + channel (CSP) | LangGraph StateGraph |
| **事件总线** | channel + 手写 trace/防活锁 | asyncio + pub/sub |
| **RAG** | 手写 BM25（标准库） | LangChain 生态 |
| **MCP** | 手写 JSON-RPC Server | 生态库 |
| **LLM** | 手写 OpenAI 兼容客户端 | openai SDK |
| **并发模型** | goroutine 仅 4KB | 协程（I/O 密集） |
| **适合场景** | 高并发、低延迟、深度定制 | AI 生态丰富、快速迭代 |

### 架构参考

| 项目 | 参考了什么 |
|------|-----------|
| [CrewAI](https://github.com/crewAIInc/crewAI) | Agent协作模式、编排设计 |
| [LangGraph](https://github.com/langchain-ai/langgraph) | StateGraph 状态机、条件路由 |
| [Solace Agent Mesh](https://github.com/SolaceLabs/solace-agent-mesh) | 事件驱动Mesh架构 |
| IntelliCode (EACL 2026 论文) | 多Agent教育架构、集中式学习者模型 |

---

## 📚 学习路线

1. **理解原理**：阅读 [docs/architecture.md](docs/architecture.md)，理解 SM-2 与 BKT 核心公式
2. **看懂代码**：Go 版从 [internal/eventbus/eventbus.go](golang/internal/eventbus/eventbus.go) 开始，逐个阅读 5 个 Agent，运行 `go test ./...`
3. **动手修改**：修改苏格拉底式 Prompt 模板、调整 SM-2 参数、给评测集新增用例（`internal/eval/eval.go`）

---

## 📄 开源协议

本项目采用 [MIT License](LICENSE) 开源协议，可自由使用、修改、分发。
