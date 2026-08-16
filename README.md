# 🎓 多Agent智能教育与个性化学习系统

> **Python + Go 双语言实现 | 企业级 Mesh + 事件驱动架构**

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](python/)
[![Go](https://img.shields.io/badge/Go-1.22+-cyan.svg)](golang/)
[![React](https://img.shields.io/badge/React-18+-purple.svg)](frontend/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📖 项目简介

这是一个**多Agent智能教育系统**，用5个AI Agent协作完成个性化教学：

- **个性化评估**：贝叶斯知识追踪（BKT）实时估计学生对每个知识点的掌握度
- **引导式教学**：苏格拉底式提问引导学生自己思考，而非直接给答案
- **自适应复习**：基于间隔重复算法（SM-2）动态排期，在前置知识达标后再推进

核心设计目标：**从"千人一面的网课"到"根据每个学生的薄弱点实时调整的教学系统"**。

---

## 🏗️ 系统架构

### 整体架构图

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
| **Tutor Agent (教学)** | 苏格拉底式提问教学，动态调整难度 | Prompt Engineering、难度自适应 |
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

### 核心事件流

```
学生答题
  → STUDENT_SUBMISSION 事件
  → Assessment Agent 处理
      → MASTERY_UPDATED 事件
          → Curriculum Agent（更新SM-2复习计划）
      → ASSESSMENT_COMPLETE 事件
          → Tutor Agent（生成苏格拉底式回复）
          → Engagement Agent（分析学习状态）
  → Engagement Agent 并行处理
      → 如果检测到挫败 → ENGAGEMENT_ALERT 事件
          → Tutor Agent（降低难度）
          → Curriculum Agent（放慢节奏）
```

---

## 📂 项目结构

```
multi-agent-education/
│
├── 📄 README.md              ← 项目说明
├── 📄 LICENSE                ← MIT 开源协议
├── 📄 docker-compose.yml     ← 一键启动全部服务
├── 📄 .env.example           ← 环境变量模板（不含真实密钥）
│
├── 📁 docs/                  ← 架构与部署文档
│   ├── architecture.md       ← 架构设计详解
│   ├── knowledge-points.md   ← 知识图谱知识点体系
│   └── deployment.md         ← 部署指南
│
├── 📁 python/                ← 🐍 Python 实现（AI 生态最丰富）
│   ├── README.md
│   ├── agents/               ← 5个Agent实现
│   ├── core/                 ← 核心模块（事件总线、BKT、SM-2、知识图谱、LLM客户端）
│   ├── api/                  ← FastAPI + WebSocket
│   ├── config/               ← 配置管理
│   └── tests/                ← 单元与集成测试
│
├── 📁 golang/                ← 🔷 Go 实现（goroutine + channel）
│   ├── README.md
│   └── internal/             ← agent / eventbus / model / api
│
└── 📁 frontend/              ← ⚛️ React 前端（两个后端通用）
    ├── package.json
    └── src/
```

---

## 🚀 快速开始

### 方式一：Docker 一键启动（全部服务）

```bash
docker-compose up -d

# 前端：http://localhost:3000
# Python API：http://localhost:8000/docs
```

### 方式二：Python 版（推荐）

```bash
cd python
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp ../.env.example ../.env   # 填入你的 LLM API Key
python -m api.main
# 打开 http://localhost:8000/docs 查看 API 文档
```

### 方式三：Go 版

```bash
cd golang
go mod tidy
go run cmd/main.go
# 访问 http://localhost:8081/api/v1/health
```

---

## 🧠 核心算法

### 1. SM-2 间隔重复算法（Curriculum Agent）

```
复习间隔计算：
  I(1) = 1 天（第1次复习）
  I(2) = 6 天（第2次复习）
  I(n) = I(n-1) × EF（之后每次间隔 = 上次间隔 × 难度因子）

难度因子更新：
  EF' = EF - 0.8 + 0.28 × q - 0.02 × q²   （q 为回答质量 0-5 分）
```

代码位置：[python/core/spaced_repetition.py](python/core/spaced_repetition.py)

### 2. 贝叶斯知识追踪 BKT（Assessment Agent）

```
四个核心参数：
  P(L₀) = 初始掌握概率    P(T) = 学习转移概率
  P(G)  = 猜测概率        P(S) = 失误概率

更新公式（贝叶斯后验）：
  答对: P(Lₙ|correct) = P(Lₙ₋₁) × (1 - P(S)) / P(correct)
  答错: P(Lₙ|wrong)   = P(Lₙ₋₁) × P(S) / P(wrong)
```

代码位置：[python/core/learner_model.py](python/core/learner_model.py)

### 3. 苏格拉底式教学（Tutor Agent）

不直接给答案，通过提问引导学生自己发现：

```
学生："二次函数 y = x² + 2x + 1 的顶点在哪里？"

❌ 直接给答案："顶点是 (-1, 0)"

✅ 苏格拉底式引导：
  第1轮："你知道二次函数的顶点公式吗？或者，你能把这个式子配方吗？"
  第2轮："很好！你配成了 y = (x+1)²，那 (x+1)² 最小值是多少？"
  第3轮："对了！所以顶点坐标是？"
```

代码位置：[python/agents/tutor_agent.py](python/agents/tutor_agent.py)

---

## 🔧 技术栈与双语言对比

| 维度 | Python | Go |
|------|--------|-----|
| **框架** | LangGraph + FastAPI | Gin/标准库 |
| **Agent编排** | StateGraph 状态机 | goroutine + channel |
| **事件总线** | asyncio + pub/sub | Go channel (CSP) |
| **WebSocket** | FastAPI WebSocket | gorilla/websocket |
| **数据库** | SQLAlchemy + asyncpg | GORM |
| **并发模型** | 协程（I/O密集型） | goroutine 仅4KB（高并发） |
| **适合场景** | AI/ML 生态最丰富 | 高并发、低延迟 |

### 架构参考

| 项目 | 参考了什么 |
|------|-----------|
| [CrewAI](https://github.com/crewAIInc/crewAI) | Agent协作模式、编排设计 |
| [LangGraph](https://github.com/langchain-ai/langgraph) | StateGraph 状态机、条件路由 |
| [Solace Agent Mesh](https://github.com/SolaceLabs/solace-agent-mesh) | 事件驱动Mesh架构 |
| IntelliCode (EACL 2026 论文) | 多Agent教育架构、集中式学习者模型 |

---

## 📚 学习路线

1. **理解原理**：阅读 [docs/architecture.md](docs/architecture.md) 了解架构设计，理解 SM-2 与 BKT 核心公式
2. **看懂代码**：从 [python/core/event_bus.py](python/core/event_bus.py) 开始，逐个阅读 5 个 Agent，运行测试 `python -m pytest tests/`
3. **动手修改**：添加新知识点到知识图谱、修改苏格拉底式 Prompt 模板、调整 SM-2 参数观察效果

---

## 📄 开源协议

本项目采用 [MIT License](LICENSE) 开源协议，可自由使用、修改、分发。
