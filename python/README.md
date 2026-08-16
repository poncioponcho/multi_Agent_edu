# Python 版 -- 多Agent智能教育系统

> 基于 LangGraph + FastAPI + asyncio 的实现

## 技术栈

- **Agent框架**: LangGraph (StateGraph)
- **Web框架**: FastAPI + Uvicorn
- **实时通信**: WebSocket
- **异步**: asyncio (Python 3.11+)
- **数据模型**: Pydantic v2
- **测试**: pytest + pytest-asyncio

## 快速开始

```bash
# 1. 创建虚拟环境
python -m venv venv
source venv/bin/activate

# 2. 安装依赖
pip install -r requirements.txt

# 3. 配置环境变量
cp ../.env.example ../.env
# 编辑 .env 填入 API Key

# 4. 启动
python -m api.main

# 5. 查看 API 文档
# 浏览器打开 http://localhost:8000/docs
```

## 运行测试

```bash
python -m pytest tests/ -v
```

## 目录结构

```
python/
├── agents/                  # 5个Agent实现
│   ├── base_agent.py        # 基类（模板方法模式）
│   ├── assessment_agent.py  # BKT知识追踪
│   ├── tutor_agent.py       # 苏格拉底式教学
│   ├── curriculum_agent.py  # SM-2间隔重复 + 路径规划
│   ├── hint_agent.py        # 三级提示策略
│   └── engagement_agent.py  # 学习状态监测
├── core/                    # 核心算法模块
│   ├── event_bus.py         # 事件驱动总线（Mesh通信层）
│   ├── learner_model.py     # 贝叶斯知识追踪
│   ├── spaced_repetition.py # SM-2算法
│   └── knowledge_graph.py   # 知识图谱（DAG）
├── api/                     # FastAPI接口层
│   ├── main.py              # 入口
│   ├── orchestrator.py      # Agent编排器
│   ├── routes.py            # REST路由
│   └── websocket.py         # WebSocket端点
├── config/                  # 配置
│   └── settings.py          # 环境变量管理
└── tests/                   # 单元测试
    └── test_agents.py       # 核心模块测试
```

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/health` | 健康检查 |
| POST | `/api/v1/submit` | 提交答题结果 |
| POST | `/api/v1/question` | 学生提问 |
| POST | `/api/v1/message` | 学生消息（对话） |
| GET | `/api/v1/progress/{learner_id}` | 学习进度 |
| GET | `/api/v1/knowledge-graph` | 知识图谱 |
| WS | `/ws/{learner_id}` | WebSocket实时通信 |
