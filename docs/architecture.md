# 架构设计详解

## 1. 整体架构

本系统采用 **Mesh + 事件驱动** 架构，5个Agent通过EventBus双向异步通信。

### 架构图

```
                        ┌──────────────┐
                        │  React 前端   │
                        │  (WebSocket)  │
                        └──────┬───────┘
                               │
                        ┌──────▼───────┐
                        │ API Gateway  │
                        │  / EventBus  │
                        └──────┬───────┘
                               │
      ┌────────────────────────┼────────────────────────┐
      │                        │                        │
 ┌────▼──────┐          ┌─────▼──────┐          ┌──────▼──────┐
 │ Assessment│◄────────►│   Tutor    │◄────────►│    Hint     │
 │   Agent   │          │   Agent    │          │    Agent    │
 └────┬──────┘          └────────────┘          └─────────────┘
      │                       ▲
      │                       │
 ┌────▼──────┐          ┌─────┴──────┐
 │ Curriculum│◄────────►│ Engagement │
 │   Agent   │          │   Agent    │
 └───────────┘          └────────────┘
      │                       │
      └───────┬───────────────┘
              ▼
      ┌───────────────┐
      │ 共享学习者状态  │
      │ (PostgreSQL)  │
      └───────────────┘
```

## 2. 事件流设计

### 核心事件流：学生答题

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

### 提示流：学生卡住

```
学生连续答错
  → Tutor Agent 检测到 attempts >= 2
  → HINT_NEEDED 事件
  → Hint Agent 处理
      → 判断提示级别（1/2/3）
      → HINT_RESPONSE 事件
      → Tutor Agent 转发给学生
```

## 3. 为什么选择 Mesh + 事件驱动

### 与其他架构模式的对比

**Supervisor 模式**：
- 中心化调度器分配任务给各Agent
- 缺点：调度器是瓶颈和单点故障
- 不适合本场景：教育交互是双向的、非线性的

**Pipeline 模式**：
- Agent按固定顺序串行处理
- 缺点：灵活性差，不支持双向交互
- 不适合本场景：学习路径不是固定的

**Mesh 模式** (我们的选择)：
- Agent之间通过事件总线自由通信
- 优点：松耦合、可扩展、支持双向交互
- 适合本场景：Agent需要实时感知和响应其他Agent的状态

### 开闭原则验证

新增一个"家长通知Agent"只需要：

```python
class ParentNotifyAgent(BaseAgent):
    @property
    def subscribed_events(self):
        return [EventType.WEAKNESS_DETECTED, EventType.ENCOURAGEMENT]

    async def handle_event(self, event):
        # 发送通知给家长
        pass
```

无需修改任何现有Agent代码。

## 4. 三种语言的事件总线实现对比

| 维度 | Python | Java | Go |
|------|--------|------|----|
| 事件总线 | 自定义EventBus (asyncio) | Spring ApplicationEvent | channel + goroutine |
| 订阅方式 | bus.subscribe(type, handler) | @EventListener注解 | bus.Subscribe(type, fn) |
| 异步机制 | asyncio.gather并发 | @Async线程池 | go func(){}() |
| 线程安全 | asyncio单线程（天然安全） | ConcurrentHashMap | sync.RWMutex |
| 分发策略 | 并发通知所有handler | Spring容器管理 | 每个handler独立goroutine |

## 5. 数据流

### 学习者模型数据流

```
学生行为数据 → Assessment Agent → BKT更新mastery
                                → 持久化到数据库
                                → 事件通知其他Agent
                                      ↓
                               Curriculum Agent → SM-2更新复习计划
                               Tutor Agent → 调整教学难度
                               Engagement Agent → 检测学习状态
```

### 状态一致性保证

- **单写者策略**：mastery只由Assessment Agent写入
- **事件溯源**：所有变更通过事件记录，可追溯
- **版本号**：每次更新带version，防止并发冲突
