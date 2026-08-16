# Go 版 -- 多Agent智能教育系统

> 基于 Go 标准库 + goroutine + channel 的实现

## 技术栈

- **HTTP**: Go 1.22 标准库 `net/http`（新路由语法）
- **Agent通信**: channel（CSP模型）
- **并发**: goroutine（每个Agent独立goroutine）
- **数据安全**: sync.RWMutex + ConcurrentMap

## Go 版的优势

| 维度 | Go 版 | Python 版 | Java 版 |
|------|-------|----------|---------|
| 并发模型 | goroutine (轻量级) | asyncio (协程) | Thread (重量级) |
| 通信 | channel (CSP) | asyncio queue | EventPublisher |
| 内存 | goroutine 仅4KB | 协程较轻量 | 线程约1MB |
| 适合场景 | 高并发、低延迟 | AI/ML生态 | 企业级、强类型 |

## 快速开始

```bash
cd golang
go mod tidy
go run cmd/main.go
# 访问 http://localhost:8081/api/v1/health
```

## 目录结构

```
golang/
├── cmd/main.go              # 入口
├── go.mod
└── internal/
    ├── agent/agents.go      # 5个Agent实现
    ├── eventbus/eventbus.go # channel事件总线
    ├── model/learner.go     # BKT + SM-2
    └── api/handler.go       # HTTP路由
```
