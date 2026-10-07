"""REST API 路由。"""

import asyncio
import os
import sys
import time

from fastapi import APIRouter, Request
from pydantic import BaseModel

router = APIRouter(tags=["education"])

# 进程启动时间，用于 /debug/stats 计算运行时长
_STARTED_AT = time.monotonic()


def _peak_rss_bytes() -> int:
    """进程峰值常驻内存。

    Go 版对应字段是堆分配量；Python 侧取 ru_maxrss（峰值 RSS）作为近似。
    注意单位差异：macOS 返回字节，Linux 返回 KB。
    """
    try:
        import resource

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(rss if sys.platform == "darwin" else rss * 1024)
    except Exception:  # pragma: no cover - 取不到指标不应影响接口
        return 0


class SubmitAnswerRequest(BaseModel):
    learner_id: str
    knowledge_id: str
    is_correct: bool
    time_spent_seconds: float = 0


class AskQuestionRequest(BaseModel):
    learner_id: str
    knowledge_id: str
    question: str


class SendMessageRequest(BaseModel):
    learner_id: str
    message: str
    knowledge_id: str = "general"


@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "multi-agent-education", "agents": 5}


@router.post("/submit")
async def submit_answer(req: SubmitAnswerRequest, request: Request):
    """学生提交答题结果。"""
    orch = request.app.state.orchestrator
    events = await orch.submit_answer(
        req.learner_id, req.knowledge_id, req.is_correct, req.time_spent_seconds
    )
    return {
        "status": "processed",
        "events_triggered": len(events),
        "events": [
            {"type": e.type.value, "source": e.source, "data": e.data}
            for e in events[-10:]
        ],
    }


@router.post("/question")
async def ask_question(req: AskQuestionRequest, request: Request):
    """学生提问。"""
    orch = request.app.state.orchestrator
    events = await orch.ask_question(req.learner_id, req.knowledge_id, req.question)
    return {
        "status": "processed",
        "events_triggered": len(events),
        "events": [
            {"type": e.type.value, "source": e.source, "data": e.data}
            for e in events[-10:]
        ],
    }


@router.post("/message")
async def send_message(req: SendMessageRequest, request: Request):
    """学生发送消息（对话）。"""
    orch = request.app.state.orchestrator
    events = await orch.send_message(req.learner_id, req.message, req.knowledge_id)
    return {
        "status": "processed",
        "events_triggered": len(events),
        "events": [
            {"type": e.type.value, "source": e.source, "data": e.data}
            for e in events[-10:]
        ],
    }


@router.get("/progress/{learner_id}")
async def get_progress(learner_id: str, request: Request):
    """获取学生学习进度。"""
    orch = request.app.state.orchestrator
    return orch.get_learner_progress(learner_id)


@router.get("/knowledge-graph")
async def get_knowledge_graph(request: Request):
    """获取知识图谱结构。"""
    orch = request.app.state.orchestrator
    graph = orch.curriculum.knowledge_graph
    return {
        "nodes": [
            {
                "id": n.id,
                "name": n.name,
                "difficulty": n.difficulty,
                "prerequisites": n.prerequisites,
                "tags": n.tags,
            }
            for n in graph.nodes.values()
        ],
        "learning_order": graph.topological_sort(),
    }


@router.get("/debug/stats")
async def debug_stats(request: Request):
    """运行时指标 —— 供压测采样。

    字段名与 Go 版 `/api/v1/debug/stats` 对齐，使同一个压测器可以同时压两边。
    Python 侧没有 goroutine / GC 计数，对应字段置 0 而非省略，避免压测器解析失败。
    """
    from api.websocket import manager

    orch = getattr(request.app.state, "orchestrator", None)

    ws_connections = sum(len(v) for v in manager.active_connections.values())

    return {
        "uptime_seconds": round(time.monotonic() - _STARTED_AT, 3),
        "goroutines": 0,  # Python 侧无 goroutine 概念
        "asyncio_tasks": len(asyncio.all_tasks()),
        "num_cpu": os.cpu_count() or 0,
        "heap_alloc_bytes": _peak_rss_bytes(),  # 语义为峰值 RSS，见 _peak_rss_bytes
        "gc_count": 0,
        "learners": len(orch.learner_models) if orch else 0,
        "ws_connections": ws_connections,
        "ws_learners": len(manager.active_connections),
        "eventbus": {
            "history_len": len(orch.event_bus._event_history) if orch else 0,
            "seen_len": 0,
            "queue_len": 0,
            "queue_cap": 0,
            "dropped": 0,
        },
    }
