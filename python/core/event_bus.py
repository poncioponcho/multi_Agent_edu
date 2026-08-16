"""
事件驱动总线 -- Mesh架构的核心通信层。

所有Agent通过EventBus发布/订阅事件，实现双向异步通信。
这是Mesh架构区别于Supervisor架构的关键：没有中心调度者，
每个Agent自主决定响应哪些事件。

面试要点：
- 发布-订阅模式 vs 请求-响应模式
- 异步解耦的优势：松耦合、可扩展、容错
- 事件溯源（Event Sourcing）的基本思想
"""

import asyncio
import logging
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Coroutine

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    """系统中所有事件类型的枚举定义。"""

    # 学生交互事件
    STUDENT_SUBMISSION = "student.submission"
    STUDENT_QUESTION = "student.question"
    STUDENT_MESSAGE = "student.message"

    # Assessment Agent 事件
    ASSESSMENT_COMPLETE = "assessment.complete"
    MASTERY_UPDATED = "assessment.mastery_updated"
    WEAKNESS_DETECTED = "assessment.weakness_detected"

    # Tutor Agent 事件
    TEACHING_RESPONSE = "tutor.teaching_response"
    HINT_NEEDED = "tutor.hint_needed"
    DIFFICULTY_ADJUSTED = "tutor.difficulty_adjusted"

    # Curriculum Agent 事件
    PATH_UPDATED = "curriculum.path_updated"
    REVIEW_SCHEDULED = "curriculum.review_scheduled"
    NEXT_TOPIC = "curriculum.next_topic"

    # Hint Agent 事件
    HINT_RESPONSE = "hint.response"

    # Engagement Agent 事件
    ENGAGEMENT_ALERT = "engagement.alert"
    ENCOURAGEMENT = "engagement.encouragement"
    PACE_ADJUSTMENT = "engagement.pace_adjustment"


class Event(BaseModel):
    """事件数据模型。"""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: EventType
    source: str  # 发布事件的Agent名称
    timestamp: datetime = Field(default_factory=datetime.now)
    learner_id: str
    data: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None  # 用于追踪事件链


EventHandler = Callable[[Event], Coroutine[Any, Any, None]]


class EventBus:
    """
    异步事件总线，支持发布-订阅模式。

    核心设计：
    1. 每个Agent注册自己感兴趣的事件类型
    2. 发布事件时，总线将事件分发给所有订阅者
    3. 支持事件历史记录（Event Sourcing基础）
    4. 支持事件过滤和优先级（可选扩展）
    """

    def __init__(self) -> None:
        self._subscribers: dict[EventType, list[EventHandler]] = {}
        self._event_history: list[Event] = []
        self._lock = asyncio.Lock()

    def subscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """订阅某类事件。"""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)
        logger.info("Handler subscribed to %s", event_type.value)

    def unsubscribe(self, event_type: EventType, handler: EventHandler) -> None:
        """取消订阅。"""
        if event_type in self._subscribers:
            self._subscribers[event_type].remove(handler)

    async def publish(self, event: Event) -> None:
        """
        发布事件到总线，异步通知所有订阅者。

        关键设计：使用 asyncio.gather 并发通知所有订阅者，
        而不是串行调用，提高吞吐量。
        """
        async with self._lock:
            self._event_history.append(event)

        logger.info(
            "[EventBus] %s -> %s (learner=%s)",
            event.source,
            event.type.value,
            event.learner_id,
        )

        handlers = self._subscribers.get(event.type, [])
        if not handlers:
            logger.debug("No handlers for event type %s", event.type.value)
            return

        tasks = [self._safe_handle(handler, event) for handler in handlers]
        await asyncio.gather(*tasks)

    async def _safe_handle(self, handler: EventHandler, event: Event) -> None:
        """安全执行handler，捕获异常避免影响其他订阅者。"""
        try:
            await handler(event)
        except Exception:
            logger.exception(
                "Error in handler for event %s from %s",
                event.type.value,
                event.source,
            )

    def get_history(
        self,
        learner_id: str | None = None,
        event_type: EventType | None = None,
        limit: int = 50,
    ) -> list[Event]:
        """查询事件历史，支持按学习者和事件类型过滤。"""
        events = self._event_history
        if learner_id:
            events = [e for e in events if e.learner_id == learner_id]
        if event_type:
            events = [e for e in events if e.type == event_type]
        return events[-limit:]

    def clear_history(self) -> None:
        """清空事件历史（用于测试）。"""
        self._event_history.clear()
