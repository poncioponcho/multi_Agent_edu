"""
Agent 基类 -- 所有Agent的公共接口和行为。

每个Agent都：
1. 有一个名称和角色描述
2. 连接到EventBus，订阅感兴趣的事件
3. 可以发布事件通知其他Agent
4. 有自己的处理逻辑（子类实现）

面试要点：
- 模板方法模式：基类定义骨架，子类实现细节
- 依赖注入：EventBus通过构造函数注入
- 单一职责原则：每个Agent只关注自己的领域
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime

from core.event_bus import Event, EventBus, EventType
from core.learner_model import LearnerModel, KnowledgeState

logger = logging.getLogger(__name__)


class BaseAgent(ABC):
    """
    Agent 基类。

    子类需要实现：
    - subscribed_events: 返回自己订阅的事件类型列表
    - handle_event: 处理接收到的事件
    """

    def __init__(
        self,
        name: str,
        event_bus: EventBus,
        learner_models: dict[str, LearnerModel],
    ) -> None:
        self.name = name
        self.event_bus = event_bus
        self.learner_models = learner_models
        self._current_correlation_id: str | None = None
        self._register_handlers()
        logger.info("[%s] Agent initialized", self.name)

    def _register_handlers(self) -> None:
        """注册事件处理器到EventBus。"""
        for event_type in self.subscribed_events:
            self.event_bus.subscribe(event_type, self._wrapped_handle_event)
            logger.debug("[%s] Subscribed to %s", self.name, event_type.value)

    @property
    @abstractmethod
    def subscribed_events(self) -> list[EventType]:
        """子类声明自己订阅哪些事件。"""
        ...

    @abstractmethod
    async def handle_event(self, event: Event) -> None:
        """子类实现事件处理逻辑。"""
        ...

    async def _wrapped_handle_event(self, event: Event) -> None:
        """包装 handler，自动传递 correlation_id。"""
        self._current_correlation_id = event.correlation_id
        try:
            await self.handle_event(event)
        finally:
            self._current_correlation_id = None

    async def emit(self, event_type: EventType, learner_id: str, data: dict, correlation_id: str | None = None) -> None:
        """便捷方法：发布事件。"""
        event = Event(
            type=event_type,
            source=self.name,
            learner_id=learner_id,
            data=data,
            correlation_id=correlation_id or self._current_correlation_id,
        )
        await self.event_bus.publish(event)

    def get_learner_model(self, learner_id: str) -> LearnerModel:
        """获取学习者模型，不存在则创建。"""
        if learner_id not in self.learner_models:
            self.learner_models[learner_id] = LearnerModel(learner_id)
        return self.learner_models[learner_id]

    async def load_learner_from_db(self, learner_id: str) -> bool:
        """尝试从数据库加载学习者状态。"""
        from core.database import db_manager
        if not db_manager.is_available:
            return False
        data = await db_manager.load_learner(learner_id)
        if not data:
            return False
        model = LearnerModel(learner_id)
        model.total_interactions = data.get("total_interactions", 0)
        model.session_start = data.get("session_start", datetime.now())
        model.metadata = data.get("metadata", {})
        for kid, ks in data.get("knowledge_states", {}).items():
            model.knowledge_states[kid] = KnowledgeState(
                knowledge_id=kid,
                mastery=ks.get("mastery", 0.1),
                alpha=ks.get("alpha", 1.0),
                beta=ks.get("beta", 9.0),
                attempts=ks.get("attempts", 0),
                correct_count=ks.get("correct_count", 0),
                last_attempt=ks.get("last_attempt"),
                streak=ks.get("streak", 0),
            )
        self.learner_models[learner_id] = model
        return True

    async def save_learner_to_db(self, learner_id: str) -> bool:
        """保存学习者状态到数据库。"""
        from core.database import db_manager
        if not db_manager.is_available or learner_id not in self.learner_models:
            return False
        model = self.learner_models[learner_id]
        return await db_manager.save_learner(learner_id, model.to_dict())
