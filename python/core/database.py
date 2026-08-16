"""
数据库模型与持久化层 -- LearnerModel 持久化支持。

设计原则：
- 数据库是可选的：没有 PostgreSQL 时自动降级为内存模型
- 异步操作：所有 DB 操作使用 async SQLAlchemy
- 懒加载：LearnerModel 在首次访问时从 DB 加载
- 自动保存：mastery 更新后自动持久化

面试要点：
- 为什么用 async SQLAlchemy：FastAPI 全链路异步，避免阻塞事件循环
- 可选降级策略：开发/测试环境不依赖外部 DB，降低上手门槛
"""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Column,
    String,
    Float,
    Integer,
    DateTime,
    JSON,
    create_engine,
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base, sessionmaker

from config.settings import settings

logger = logging.getLogger(__name__)

Base = declarative_base()


class LearnerStateORM(Base):
    """学习者状态表。"""

    __tablename__ = "learner_states"

    learner_id = Column(String, primary_key=True)
    total_interactions = Column(Integer, default=0)
    session_start = Column(DateTime, default=datetime.now)
    metadata_json = Column(JSON, default=dict)  # 额外元数据
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class KnowledgeStateORM(Base):
    """知识点掌握状态表。"""

    __tablename__ = "knowledge_states"

    learner_id = Column(String, primary_key=True)
    knowledge_id = Column(String, primary_key=True)
    mastery = Column(Float, default=0.1)
    alpha = Column(Float, default=1.0)
    beta = Column(Float, default=9.0)
    attempts = Column(Integer, default=0)
    correct_count = Column(Integer, default=0)
    last_attempt = Column(DateTime, nullable=True)
    streak = Column(Integer, default=0)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class DatabaseManager:
    """数据库管理器：处理连接、初始化和 CRUD。"""

    def __init__(self) -> None:
        self.async_session: async_sessionmaker[AsyncSession] | None = None
        self._available = False
        self._init_error: str | None = None

    @property
    def is_available(self) -> bool:
        return self._available

    async def initialize(self) -> None:
        """初始化数据库连接和表结构。"""
        if not settings.database_url:
            logger.info("DatabaseManager: no DATABASE_URL, skipping persistence")
            return

        try:
            engine = create_async_engine(settings.database_url, echo=False)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            self.async_session = async_sessionmaker(engine, expire_on_commit=False)
            self._available = True
            logger.info("DatabaseManager initialized: %s", settings.database_url)
        except Exception as e:
            self._init_error = str(e)
            logger.warning("DatabaseManager init failed: %s. Will use in-memory mode.", e)

    async def load_learner(self, learner_id: str) -> dict[str, Any] | None:
        """从数据库加载学习者完整状态。"""
        if not self._available or not self.async_session:
            return None

        try:
            async with self.async_session() as session:
                from sqlalchemy import select

                # 加载 learner_state
                result = await session.execute(
                    select(LearnerStateORM).where(LearnerStateORM.learner_id == learner_id)
                )
                learner_orm = result.scalar_one_or_none()
                if not learner_orm:
                    return None

                # 加载所有 knowledge_states
                result = await session.execute(
                    select(KnowledgeStateORM).where(
                        KnowledgeStateORM.learner_id == learner_id
                    )
                )
                knowledge_orms = result.scalars().all()

                return {
                    "learner_id": learner_orm.learner_id,
                    "total_interactions": learner_orm.total_interactions,
                    "session_start": learner_orm.session_start,
                    "metadata": learner_orm.metadata_json or {},
                    "knowledge_states": {
                        k.knowledge_id: {
                            "mastery": k.mastery,
                            "alpha": k.alpha,
                            "beta": k.beta,
                            "attempts": k.attempts,
                            "correct_count": k.correct_count,
                            "last_attempt": k.last_attempt,
                            "streak": k.streak,
                        }
                        for k in knowledge_orms
                    },
                }
        except Exception:
            logger.exception("[DB] load_learner failed for %s", learner_id)
            return None

    async def save_learner(self, learner_id: str, state_dict: dict[str, Any]) -> bool:
        """保存学习者完整状态到数据库。"""
        if not self._available or not self.async_session:
            return False

        try:
            async with self.async_session() as session:
                from sqlalchemy import select
                from sqlalchemy.dialects.postgresql import insert as pg_insert

                # Upsert learner_state
                stmt = (
                    pg_insert(LearnerStateORM)
                    .values(
                        learner_id=learner_id,
                        total_interactions=state_dict.get("total_interactions", 0),
                        session_start=state_dict.get("session_start", datetime.now()),
                        metadata_json=state_dict.get("metadata", {}),
                        updated_at=datetime.now(),
                    )
                    .on_conflict_do_update(
                        index_elements=["learner_id"],
                        set_={
                            "total_interactions": state_dict.get("total_interactions", 0),
                            "metadata_json": state_dict.get("metadata", {}),
                            "updated_at": datetime.now(),
                        },
                    )
                )
                await session.execute(stmt)

                # Upsert knowledge_states
                knowledge_states = state_dict.get("knowledge_states", {})
                for kid, ks in knowledge_states.items():
                    stmt = (
                        pg_insert(KnowledgeStateORM)
                        .values(
                            learner_id=learner_id,
                            knowledge_id=kid,
                            mastery=ks.get("mastery", 0.1),
                            alpha=ks.get("alpha", 1.0),
                            beta=ks.get("beta", 9.0),
                            attempts=ks.get("attempts", 0),
                            correct_count=ks.get("correct_count", 0),
                            last_attempt=ks.get("last_attempt"),
                            streak=ks.get("streak", 0),
                            updated_at=datetime.now(),
                        )
                        .on_conflict_do_update(
                            index_elements=["learner_id", "knowledge_id"],
                            set_={
                                "mastery": ks.get("mastery", 0.1),
                                "alpha": ks.get("alpha", 1.0),
                                "beta": ks.get("beta", 9.0),
                                "attempts": ks.get("attempts", 0),
                                "correct_count": ks.get("correct_count", 0),
                                "last_attempt": ks.get("last_attempt"),
                                "streak": ks.get("streak", 0),
                                "updated_at": datetime.now(),
                            },
                        )
                    )
                    await session.execute(stmt)

                await session.commit()
                return True
        except Exception:
            logger.exception("[DB] save_learner failed for %s", learner_id)
            return False


# 全局实例
db_manager = DatabaseManager()
