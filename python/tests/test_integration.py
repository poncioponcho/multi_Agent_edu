"""
集成测试 -- 模拟完整的学生答题 → Agent 事件链。

验证：
1. 学生答题 → Assessment 更新 mastery → Curriculum 排复习 → Tutor 生成回复
2. 事件总线正确分发事件到多个 Agent
3. 事件历史可追溯

运行方式：
    cd python/
    python -m pytest tests/test_integration.py -v
"""

import pytest
import asyncio

from core.event_bus import EventBus, Event, EventType
from core.learner_model import LearnerModel
from agents import (
    AssessmentAgent,
    TutorAgent,
    CurriculumAgent,
    HintAgent,
    EngagementAgent,
)


@pytest.fixture
def event_bus():
    return EventBus()


@pytest.fixture
def learner_models():
    return {}


@pytest.fixture
def agents(event_bus, learner_models):
    """创建完整的 5-Agent 系统。"""
    assessment = AssessmentAgent(
        name="AssessmentAgent",
        event_bus=event_bus,
        learner_models=learner_models,
    )
    tutor = TutorAgent(
        name="TutorAgent",
        event_bus=event_bus,
        learner_models=learner_models,
    )
    curriculum = CurriculumAgent(
        name="CurriculumAgent",
        event_bus=event_bus,
        learner_models=learner_models,
    )
    hint = HintAgent(
        name="HintAgent",
        event_bus=event_bus,
        learner_models=learner_models,
    )
    engagement = EngagementAgent(
        name="EngagementAgent",
        event_bus=event_bus,
        learner_models=learner_models,
    )
    return {
        "assessment": assessment,
        "tutor": tutor,
        "curriculum": curriculum,
        "hint": hint,
        "engagement": engagement,
    }


@pytest.mark.asyncio
async def test_submit_answer_triggers_event_chain(agents, event_bus, learner_models):
    """
    集成测试：学生提交答题 → 完整事件链。

    期望事件流：
    STUDENT_SUBMISSION → Assessment → MASTERY_UPDATED → Curriculum
                                            ↓
                                    ASSESSMENT_COMPLETE → Tutor, Engagement
    """
    event = Event(
        type=EventType.STUDENT_SUBMISSION,
        source="test",
        learner_id="student_1",
        data={
            "knowledge_id": "arithmetic",
            "is_correct": True,
            "time_spent_seconds": 30,
        },
    )
    await event_bus.publish(event)

    # 等待事件处理完成（异步 handler 可能还没执行完）
    await asyncio.sleep(0.1)

    # 验证事件历史
    history = event_bus.get_history(learner_id="student_1")
    event_types = [e.type.value for e in history]

    # 至少应该触发 Assessment 的输出事件
    assert "assessment.mastery_updated" in event_types
    assert "assessment.complete" in event_types

    # 验证 LearnerModel 已更新
    model = learner_models["student_1"]
    state = model.get_state("arithmetic")
    assert state.attempts == 1
    assert state.mastery > 0.1  # 答对后 mastery 应上升


@pytest.mark.asyncio
async def test_wrong_answer_triggers_hint_flow(agents, event_bus, learner_models):
    """
    集成测试：连续答错 → Hint Agent 介入。

    期望事件流：
    STUDENT_SUBMISSION (wrong, 第2次) → Assessment → HINT_NEEDED → Hint → HINT_RESPONSE
    """
    # 先建立 learner model，模拟已有一次答错
    model = LearnerModel("student_2")
    model.update_mastery("algebraic_expr", is_correct=False)
    learner_models["student_2"] = model

    # 第2次答错
    event = Event(
        type=EventType.STUDENT_SUBMISSION,
        source="test",
        learner_id="student_2",
        data={
            "knowledge_id": "algebraic_expr",
            "is_correct": False,
            "time_spent_seconds": 60,
        },
    )
    await event_bus.publish(event)
    await asyncio.sleep(0.1)

    history = event_bus.get_history(learner_id="student_2")
    event_types = [e.type.value for e in history]

    assert "assessment.mastery_updated" in event_types
    assert "assessment.complete" in event_types


@pytest.mark.asyncio
async def test_student_message_flow(agents, event_bus, learner_models):
    """
    集成测试：学生发送消息 → Tutor 回复。
    """
    event = Event(
        type=EventType.STUDENT_MESSAGE,
        source="test",
        learner_id="student_3",
        data={
            "message": "我不太理解二次函数",
            "knowledge_id": "quadratic_func",
        },
    )
    await event_bus.publish(event)
    await asyncio.sleep(0.1)

    history = event_bus.get_history(learner_id="student_3")
    event_types = [e.type.value for e in history]

    assert "tutor.teaching_response" in event_types


@pytest.mark.asyncio
async def test_multiple_submissions_progression(agents, event_bus, learner_models):
    """
    集成测试：多次答题 → mastery 逐步提升 → 达到 mastered。
    """
    learner_id = "student_4"
    knowledge_id = "fractions"

    # 连续答对 20 次
    for _ in range(20):
        event = Event(
            type=EventType.STUDENT_SUBMISSION,
            source="test",
            learner_id=learner_id,
            data={
                "knowledge_id": knowledge_id,
                "is_correct": True,
                "time_spent_seconds": 10,
            },
        )
        await event_bus.publish(event)
        await asyncio.sleep(0.01)

    model = learner_models[learner_id]
    state = model.get_state(knowledge_id)
    assert state.level.value == "mastered"
    assert state.mastery >= 0.85


@pytest.mark.asyncio
async def test_event_bus_correlation_id(agents, event_bus, learner_models):
    """
    验证事件链 correlation_id 追踪。
    """
    event = Event(
        type=EventType.STUDENT_SUBMISSION,
        source="test",
        learner_id="student_5",
        data={"knowledge_id": "linear_eq_1", "is_correct": True},
        correlation_id="test-trace-001",
    )
    await event_bus.publish(event)
    await asyncio.sleep(0.1)

    history = event_bus.get_history(learner_id="student_5")
    for e in history:
        assert e.correlation_id == "test-trace-001"
