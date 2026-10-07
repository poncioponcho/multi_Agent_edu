"""
并发正确性测试 —— 与 Go 版 `golang/internal/model/learner_concurrency_test.go` 对应。

⚠️ Python 版与 Go 版的并发模型不同，**问题表现也不同，不能照搬结论**：

- **Go**：多线程真并行。`UpdateMastery` 的读-改-写在锁外执行会直接丢更新
  （实测 1600 次提交只记到 1540 次，丢 3.8%）。
- **Python**：asyncio 单线程 + `update_mastery` 是**同步函数**（内部无 await），
  协程无法在其执行中途抢占，所以**计数不会被覆盖**。

但 Python 侧仍有一个真实缺陷，且成因与 Go **同源** —— 返回值是内部可变对象的
**引用**而非快照：`update_mastery()` / `get_state()` 返回的就是
`self.knowledge_states[knowledge_id]` 这个对象本身。调用方（Agent）会跨多个
`await` 持有它，期间另一个协程对同一知识点做更新会**就地改写**它，于是后续
发出的事件携带的是**别人的状态**。

运行：
    cd python && ./venv/bin/python -m pytest tests/test_concurrency.py -v
"""

import asyncio

import pytest

from agents.assessment_agent import AssessmentAgent
from core.event_bus import Event, EventBus, EventType
from core.learner_model import LearnerModel

# ─────────────────────────────────────────────────────────────────────────────
# 根因：返回值是内部可变对象的引用，而不是快照
# ─────────────────────────────────────────────────────────────────────────────


def test_update_mastery_returns_independent_snapshot():
    """两次 update_mastery 的返回值必须互相独立。

    修复前：两次返回同一个对象，第一次的返回值会被第二次更新就地改写。
    """
    model = LearnerModel("L")

    first = model.update_mastery("kp", is_correct=True)
    first_mastery = first.mastery

    second = model.update_mastery("kp", is_correct=True)

    assert first is not second, (
        "两次 update_mastery 返回了同一个对象：返回值是内部可变对象的引用，"
        "调用方跨 await 读取会读到后续更新的结果"
    )
    assert first.mastery == first_mastery, "第一次的返回值被第二次更新就地改写了"
    assert second.mastery > first_mastery


def test_get_state_returns_independent_snapshot():
    """get_state 的返回值必须是一份快照，不被后续更新影响。"""
    model = LearnerModel("L")

    before = model.get_state("kp")
    assert before.attempts == 0
    initial_mastery = before.mastery

    model.update_mastery("kp", is_correct=True)

    assert before.attempts == 0, "get_state 的返回值被后续 update_mastery 改写了"
    assert before.mastery == initial_mastery, "get_state 的返回值被后续 update_mastery 改写了"


def test_get_weak_points_returns_snapshots():
    """get_weak_points 返回的也必须是快照（API 层会序列化它们）。"""
    model = LearnerModel("L")
    model.update_mastery("kp", is_correct=False)

    weak = model.get_weak_points(threshold=0.5)
    assert len(weak) == 1
    snapshot_mastery = weak[0].mastery

    model.update_mastery("kp", is_correct=True)

    assert weak[0].mastery == snapshot_mastery, (
        "get_weak_points 返回的是内部实时对象，列表在调用后被就地改写"
    )


def test_get_state_does_not_alias_internal_storage():
    """get_state 返回的对象不应与内部存储是同一个对象。"""
    model = LearnerModel("L")
    state = model.get_state("kp")
    assert state is not model.knowledge_states["kp"], (
        "get_state 把内部存储对象直接交给了调用方"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 表现层：并发提交时，事件必须携带「自己那次提交」的结果
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_assessment_event_reports_own_submission_under_concurrency():
    """两个并发提交，各自发出的 ASSESSMENT_COMPLETE 必须报告自己那次的 mastery。

    构造方式：给 MASTERY_UPDATED 挂一个会 `sleep` 的订阅者，强制第一次提交在
    emit 时让出事件循环，从而与第二次提交真正交错。

    修复前：两次 ASSESSMENT_COMPLETE 都报告第二次的 mastery（[m2, m2]）。
    修复后：分别报告 [m1, m2]。
    """
    bus = EventBus()
    models: dict[str, LearnerModel] = {}
    agent = AssessmentAgent(name="AssessmentAgent", event_bus=bus, learner_models=models)

    completes: list[float] = []

    async def slow_handler(event: Event) -> None:
        # 关键：让出事件循环，制造交错
        await asyncio.sleep(0.01)

    async def record_complete(event: Event) -> None:
        completes.append(event.data["mastery"])

    bus.subscribe(EventType.MASTERY_UPDATED, slow_handler)
    bus.subscribe(EventType.ASSESSMENT_COMPLETE, record_complete)

    async def submit() -> None:
        await agent.handle_event(
            Event(
                type=EventType.STUDENT_SUBMISSION,
                source="test",
                learner_id="L",
                data={"knowledge_id": "kp", "is_correct": True},
            )
        )

    await asyncio.gather(submit(), submit())

    # 用同样序列算出「第 1 次」和「第 2 次」更新后应有的 mastery
    ref = LearnerModel("ref")
    m1 = ref.update_mastery("kp", is_correct=True).mastery
    m2 = ref.update_mastery("kp", is_correct=True).mastery

    assert len(completes) == 2, f"期望 2 个 ASSESSMENT_COMPLETE，实际 {len(completes)}"
    assert sorted(completes) == pytest.approx(sorted([m1, m2])), (
        f"两次提交的 ASSESSMENT_COMPLETE 报告 mastery={sorted(completes)}，"
        f"期望 {sorted([m1, m2])}；若两次相同说明事件携带了另一次提交的状态"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 不变量：Python 侧计数不会丢 —— 但这依赖一个脆弱的前提
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_concurrent_submissions_preserve_exact_count():
    """8 个并发提交，attempts 必须精确等于 8。

    ⚠️ 这条通过的原因**不是**因为加了锁，而是因为 asyncio 是协作式调度、且
    `update_mastery` 内部没有任何 `await`，所以它不可被抢占。

    这个前提很脆弱：**一旦有人在 `update_mastery` 里加了 `await`**（例如为了
    每次更新都落库），计数就会开始丢失。本用例的价值就是把这个隐含前提钉成
    可执行的回归测试。
    """
    bus = EventBus()
    models: dict[str, LearnerModel] = {}
    agent = AssessmentAgent(name="AssessmentAgent", event_bus=bus, learner_models=models)

    async def submit() -> None:
        await agent.handle_event(
            Event(
                type=EventType.STUDENT_SUBMISSION,
                source="test",
                learner_id="L",
                data={"knowledge_id": "kp", "is_correct": True},
            )
        )

    await asyncio.gather(*[submit() for _ in range(8)])

    attempts = models["L"].get_state("kp").attempts
    assert attempts == 8, f"并发提交丢计数：attempts={attempts}，期望 8"
