import React, { useEffect, useState } from 'react'
import 'katex/dist/katex.min.css'
import { useWebSocket, AgentEvent } from './hooks/useWebSocket'
import { renderRichText } from './utils/renderMath'

const AGENT_COLORS: Record<string, string> = {
  AssessmentAgent: '#3b82f6',
  TutorAgent: '#10b981',
  CurriculumAgent: '#f59e0b',
  HintAgent: '#8b5cf6',
  EngagementAgent: '#ef4444',
  api: '#6b7280',
}

// Agent 中文名（面试演示用，非技术观众也能看懂）
const AGENT_LABELS: Record<string, string> = {
  AssessmentAgent: '评估 Agent（BKT 掌握度）',
  TutorAgent: '教学 Agent（苏格拉底式）',
  CurriculumAgent: '课程 Agent（SM-2 排期）',
  HintAgent: '提示 Agent（分级提示）',
  EngagementAgent: '互动 Agent（节奏监控）',
}

// 知识点 key → 中文名（避免界面暴露内部 ID）
const KNOWLEDGE_NAMES: Record<string, string> = {
  arithmetic: '四则运算',
  fractions: '分数运算',
  algebraic_expr: '代数式',
  linear_eq_1: '一元一次方程',
  factoring: '因式分解',
  quadratic_eq: '一元二次方程',
  quadratic_func: '二次函数',
  pythagorean: '勾股定理',
  probability: '概率初步',
}

// 把文本中出现的知识点 key 替换为中文名
function localize(text: string): string {
  let result = text
  for (const [key, name] of Object.entries(KNOWLEDGE_NAMES)) {
    result = result.replace(new RegExp(`\\b${key}\\b`, 'g'), name)
  }
  return result
}

function EventCard({ event }: { event: AgentEvent }) {
  const color = AGENT_COLORS[event.source] || '#6b7280'
  return (
    <div style={{
      border: `2px solid ${color}`,
      borderRadius: 8,
      padding: 12,
      marginBottom: 8,
      background: `${color}11`,
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
        <strong style={{ color }}>{event.source}</strong>
        <span style={{ fontSize: 12, color: '#999' }}>{event.event_type}</span>
      </div>
      {!!event.data.response && (
        <p
          style={{ margin: 0, lineHeight: 1.7 }}
          dangerouslySetInnerHTML={{ __html: renderRichText(localize(String(event.data.response))) }}
        />
      )}
      {!!event.data.message && (
        <p
          style={{ margin: 0, lineHeight: 1.7 }}
          dangerouslySetInnerHTML={{ __html: renderRichText(localize(String(event.data.message))) }}
        />
      )}
      {event.data.mastery !== undefined && (
        <div style={{ marginTop: 4 }}>
          <span>掌握度: </span>
          <strong>{(Number(event.data.mastery) * 100).toFixed(0)}%</strong>
          {!!event.data.level && <span> ({String(event.data.level)})</span>}
        </div>
      )}
      {!!event.data.hint_text && (
        <p
          style={{ margin: '4px 0 0', fontStyle: 'italic', lineHeight: 1.7 }}
          dangerouslySetInnerHTML={{ __html: renderRichText(localize(String(event.data.hint_text))) }}
        />
      )}
    </div>
  )
}

export default function App() {
  const [learnerId] = useState('student_001')
  const { events, connected, send } = useWebSocket(learnerId)
  const [knowledgeId, setKnowledgeId] = useState('quadratic_eq')
  const [message, setMessage] = useState('')
  // 提问后 LLM 推理中：显示"思考中"指示，收到 Agent 回复后清除
  const [thinking, setThinking] = useState(false)
  // 每秒刷新一次，让 Agent 活跃状态随时间自动回落
  const [now, setNow] = useState(Date.now())
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(t)
  }, [])

  // 每个 Agent 最近一次发出事件的时间（10 秒内视为活跃）
  const lastActive: Record<string, number> = {}
  for (const e of events) {
    const t = new Date(e.timestamp).getTime()
    if (t > (lastActive[e.source] ?? 0)) lastActive[e.source] = t
  }
  const isActive = (source: string) => now - (lastActive[source] ?? 0) < 10000

  // 收到 Tutor/Hint 的回复事件即视为"思考结束"
  useEffect(() => {
    const last = events[events.length - 1]
    if (last && (last.source === 'TutorAgent' || last.source === 'HintAgent')) {
      setThinking(false)
    }
  }, [events])

  const handleSubmit = (isCorrect: boolean) => {
    send({
      action: 'submit',
      knowledge_id: knowledgeId,
      is_correct: isCorrect,
      time_spent_seconds: Math.floor(Math.random() * 60) + 10,
    })
  }

  const handleQuestion = () => {
    if (!message.trim()) return
    setThinking(true)
    send({ action: 'question', knowledge_id: knowledgeId, question: message })
    setMessage('')
    // 兜底：LLM 异常超时 60s 后自动清除指示（期间可能触发降级模板）
    setTimeout(() => setThinking(false), 60000)
  }

  return (
    <div style={{
      maxWidth: 900,
      margin: '0 auto',
      padding: 24,
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    }}>
      <style>{`
        @keyframes thinkingPulse {
          0%, 100% { opacity: 0.3; transform: scale(0.8); }
          50% { opacity: 1; transform: scale(1.2); }
        }
      `}</style>
      <header style={{ marginBottom: 24 }}>
        <h1 style={{ margin: 0 }}>多Agent智能教育系统</h1>
        <p style={{ color: '#666' }}>
          5-Agent Mesh + 事件驱动架构 | 学习者: {learnerId}
          <span style={{
            marginLeft: 12,
            padding: '2px 8px',
            borderRadius: 4,
            background: connected ? '#10b981' : '#ef4444',
            color: '#fff',
            fontSize: 12,
          }}>
            {connected ? '已连接' : '未连接'}
          </span>
        </p>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 24 }}>
        {/* 左侧：操作面板 */}
        <div>
          <h2>学习操作</h2>

          <div style={{ marginBottom: 16 }}>
            <label>知识点：</label>
            <select
              value={knowledgeId}
              onChange={(e) => setKnowledgeId(e.target.value)}
              style={{ padding: 8, borderRadius: 4, border: '1px solid #ddd', width: '100%' }}
            >
              <option value="arithmetic">四则运算</option>
              <option value="fractions">分数运算</option>
              <option value="algebraic_expr">代数式</option>
              <option value="linear_eq_1">一元一次方程</option>
              <option value="factoring">因式分解</option>
              <option value="quadratic_eq">一元二次方程</option>
              <option value="quadratic_func">二次函数</option>
              <option value="pythagorean">勾股定理</option>
              <option value="probability">概率初步</option>
            </select>
          </div>

          <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
            <button
              onClick={() => handleSubmit(true)}
              style={{
                flex: 1, padding: 12, borderRadius: 8,
                background: '#10b981', color: '#fff', border: 'none',
                cursor: 'pointer', fontSize: 16,
              }}
            >
              答对了
            </button>
            <button
              onClick={() => handleSubmit(false)}
              style={{
                flex: 1, padding: 12, borderRadius: 8,
                background: '#ef4444', color: '#fff', border: 'none',
                cursor: 'pointer', fontSize: 16,
              }}
            >
              答错了
            </button>
          </div>

          <div style={{ marginBottom: 16 }}>
            <textarea
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              placeholder="输入你的问题..."
              style={{
                width: '100%', padding: 8, borderRadius: 4,
                border: '1px solid #ddd', minHeight: 80, resize: 'vertical',
              }}
            />
            <button
              onClick={handleQuestion}
              style={{
                marginTop: 8, padding: '8px 16px', borderRadius: 8,
                background: '#3b82f6', color: '#fff', border: 'none',
                cursor: 'pointer', width: '100%',
              }}
            >
              提问
            </button>
          </div>

          <div style={{
            padding: 12, borderRadius: 8,
            background: '#f8fafc', border: '1px solid #e2e8f0',
          }}>
            <h3 style={{ margin: '0 0 8px' }}>Agent 状态</h3>
            {Object.entries(AGENT_COLORS).filter(([k]) => k !== 'api').map(([name, color]) => {
              const active = isActive(name)
              return (
                <div key={name} style={{
                  display: 'flex', alignItems: 'center', gap: 8,
                  marginBottom: 8, opacity: active ? 1 : 0.55,
                  padding: '4px 8px', borderRadius: 6,
                  background: active ? `${color}15` : 'transparent',
                  transition: 'all 0.4s',
                }}>
                  <div style={{
                    width: 12, height: 12, borderRadius: '50%',
                    background: active ? color : '#cbd5e1',
                    boxShadow: active ? `0 0 8px ${color}` : 'none',
                    transition: 'all 0.4s',
                  }} />
                  <span style={{ fontSize: 13 }}>{AGENT_LABELS[name] || name}</span>
                  <span style={{
                    fontSize: 12, marginLeft: 'auto',
                    color: active ? '#10b981' : '#94a3b8',
                    fontWeight: active ? 600 : 400,
                  }}>
                    {active ? '活跃' : '待命'}
                  </span>
                </div>
              )
            })}
          </div>
        </div>

        {/* 右侧：事件流 */}
        <div>
          <h2>Agent 事件流 ({events.length})</h2>
          <div style={{
            maxHeight: 'calc(100vh - 200px)',
            minHeight: 400,
            overflowY: 'auto',
            border: '1px solid #e2e8f0',
            borderRadius: 8,
            padding: 12,
          }}>
            {events.length === 0 ? (
              <p style={{ color: '#999', textAlign: 'center' }}>
                点击"答对了"或"答错了"触发Agent事件流
              </p>
            ) : (
              [...events].reverse().map((event, i) => (
                <EventCard key={i} event={event} />
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
