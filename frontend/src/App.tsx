import React, { useState } from 'react'
import { useWebSocket, AgentEvent } from './hooks/useWebSocket'

const AGENT_COLORS: Record<string, string> = {
  AssessmentAgent: '#3b82f6',
  TutorAgent: '#10b981',
  CurriculumAgent: '#f59e0b',
  HintAgent: '#8b5cf6',
  EngagementAgent: '#ef4444',
  api: '#6b7280',
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
      {event.data.response && (
        <p style={{ margin: 0 }}>{String(event.data.response)}</p>
      )}
      {event.data.message && (
        <p style={{ margin: 0 }}>{String(event.data.message)}</p>
      )}
      {event.data.mastery !== undefined && (
        <div style={{ marginTop: 4 }}>
          <span>掌握度: </span>
          <strong>{(Number(event.data.mastery) * 100).toFixed(1)}%</strong>
          {event.data.level && <span> ({String(event.data.level)})</span>}
        </div>
      )}
      {event.data.hint_text && (
        <p style={{ margin: '4px 0 0', fontStyle: 'italic' }}>{String(event.data.hint_text)}</p>
      )}
    </div>
  )
}

export default function App() {
  const [learnerId] = useState('student_001')
  const { events, connected, send } = useWebSocket(learnerId)
  const [knowledgeId, setKnowledgeId] = useState('quadratic_eq')
  const [message, setMessage] = useState('')

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
    send({ action: 'question', knowledge_id: knowledgeId, question: message })
    setMessage('')
  }

  return (
    <div style={{
      maxWidth: 900,
      margin: '0 auto',
      padding: 24,
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    }}>
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
            {Object.entries(AGENT_COLORS).filter(([k]) => k !== 'api').map(([name, color]) => (
              <div key={name} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
                <div style={{ width: 12, height: 12, borderRadius: '50%', background: color }} />
                <span style={{ fontSize: 14 }}>{name}</span>
              </div>
            ))}
          </div>
        </div>

        {/* 右侧：事件流 */}
        <div>
          <h2>Agent 事件流 ({events.length})</h2>
          <div style={{
            maxHeight: 600,
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
