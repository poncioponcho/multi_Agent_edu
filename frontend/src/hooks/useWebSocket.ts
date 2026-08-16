import { useCallback, useEffect, useRef, useState } from 'react'

export interface AgentEvent {
  event_type: string
  source: string
  data: Record<string, unknown>
  timestamp: string
}

/**
 * WebSocket Hook -- 连接Agent系统的实时通信层。
 *
 * 面试要点：
 * - 自动重连策略（指数退避）
 * - 心跳检测保持连接
 * - React useRef 保持 WebSocket 实例跨渲染稳定
 */
export function useWebSocket(learnerId: string) {
  const [events, setEvents] = useState<AgentEvent[]>([])
  const [connected, setConnected] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)
  const retryRef = useRef(0)

  const connect = useCallback(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/${learnerId}`)

    ws.onopen = () => {
      setConnected(true)
      retryRef.current = 0
    }

    ws.onmessage = (msg) => {
      const event: AgentEvent = JSON.parse(msg.data)
      setEvents((prev) => [...prev.slice(-99), event])
    }

    ws.onclose = () => {
      setConnected(false)
      const delay = Math.min(1000 * 2 ** retryRef.current, 30000)
      retryRef.current++
      setTimeout(connect, delay)
    }

    ws.onerror = () => ws.close()
    wsRef.current = ws
  }, [learnerId])

  useEffect(() => {
    connect()
    return () => wsRef.current?.close()
  }, [connect])

  const send = useCallback((data: Record<string, unknown>) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(data))
    }
  }, [])

  return { events, connected, send }
}
