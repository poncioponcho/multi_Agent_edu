#!/bin/bash
# ============================================================
# 多Agent智能教育系统 - 面试演示一键脚本
#
# 用法:
#   ./demo.sh start     # 干净启动全部服务（自动清理旧进程）
#   ./demo.sh stop      # 停止全部服务
#   ./demo.sh restart   # 重启（演示前拿干净状态用这个）
#   ./demo.sh status    # 查看各服务运行状态
#   ./demo.sh logs      # 查看各服务最近日志
#
# 服务与端口:
#   Python 后端  http://localhost:8000   (REST + WebSocket，前端数据源)
#   React 前端   http://localhost:3000   (浏览器打开这个)
#   Go 后端      http://localhost:8081    (REST + 链路回放，curl 演示用)
#
# LLM 配置:
#   编辑项目根目录 .env 的 OPENAI_API_KEY（支持任意 OpenAI 兼容服务，
#   配合 OPENAI_BASE_URL / OPENAI_MODEL 可接 DeepSeek/GLM/Kimi 等）。
#   key 未配置时自动降级为模板回复，服务照常可用。
# ============================================================
set -u

ROOT="$(cd "$(dirname "$0")" && pwd)"
RUN=/tmp/edu-demo
mkdir -p "$RUN"

PY_LOG="$RUN/python.log";  PY_PID="$RUN/python.pid"
FE_LOG="$RUN/frontend.log"; FE_PID="$RUN/frontend.pid"
GO_LOG="$RUN/go.log";       GO_PID="$RUN/go.pid"
GO_BIN="$RUN/edu-go-server"

# 加载 .env 并导出（Go 端靠环境变量拿 LLM 配置；Python pydantic 自行读取）
load_env() {
  if [ -f "$ROOT/.env" ]; then
    set -a; . "$ROOT/.env"; set +a
  fi
}

llm_mode() {
  load_env  # 与服务启动时读到的一致：.env 优先于 shell 环境变量
  if [ -z "${OPENAI_API_KEY:-}" ] || [ "$OPENAI_API_KEY" = "your-openai-api-key-here" ]; then
    echo "模板降级模式（.env 未配置有效 key）"
  else
    echo "LLM 已配置（model=${OPENAI_MODEL:-gpt-4o}，base=${OPENAI_BASE_URL:-api.openai.com}）"
  fi
}

stop_all() {
  for f in "$PY_PID" "$FE_PID" "$GO_PID"; do
    if [ -f "$f" ]; then
      kill "$(cat "$f")" 2>/dev/null
      rm -f "$f"
    fi
  done
  # 兜底：清理端口上的残留进程（本脚本专用端口）
  for port in 3000 8000 8081; do
    pids=$(lsof -t -i ":$port" 2>/dev/null)
    [ -n "$pids" ] && kill $pids 2>/dev/null
  done
  sleep 1
}

wait_healthy() {  # $1=url $2=最长等待秒数
  local i=0
  while [ $i -lt "$2" ]; do
    curl -s -m 2 -o /dev/null "$1" && return 0
    sleep 1; i=$((i + 1))
  done
  return 1
}

start_all() {
  stop_all
  load_env

  # Python 后端
  (cd "$ROOT/python" && nohup venv/bin/python -m uvicorn api.main:app --port 8000 \
    > "$PY_LOG" 2>&1 &)

  # React 前端（Vite dev server）
  (cd "$ROOT/frontend" && nohup npm run dev > "$FE_LOG" 2>&1 &)

  # Go 后端（先编译成二进制，便于干净的进程管理）
  (cd "$ROOT/golang" && go build -o "$GO_BIN" ./cmd/main.go \
    && nohup "$GO_BIN" > "$GO_LOG" 2>&1 &)

  echo -n "启动中"
  wait_healthy http://localhost:8000/api/v1/health 25 && py_ok=✅ || py_ok=❌
  wait_healthy http://localhost:3000 20 && fe_ok=✅ || fe_ok=❌
  wait_healthy http://localhost:8081/api/v1/health 25 && go_ok=✅ || go_ok=❌

  # 通过监听端口记录实际 PID（npm/go build 会产生中间进程）
  lsof -t -i :8000 -sTCP:LISTEN 2>/dev/null | head -1 > "$PY_PID"
  lsof -t -i :3000 -sTCP:LISTEN 2>/dev/null | head -1 > "$FE_PID"
  lsof -t -i :8081 -sTCP:LISTEN 2>/dev/null | head -1 > "$GO_PID"

  echo ""
  echo "──────────────────────────────────────────────"
  echo " 服务状态    Python$py_ok  前端$fe_ok  Go$go_ok"
  echo " 浏览器打开  http://localhost:3000"
  echo " LLM        $(llm_mode)"
  echo " 日志目录    $RUN/（./demo.sh logs 查看）"
  echo "──────────────────────────────────────────────"
}

status_all() {
  check() {  # $1=名称 $2=pid文件 $3=端口
    local state="❌ 未运行"
    if [ -f "$2" ] && kill -0 "$(cat "$2")" 2>/dev/null; then state="✅ 运行中"; fi
    printf "  %-8s %s (端口 %s)\n" "$1" "$state" "$3"
  }
  echo "── 服务状态 ──"
  check Python "$PY_PID" 8000
  check 前端  "$FE_PID" 3000
  check Go    "$GO_PID" 8081
  echo "  LLM     $(llm_mode)"
}

show_logs() {
  for f in "$PY_LOG" "$FE_LOG" "$GO_LOG"; do
    echo "════ $f ════"
    tail -n 20 "$f" 2>/dev/null || echo "(暂无)"
  done
}

case "${1:-start}" in
  start)   start_all ;;
  stop)    stop_all; echo "全部服务已停止" ;;
  restart) start_all ;;
  status)  status_all ;;
  logs)    show_logs ;;
  *) echo "用法: $0 {start|stop|restart|status|logs}"; exit 1 ;;
esac
