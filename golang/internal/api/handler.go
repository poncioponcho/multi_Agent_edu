package api

import (
	"encoding/json"
	"net/http"

	"github.com/multi-agent-education/golang/internal/agent"
	"github.com/multi-agent-education/golang/internal/eventbus"
	"github.com/multi-agent-education/golang/internal/model"
	"github.com/multi-agent-education/golang/internal/rag"
)

// SetupRouter 配置HTTP路由
func SetupRouter(bus *eventbus.EventBus, assessment *agent.AssessmentAgent, retriever *rag.BM25) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]interface{}{
			"status": "ok", "service": "multi-agent-education-go", "agents": 5,
		})
	})

	mux.HandleFunc("POST /api/v1/submit", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			LearnerID   string  `json:"learner_id"`
			KnowledgeID string  `json:"knowledge_id"`
			IsCorrect   bool    `json:"is_correct"`
			TimeSpent   float64 `json:"time_spent_seconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		bus.Publish(eventbus.Event{
			Type: eventbus.StudentSubmission, Source: "api",
			LearnerID: body.LearnerID,
			Data: map[string]interface{}{
				"knowledge_id":       body.KnowledgeID,
				"is_correct":         body.IsCorrect,
				"time_spent_seconds": body.TimeSpent,
			},
		})

		writeJSON(w, map[string]interface{}{"status": "processed", "learner_id": body.LearnerID})
	})

	mux.HandleFunc("POST /api/v1/question", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			LearnerID   string `json:"learner_id"`
			KnowledgeID string `json:"knowledge_id"`
			Question    string `json:"question"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		bus.Publish(eventbus.Event{
			Type: eventbus.StudentQuestion, Source: "api",
			LearnerID: body.LearnerID,
			Data: map[string]interface{}{
				"knowledge_id": body.KnowledgeID,
				"question":     body.Question,
			},
		})

		writeJSON(w, map[string]interface{}{"status": "processed"})
	})

	mux.HandleFunc("GET /api/v1/events/{learnerID}", func(w http.ResponseWriter, r *http.Request) {
		learnerID := r.PathValue("learnerID")
		events := bus.GetHistory(learnerID, 20)
		writeJSON(w, map[string]interface{}{"learner_id": learnerID, "events": events})
	})

	// 全链路追踪：按 correlation_id 回放完整事件链
	mux.HandleFunc("GET /api/v1/trace/{correlationID}", func(w http.ResponseWriter, r *http.Request) {
		trace := bus.GetTrace(r.PathValue("correlationID"))
		writeJSON(w, map[string]interface{}{"correlation_id": r.PathValue("correlationID"), "trace": trace})
	})

	// RAG 检索：按知识点名称检索教材片段
	mux.HandleFunc("GET /api/v1/retrieve", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("q")
		if query == "" {
			http.Error(w, "missing query param q", http.StatusBadRequest)
			return
		}
		topK := 3
		if retriever == nil {
			writeJSON(w, map[string]interface{}{"error": "retriever not configured"})
			return
		}
		hits := retriever.Search(query, topK)
		writeJSON(w, map[string]interface{}{"query": query, "hits": hits})
	})

	// 学习路径推荐：基于掌握度推荐可学知识点（前置已达标）
	mux.HandleFunc("GET /api/v1/next-topics/{learnerID}", func(w http.ResponseWriter, r *http.Request) {
		learnerID := r.PathValue("learnerID")
		m := assessment.GetModel(learnerID)
		mastery := make(map[string]float64)
		for _, id := range knowledgeIDs() {
			mastery[id] = m.GetState(id).Mastery
		}
		next := model.RecommendNext(mastery)
		writeJSON(w, map[string]interface{}{"learner_id": learnerID, "next_topics": next})
	})

	return withCORS(mux)
}

func knowledgeIDs() []string {
	ids := make([]string, 0, len(model.KnowledgeGraph))
	for _, n := range model.KnowledgeGraph {
		ids = append(ids, n.ID)
	}
	return ids
}

func writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
