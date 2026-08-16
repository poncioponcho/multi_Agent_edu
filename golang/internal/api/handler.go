package api

import (
	"encoding/json"
	"net/http"

	"github.com/multi-agent-education/golang/internal/agent"
	"github.com/multi-agent-education/golang/internal/eventbus"
)

// SetupRouter 配置HTTP路由
func SetupRouter(bus *eventbus.EventBus, assessment *agent.AssessmentAgent) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]interface{}{
			"status": "ok", "service": "multi-agent-education-go", "agents": 5,
		})
	})

	mux.HandleFunc("POST /api/v1/submit", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			LearnerID  string  `json:"learner_id"`
			KnowledgeID string `json:"knowledge_id"`
			IsCorrect  bool   `json:"is_correct"`
			TimeSpent  float64 `json:"time_spent_seconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		bus.Publish(eventbus.Event{
			Type: eventbus.StudentSubmission, Source: "api",
			LearnerID: body.LearnerID,
			Data: map[string]interface{}{
				"knowledge_id":      body.KnowledgeID,
				"is_correct":        body.IsCorrect,
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

	return withCORS(mux)
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
