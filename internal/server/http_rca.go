package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	jkafka "github.com/kkc/javi-collector/internal/kafka"
	"github.com/kkc/javi-collector/internal/store"
)

// handleDeployEvent는 CI/CD 파이프라인이 전송하는 배포 이벤트를 수신한다.
//
//	POST /v1/events/deploy
//	body: {"service_name":"...","version":"...","environment":"...","deployed_by":"...","timestamp_ms":0,"metadata":{}}
func (s *HTTPServer) handleDeployEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.deployProducer == nil && s.deploymentStore == nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "deployment events unavailable (KAFKA_ENABLED=false and ClickHouse disabled)"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	var ev jkafka.DeploymentEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if ev.ServiceName == "" || ev.Version == "" {
		http.Error(w, "service_name and version are required", http.StatusBadRequest)
		return
	}
	if ev.TimestampMs == 0 {
		ev.TimestampMs = time.Now().UnixMilli()
	}

	// Kafka 발행 (KAFKA_ENABLED=true 시)
	if s.deployProducer != nil {
		s.deployProducer.Publish(ev)
	}

	// GAP-04: ClickHouse 직접 기록 (RCA Engine 상관 분석용)
	if s.deploymentStore != nil {
		storeEv := store.DeploymentEvent{
			ID:          deploymentEventID(),
			ServiceName: ev.ServiceName,
			Version:     ev.Version,
			Environment: ev.Environment,
			DeployedBy:  ev.DeployedBy,
			DeployedAt:  time.UnixMilli(ev.TimestampMs),
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := s.deploymentStore.InsertEvent(ctx, storeEv); err != nil {
			slog.Warn("deployment event clickhouse insert failed", "service", ev.ServiceName, "err", err)
		}
	}

	slog.Info("deployment event received", "service", ev.ServiceName, "version", ev.Version, "env", ev.Environment)

	w.Header().Set("Content-Type", jsonContentType)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

// deploymentEventID는 배포 이벤트용 랜덤 16진수 ID를 생성한다.
func deploymentEventID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// queryRCAReports는 RCA 분석 결과를 반환한다.
//
//	GET /api/rca/reports?service=svc&severity=critical&from=<ms>&to=<ms>&limit=100
func (s *HTTPServer) queryRCAReports(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.rcaReports == nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "RCA reports unavailable (ClickHouse disabled)"})
		return
	}

	q := r.URL.Query()
	service := q.Get("service")
	severity := q.Get("severity")
	fromMs, _ := strconv.ParseInt(q.Get("from"), 10, 64)
	toMs, _ := strconv.ParseInt(q.Get("to"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	reports, err := s.rcaReports.QueryRCAReports(ctx, service, severity, fromMs, toMs, limit)
	if err != nil {
		slog.Warn("rca reports query failed", "err", err)
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	if reports == nil {
		reports = []store.RCAReport{}
	}
	w.Header().Set("Content-Type", jsonContentType)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"reports": reports,
		"total":   len(reports),
	})
}

// updateRCAFeedback는 RCA 보고서의 resolved 상태와 피드백을 업데이트한다.
//
//	POST /api/rca/feedback
//	body: {"id":"...","resolved":1,"feedback":"false positive — deploy skew"}
func (s *HTTPServer) updateRCAFeedback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.rcaReports == nil {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "RCA reports unavailable"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	var req struct {
		ID       string `json:"id"`
		Resolved uint8  `json:"resolved"`
		Feedback string `json:"feedback"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.ID == "" {
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "id field required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := s.rcaReports.UpdateRCAFeedback(ctx, req.ID, req.Resolved, req.Feedback); err != nil {
		slog.Warn("rca feedback update failed", "id", req.ID, "err", err)
		w.Header().Set("Content-Type", jsonContentType)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", jsonContentType)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
