//go:build integration

// RAG 실제 검색 데모 테스트
// 실행: go test -tags integration ./internal/rag/... -v -run TestRAGDemo -timeout 120s
package rag

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

const demoCollection = "rag_demo_search"

// TestRAGDemo는 현실적인 스팬 데이터로 실제 RAG Retrieval 동작을 시연한다.
// Embed 없이 직접 벡터를 삽입 (Ollama 없이 동작 확인).
func TestRAGDemo(t *testing.T) {
	qdrant := NewQdrantClient("http://localhost:6333", demoCollection)
	ctx := context.Background()

	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Log("  RAG Retrieval 실제 동작 데모 (8차원 벡터)")
	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// 1. 컬렉션 생성 (8차원)
	if err := qdrant.EnsureCollection(ctx, 8); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}

	// 2. 과거 스팬 데이터 삽입
	// 벡터: [error_weight, slow_weight, db_weight, http_weight, payment_weight, order_weight, auth_weight, infra_weight]
	now := time.Now().UnixMilli()
	points := []qdrantPoint{
		// ── payment-service 에러 사례 ──
		{
			ID:     spanIDToPointID("aa00000000000001"), // payment DB connection refused
			Vector: []float32{0.9, 0.1, 0.0, 0.8, 0.9, 0.0, 0.0, 0.1},
			Payload: map[string]any{
				"text":         "[SPAN ERROR] payment-service | POST /api/pay\nexception: java.net.ConnectException: Connection refused to db-primary:5432\nstatus: 500\nduration: 45ms",
				"service_name": "payment-service",
				"trace_id":     "trace_pay_001",
				"span_type":    "span error",
				"timestamp_ms": now - 86400000, // 1일 전
			},
		},
		{
			ID:     spanIDToPointID("aa00000000000002"), // payment DB timeout
			Vector: []float32{0.85, 0.2, 0.1, 0.75, 0.85, 0.0, 0.0, 0.1},
			Payload: map[string]any{
				"text":         "[SPAN ERROR] payment-service | POST /api/pay\nexception: java.util.concurrent.TimeoutException: DB query timed out after 30s\nstatus: 500\nduration: 30012ms",
				"service_name": "payment-service",
				"trace_id":     "trace_pay_002",
				"span_type":    "span error",
				"timestamp_ms": now - 172800000, // 2일 전
			},
		},
		{
			ID:     spanIDToPointID("aa00000000000003"), // payment slow query
			Vector: []float32{0.5, 0.7, 0.8, 0.3, 0.6, 0.0, 0.0, 0.2},
			Payload: map[string]any{
				"text":         "[SPAN SLOW] payment-service | SELECT * FROM transactions\ndb.system: postgresql\ndb.statement: SELECT t.* FROM transactions t WHERE t.user_id = ? ORDER BY created_at DESC\nduration: 8500ms",
				"service_name": "payment-service",
				"trace_id":     "trace_pay_003",
				"span_type":    "span slow",
				"timestamp_ms": now - 3600000, // 1시간 전
			},
		},
		// ── order-service 에러 사례 ──
		{
			ID:     spanIDToPointID("bb00000000000001"), // order DB connection error
			Vector: []float32{0.8, 0.2, 0.7, 0.4, 0.1, 0.9, 0.0, 0.1},
			Payload: map[string]any{
				"text":         "[SPAN ERROR] order-service | POST /api/orders\nexception: org.springframework.dao.DataAccessException: Unable to acquire JDBC Connection\nstatus: 503\nduration: 5000ms",
				"service_name": "order-service",
				"trace_id":     "trace_ord_001",
				"span_type":    "span error",
				"timestamp_ms": now - 7200000, // 2시간 전
			},
		},
		{
			ID:     spanIDToPointID("bb00000000000002"), // order slow join query
			Vector: []float32{0.3, 0.9, 0.85, 0.2, 0.1, 0.8, 0.0, 0.1},
			Payload: map[string]any{
				"text":         "[SPAN SLOW] order-service | SELECT orders\ndb.system: mysql\ndb.statement: SELECT * FROM orders o JOIN items i ON o.id = i.order_id WHERE o.status = 'PENDING'\nduration: 12000ms",
				"service_name": "order-service",
				"trace_id":     "trace_ord_002",
				"span_type":    "span slow",
				"timestamp_ms": now - 14400000, // 4시간 전
			},
		},
		// ── auth-service 에러 사례 ──
		{
			ID:     spanIDToPointID("cc00000000000001"), // auth 401
			Vector: []float32{0.7, 0.1, 0.0, 0.9, 0.0, 0.0, 0.95, 0.0},
			Payload: map[string]any{
				"text":         "[SPAN WARN] auth-service | POST /api/auth/login\nhttp.response.status_code: 401\nhttp.request.method: POST\nhttp.route: /api/auth/login\nduration: 120ms",
				"service_name": "auth-service",
				"trace_id":     "trace_auth_001",
				"span_type":    "span warn",
				"timestamp_ms": now - 1800000, // 30분 전
			},
		},
		// ── api-gateway infra 에러 사례 ──
		{
			ID:     spanIDToPointID("dd00000000000001"), // gateway redis timeout
			Vector: []float32{0.9, 0.1, 0.0, 0.2, 0.3, 0.3, 0.2, 0.95},
			Payload: map[string]any{
				"text":         "[SPAN ERROR] api-gateway | GET /api/*\nexception: io.netty.channel.ConnectTimeoutException: connection timed out: redis-cluster:6379\nstatus: 502\nduration: 5001ms",
				"service_name": "api-gateway",
				"trace_id":     "trace_gw_001",
				"span_type":    "span error",
				"timestamp_ms": now - 43200000, // 12시간 전
			},
		},
	}

	if err := qdrant.Upsert(ctx, points); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	t.Logf("✓ %d개 과거 스팬 적재 완료\n", len(points))

	// ──── 검색 시나리오 ────
	mockEmbed := &mockEmbedClient{dim: 8}
	searcher := NewRAGSearcher(mockEmbed, qdrant, 0.3) // score threshold 0.3

	scenarios := []struct {
		name        string
		query       string
		serviceName string
		limit       int
	}{
		{
			name:        "시나리오 1: payment-service DB 연결 오류 검색",
			query:       "payment service database connection refused error 500",
			serviceName: "payment-service",
			limit:       3,
		},
		{
			name:        "시나리오 2: order-service 느린 쿼리 검색",
			query:       "order service slow query timeout database",
			serviceName: "order-service",
			limit:       3,
		},
		{
			name:        "시나리오 3: 서비스 필터 없이 전체 에러 검색",
			query:       "database connection error timeout",
			serviceName: "",
			limit:       5,
		},
		{
			name:        "시나리오 4: auth 인증 실패 검색",
			query:       "authentication login unauthorized 401",
			serviceName: "auth-service",
			limit:       3,
		},
	}

	for _, sc := range scenarios {
		t.Logf("\n%s", sc.name)
		t.Logf("  쿼리: %q  (service_filter: %q)", sc.query, sc.serviceName)
		t.Log("  " + strings.Repeat("─", 60))

		results, err := searcher.Search(ctx, SearchRequest{
			Query:       sc.query,
			ServiceName: sc.serviceName,
			Limit:       sc.limit,
		})
		if err != nil {
			t.Errorf("  Search error: %v", err)
			continue
		}

		if len(results) == 0 {
			t.Log("  → 결과 없음 (threshold 초과)")
			continue
		}

		for i, r := range results {
			age := time.Since(time.UnixMilli(r.TimestampMs)).Round(time.Minute)
			textPreview := r.Text
			if len(textPreview) > 100 {
				textPreview = textPreview[:100] + "..."
			}
			t.Logf("  [%d] score=%.4f | %s | %s전", i+1, r.Score, r.ServiceName, age)
			t.Logf("       trace=%s", r.TraceID)
			t.Logf("       %s", textPreview)
		}
		t.Logf("  → 총 %d건 검색됨", len(results))
	}

	// ──── RAG 프롬프트 생성 시연 ────
	t.Log("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Log("  LLM 프롬프트 생성 시연 (buildRCAPrompt)")
	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	demoResults, _ := searcher.Search(ctx, SearchRequest{
		Query:       "payment database connection refused",
		ServiceName: "payment-service",
		Limit:       2,
	})

	anomalyDesc := "[P0 이상 감지] payment-service | POST /api/pay\nError Rate: 45.2% (Z=5.3, 기준선 2.1%)\nP95 Latency: 8,500ms (Z=4.1, 기준선 180ms)"
	prompt := buildRCAPrompt(anomalyDesc, demoResults)

	t.Log("\n생성된 RCA 프롬프트:")
	t.Log("┌" + strings.Repeat("─", 65) + "┐")
	for _, line := range strings.Split(prompt, "\n") {
		t.Logf("│ %-63s │", line)
	}
	t.Log("└" + strings.Repeat("─", 65) + "┘")

	// FormatForLLM 결과도 확인
	t.Log("\nFormatForLLM 결과 (컨텍스트 블록):")
	formatted := FormatForLLM("payment DB 연결 오류", demoResults)
	t.Log(formatted)

	// 정리
	if err := qdrant.deleteCollection(ctx); err != nil {
		t.Logf("cleanup: %v", err)
	}

	fmt.Println("\n✓ RAG Retrieval 데모 완료")
}
