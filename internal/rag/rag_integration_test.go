//go:build integration

// Package rag — Qdrant 실제 서버 통합 테스트
// 실행 방법: go test -tags integration ./internal/rag/... -v -timeout 60s
//
// 사전 조건:
//   - Qdrant: http://localhost:6333 (docker run -d -p 6333:6333 qdrant/qdrant)
package rag

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

const testCollection = "rag_integration_test"

// TestQdrantIntegration_UpsertAndSearch는 실제 Qdrant 서버로 upsert → search를 검증한다.
func TestQdrantIntegration_UpsertAndSearch(t *testing.T) {
	qdrant := NewQdrantClient("http://localhost:6333", testCollection)
	ctx := context.Background()

	// 1. 컬렉션 생성 (4차원 테스트용)
	t.Log("Step 1: EnsureCollection")
	if err := qdrant.EnsureCollection(ctx, 4); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}

	// 2. 테스트 포인트 upsert
	t.Log("Step 2: Upsert 3 points")
	now := time.Now().UnixMilli()
	points := []qdrantPoint{
		{
			ID:     spanIDToPointID("aabb000000000001"),
			Vector: []float32{0.9, 0.1, 0.05, 0.05},
			Payload: map[string]any{
				"text":         "[SPAN ERROR] payment-service | POST /pay\nexception: TimeoutException\nduration: 3200ms",
				"service_name": "payment-service",
				"trace_id":     "trace_payment_001",
				"span_type":    "span error",
				"timestamp_ms": now,
			},
		},
		{
			ID:     spanIDToPointID("aabb000000000002"),
			Vector: []float32{0.85, 0.15, 0.05, 0.0},
			Payload: map[string]any{
				"text":         "[SPAN ERROR] payment-service | POST /pay\nexception: ConnectionRefused\nduration: 100ms",
				"service_name": "payment-service",
				"trace_id":     "trace_payment_002",
				"span_type":    "span error",
				"timestamp_ms": now - 3600000, // 1시간 전
			},
		},
		{
			ID:     spanIDToPointID("aabb000000000003"),
			Vector: []float32{0.1, 0.9, 0.05, 0.05},
			Payload: map[string]any{
				"text":         "[SPAN SLOW] order-service | SELECT orders\nduration: 5000ms",
				"service_name": "order-service",
				"trace_id":     "trace_order_001",
				"span_type":    "span slow",
				"timestamp_ms": now - 7200000, // 2시간 전
			},
		},
	}

	if err := qdrant.Upsert(ctx, points); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	t.Logf("  Upserted %d points", len(points))

	// 3. 유사도 검색
	t.Log("Step 3: Vector search (similar to payment timeout)")
	embedder := &mockEmbedClient{dim: 4}
	searcher := NewRAGSearcher(embedder, qdrant, 0.5)

	// payment-service 에러와 유사한 벡터 검색
	results, err := searcher.Search(ctx, SearchRequest{
		Query:       "payment service timeout error",
		ServiceName: "payment-service",
		Limit:       5,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	t.Logf("  검색 결과 %d건:", len(results))
	for i, r := range results {
		t.Logf("  [%d] score=%.4f service=%s trace=%s", i+1, r.Score, r.ServiceName, r.TraceID)
		t.Logf("      text=%q", r.Text[:min(80, len(r.Text))])
	}

	if len(results) == 0 {
		t.Error("Search returned no results — expected at least 1 payment-service result")
	}

	// 4. TTL 기반 삭제 테스트 (JanitorRunOnce)
	t.Log("Step 4: Janitor — delete points older than 90 minutes")
	janitor := NewQdrantJanitor(qdrant, 0, 0) // 기본값: 30일 retention
	// cutoff를 60분 후로 설정해 모든 포인트 삭제 (테스트용)
	cutoffMs := now + 60*60*1000 // 미래 시각 → 모든 포인트 삭제
	filter := map[string]any{
		"must": []map[string]any{
			{"key": "timestamp_ms", "range": map[string]any{"lt": cutoffMs}},
		},
	}
	if err := janitor.qdrant.DeleteByFilter(ctx, filter); err != nil {
		t.Fatalf("DeleteByFilter: %v", err)
	}
	t.Log("  DeleteByFilter 완료")

	// 5. 삭제 후 검색 결과 0건 확인
	t.Log("Step 5: Verify all points deleted")
	results2, err := searcher.Search(ctx, SearchRequest{
		Query: "payment timeout",
		Limit: 5,
	})
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	t.Logf("  삭제 후 검색 결과: %d건 (expected 0)", len(results2))
	if len(results2) != 0 {
		t.Errorf("expected 0 results after delete, got %d", len(results2))
	}

	// 6. 컬렉션 정리
	t.Log("Step 6: Cleanup — delete test collection")
	if err := qdrant.deleteCollection(ctx); err != nil {
		t.Logf("  (cleanup warning: %v)", err)
	}
}

// deleteCollection은 테스트 후 컬렉션을 삭제한다.
func (q *QdrantClient) deleteCollection(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		q.endpoint+"/collections/"+q.collection, nil)
	if err != nil {
		return err
	}
	resp, err := q.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("delete collection: status %d", resp.StatusCode)
	}
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
