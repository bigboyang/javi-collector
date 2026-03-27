// Package rag — RAG 파이프라인 통합 테스트
//
// 테스트 범위:
//   - DocumentBuilder: ERROR/WARN/SLOW span에서 EmbedDocument 생성
//   - EmbedPipeline: Submit → flushBatch → Qdrant upsert (mock)
//   - RAGSearcher: 쿼리 임베딩 → Qdrant 검색 (mock)
//   - RAGGenerator: Retrieval + LLM Generation (mock LLM)
//   - OllamaLLMClient: Ollama /api/chat 요청/응답 파싱
//   - QdrantJanitor: TTL 필터 구성 검증
package rag

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kkc/javi-collector/internal/model"
)

// ---- Mock 구현 ----

type mockEmbedClient struct {
	dim int // 벡터 차원
}

func (m *mockEmbedClient) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		vec := make([]float32, m.dim)
		for j := range vec {
			vec[j] = float32(i+1) * 0.01 // 단순 더미 벡터
		}
		out[i] = vec
	}
	return out, nil
}

type mockLLMClient struct {
	response string
}

func (m *mockLLMClient) Generate(_ context.Context, _ string) (string, error) {
	return m.response, nil
}

// ---- DocumentBuilder 테스트 ----

func TestBuildFromSpan_ErrorSpan(t *testing.T) {
	b := &DocumentBuilder{SlowMs: 1000}
	sp := &model.SpanData{
		SpanID:        "abcdef1234567890",
		TraceID:       "trace001",
		ServiceName:   "payment-service",
		Name:          "POST /checkout",
		StatusCode:    2, // ERROR
		StatusMessage: "database connection refused",
		Kind:          2, // SERVER
		StartTimeNano: 0,
		EndTimeNano:   500_000_000, // 500ms
		ReceivedAtMs:  time.Now().UnixMilli(),
		Attributes: map[string]any{
			"http.request.method":     "POST",
			"http.response.status_code": "500",
			"http.route":              "/checkout",
			"exception.type":          "java.sql.SQLException",
			"exception.message":       "Connection refused to db:5432",
		},
	}

	doc := b.BuildFromSpan(sp, nil)
	if doc == nil {
		t.Fatal("BuildFromSpan returned nil for ERROR span")
	}
	if doc.ServiceName != "payment-service" {
		t.Errorf("ServiceName = %q, want payment-service", doc.ServiceName)
	}
	if !strings.Contains(doc.Text, "[SPAN ERROR]") {
		t.Errorf("Text missing [SPAN ERROR] label: %s", doc.Text[:100])
	}
	if !strings.Contains(doc.Text, "java.sql.SQLException") {
		t.Errorf("Text missing exception type")
	}
	if doc.Metadata["span_type"] != "span error" {
		t.Errorf("span_type = %q, want 'span error'", doc.Metadata["span_type"])
	}
	t.Logf("생성된 문서 텍스트:\n%s", doc.Text)
}

func TestBuildFromSpan_WarnSpan(t *testing.T) {
	b := &DocumentBuilder{SlowMs: 0} // SLOW 비활성화
	sp := &model.SpanData{
		SpanID:      "warn0001",
		ServiceName: "api-gateway",
		Name:        "GET /user",
		StatusCode:  1, // OK — 하지만 HTTP 404
		Kind:        2,
		Attributes: map[string]any{
			"http.request.method":       "GET",
			"http.response.status_code": "404",
		},
		ReceivedAtMs: time.Now().UnixMilli(),
	}

	doc := b.BuildFromSpan(sp, nil)
	if doc == nil {
		t.Fatal("BuildFromSpan returned nil for WARN span (HTTP 4xx)")
	}
	if !strings.Contains(doc.Text, "[SPAN WARN]") {
		t.Errorf("Text missing [SPAN WARN] label: %s", doc.Text)
	}
}

func TestBuildFromSpan_SlowSpan(t *testing.T) {
	b := &DocumentBuilder{SlowMs: 500}
	sp := &model.SpanData{
		SpanID:        "slow0001",
		ServiceName:   "order-service",
		Name:          "SELECT * FROM orders",
		StatusCode:    1,      // OK — 하지만 느림
		StartTimeNano: 0,
		EndTimeNano:   2_000_000_000, // 2000ms > 500ms threshold
		ReceivedAtMs:  time.Now().UnixMilli(),
		Attributes: map[string]any{
			"db.system":    "mysql",
			"db.statement": "SELECT * FROM orders WHERE user_id = ?",
		},
	}

	doc := b.BuildFromSpan(sp, nil)
	if doc == nil {
		t.Fatal("BuildFromSpan returned nil for SLOW span")
	}
	if !strings.Contains(doc.Text, "[SPAN SLOW]") {
		t.Errorf("Text missing [SPAN SLOW] label: %s", doc.Text)
	}
}

func TestBuildFromSpan_NormalSpan_ReturnsNil(t *testing.T) {
	b := &DocumentBuilder{SlowMs: 1000}
	sp := &model.SpanData{
		SpanID:        "ok0001",
		ServiceName:   "auth-service",
		Name:          "GET /ping",
		StatusCode:    1,
		StartTimeNano: 0,
		EndTimeNano:   100_000_000, // 100ms — 정상
		ReceivedAtMs:  time.Now().UnixMilli(),
	}

	doc := b.BuildFromSpan(sp, nil)
	if doc != nil {
		t.Errorf("BuildFromSpan should return nil for normal span, got: %s", doc.Text)
	}
}

// ---- EmbedPipeline 테스트 ----

func TestEmbedPipeline_SubmitAndFlush(t *testing.T) {
	// Qdrant mock 서버
	var upsertCalled bool
	qdrantServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/points") && r.Method == http.MethodPut {
			upsertCalled = true
		}
		// 컬렉션 생성 PUT도 처리
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"result":true,"status":"ok","time":0.001}`)
	}))
	defer qdrantServer.Close()

	embedClient := &mockEmbedClient{dim: 4}
	qdrant := NewQdrantClient(qdrantServer.URL, "test_collection")
	pipeline := NewEmbedPipeline(embedClient, qdrant, 100, 10, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	pipeline.Start(ctx)

	// Submit 3 documents
	for i := 0; i < 3; i++ {
		pipeline.Submit(&EmbedDocument{
			SpanID:      fmt.Sprintf("span%04d", i),
			TraceID:     "trace001",
			ServiceName: "test-service",
			TimestampMs: time.Now().UnixMilli(),
			Text:        fmt.Sprintf("[ERROR] test error span %d with exception details", i),
			Metadata:    map[string]string{"service_name": "test-service"},
		})
	}

	// flush interval 대기
	time.Sleep(300 * time.Millisecond)
	pipeline.Close()

	if !upsertCalled {
		t.Error("Qdrant upsert was not called")
	}
	t.Log("EmbedPipeline: Qdrant upsert 호출 확인")
}

// ---- payload 헬퍼 테스트 ----

func TestPayloadHelpers(t *testing.T) {
	m := map[string]any{
		"str_field":   "hello",
		"float_field": float64(1234567890123),
		"int64_field": int64(9876543210),
		"str_num":     "42",
	}

	if v := payloadStr(m, "str_field"); v != "hello" {
		t.Errorf("payloadStr str_field = %q, want hello", v)
	}
	if v := payloadStr(m, "missing"); v != "" {
		t.Errorf("payloadStr missing = %q, want empty", v)
	}
	if v := payloadInt64(m, "float_field"); v != 1234567890123 {
		t.Errorf("payloadInt64 float_field = %d, want 1234567890123", v)
	}
	if v := payloadInt64(m, "int64_field"); v != 9876543210 {
		t.Errorf("payloadInt64 int64_field = %d, want 9876543210", v)
	}
	if v := payloadInt64(m, "str_num"); v != 42 {
		t.Errorf("payloadInt64 str_num = %d, want 42", v)
	}
}

// ---- OllamaLLMClient 테스트 ----

func TestOllamaLLMClient_Generate(t *testing.T) {
	// Ollama mock 서버
	wantResponse := "근본 원인: 데이터베이스 연결 풀 소진. 권장 조치: 커넥션 풀 크기 증가 및 쿼리 타임아웃 설정 확인."
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/chat" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		// 요청 검증
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if body["stream"] == true {
			t.Error("stream should be false for sync generation")
		}
		if body["model"] == nil {
			t.Error("model field missing")
		}

		// 응답
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"model": "qwen2.5:3b",
			"message": map[string]any{
				"role":    "assistant",
				"content": wantResponse,
			},
			"done": true,
		})
	}))
	defer server.Close()

	client := NewOllamaLLMClient(server.URL, "qwen2.5:3b")
	result, err := client.Generate(context.Background(), "test prompt")
	if err != nil {
		t.Fatalf("Generate error: %v", err)
	}
	if result != wantResponse {
		t.Errorf("Generate = %q, want %q", result, wantResponse)
	}
	t.Logf("LLM 응답: %s", result)
}

// ---- RAGGenerator 테스트 ----

func TestRAGGenerator_Generate(t *testing.T) {
	// Qdrant mock (search endpoint)
	qdrantServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/search") {
			// 유사 사례 1건 반환
			json.NewEncoder(w).Encode(map[string]any{
				"result": []map[string]any{
					{
						"score": 0.89,
						"payload": map[string]any{
							"text":         "[SPAN ERROR] payment-service | POST /pay\nexception: TimeoutException",
							"trace_id":     "past_trace_001",
							"service_name": "payment-service",
							"timestamp_ms": float64(time.Now().Add(-24 * time.Hour).UnixMilli()),
						},
					},
				},
			})
		} else {
			fmt.Fprint(w, `{"result":true,"status":"ok"}`)
		}
	}))
	defer qdrantServer.Close()

	wantAnalysis := "결제 서비스 DB 타임아웃 — 커넥션 풀 설정 확인 필요"
	llm := &mockLLMClient{response: wantAnalysis}
	embedder := &mockEmbedClient{dim: 4}

	qdrant := NewQdrantClient(qdrantServer.URL, "test_collection")
	searcher := NewRAGSearcher(embedder, qdrant, 0.5)
	generator := NewRAGGenerator(searcher, llm)

	req := &SearchRequest{
		Query:       "payment service database connection timeout",
		ServiceName: "payment-service",
		Limit:       3,
	}

	result, err := generator.Generate(context.Background(), "[Error Rate Spike] payment-service/POST /pay", req)
	if err != nil {
		t.Fatalf("Generate error: %v", err)
	}
	if result != wantAnalysis {
		t.Errorf("Generate = %q, want %q", result, wantAnalysis)
	}
	t.Logf("RAG Generation 결과: %s", result)
}

// ---- buildRCAPrompt 테스트 ----

func TestBuildRCAPrompt(t *testing.T) {
	similar := []SearchResult{
		{
			TraceID:     "trace_past_1",
			ServiceName: "payment-service",
			Score:       0.92,
			Text:        "[SPAN ERROR] payment-service | POST /pay\nduration: 3000ms\nexception: TimeoutException",
		},
	}

	prompt := buildRCAPrompt("[Error] payment-service P95 응답시간 급증 Z=4.2", similar)

	if !strings.Contains(prompt, "현재 이상 감지") {
		t.Error("prompt missing '현재 이상 감지' section")
	}
	if !strings.Contains(prompt, "과거 유사 사례") {
		t.Error("prompt missing '과거 유사 사례' section")
	}
	if !strings.Contains(prompt, "0.92") {
		t.Error("prompt missing similarity score")
	}
	if !strings.Contains(prompt, "근본 원인") {
		t.Error("prompt missing RCA request")
	}
	t.Logf("생성된 프롬프트:\n%s", prompt)
}

// ---- spanIDToPointID 테스트 ----

func TestSpanIDToPointID(t *testing.T) {
	tests := []struct {
		spanID string
	}{
		{"abcdef1234567890"},
		{"1234567890abcdef"},
		{"abc123"},
	}
	for _, tt := range tests {
		id := spanIDToPointID(tt.spanID)
		// UUID 형식 검증: 8-4-4-4-12
		parts := strings.Split(id, "-")
		if len(parts) != 5 {
			t.Errorf("spanIDToPointID(%q) = %q: expected 5 parts, got %d", tt.spanID, id, len(parts))
		}
	}
}
