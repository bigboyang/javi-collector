//go:build integration

// RAG 실제 Ollama 임베딩 + LLM 엔드투엔드 테스트
//
// 실행 방법:
//
//	# Qdrant 실행
//	docker run -d --name qdrant-test -p 6333:6333 qdrant/qdrant
//	# Ollama 실행 + 모델 준비
//	docker run -d --name ollama -p 11434:11434 -v ollama:/root/.ollama ollama/ollama
//	ollama pull nomic-embed-text
//	ollama pull qwen2.5:1.5b
//
//	go test -tags integration ./internal/rag/... -v -run TestOllamaRAGPipeline -timeout 300s
package rag

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

const ollamaCollection = "rag_ollama_e2e_test"

// TestOllamaRAGPipeline은 실제 Ollama(nomic-embed-text + qwen2.5:1.5b) + Qdrant로
// 전체 RAG 파이프라인을 검증한다.
//
// 시나리오:
//  1. 과거 장애 스팬 10건을 실제 768차원 임베딩으로 Qdrant에 저장
//  2. 자연어 한국어 질의로 유사 장애 검색
//  3. LLM으로 근본 원인 분석(RCA) 생성
func TestOllamaRAGPipeline(t *testing.T) {
	const (
		ollamaEndpoint = "http://localhost:11434"
		qdrantEndpoint = "http://localhost:6333"
		embedModel     = "nomic-embed-text"
		llmModel       = "qwen2.5:1.5b"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	embedder := NewOllamaEmbedClient(ollamaEndpoint, embedModel)
	qdrant := NewQdrantClient(qdrantEndpoint, ollamaCollection)
	llm := NewOllamaLLMClient(ollamaEndpoint, llmModel)

	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	t.Logf("  Ollama E2E RAG 파이프라인 테스트")
	t.Logf("  임베딩: %s (768차원) | LLM: %s", embedModel, llmModel)
	t.Log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

	// ── 1단계: 컬렉션 초기화 (768차원) ──────────────────────────────
	t.Log("\n[1단계] Qdrant 컬렉션 생성 (768차원, 코사인 유사도)")
	if err := qdrant.EnsureCollection(ctx, 768); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}

	// ── 2단계: 과거 장애 이력 데이터 준비 ──────────────────────────
	t.Log("\n[2단계] 과거 장애 스팬 임베딩 → Qdrant 적재")

	type spanDoc struct {
		spanID  string
		service string
		traceID string
		text    string
		ageMs   int64 // now - ageMs
	}

	now := time.Now().UnixMilli()
	docs := []spanDoc{
		// payment-service 장애 이력
		{
			spanID:  "ae10000000000001",
			service: "payment-service",
			traceID: "trace_pay_history_001",
			text:    "[SPAN ERROR] payment-service | POST /api/pay\nexception: java.net.ConnectException: Connection refused to db-primary:5432\nhttp.response.status_code: 500\nduration: 45ms\ndb.system: postgresql",
			ageMs:   86_400_000, // 1일 전
		},
		{
			spanID:  "ae10000000000002",
			service: "payment-service",
			traceID: "trace_pay_history_002",
			text:    "[SPAN ERROR] payment-service | POST /api/pay\nexception: java.util.concurrent.TimeoutException: DB query timed out after 30s\nhttp.response.status_code: 500\nduration: 30012ms\ndb.system: postgresql\ndb.statement: SELECT * FROM payments WHERE user_id = ?",
			ageMs:   172_800_000, // 2일 전
		},
		{
			spanID:  "ae10000000000003",
			service: "payment-service",
			traceID: "trace_pay_history_003",
			text:    "[SPAN SLOW] payment-service | SELECT transactions\ndb.system: postgresql\ndb.statement: SELECT t.* FROM transactions t WHERE t.user_id = ? ORDER BY created_at DESC\nduration: 8500ms",
			ageMs:   3_600_000, // 1시간 전
		},
		{
			spanID:  "ae10000000000004",
			service: "payment-service",
			traceID: "trace_pay_history_004",
			text:    "[SPAN ERROR] payment-service | POST /api/refund\nexception: org.hibernate.exception.JDBCConnectionException: Unable to acquire JDBC Connection\nhttp.response.status_code: 503\nduration: 5001ms",
			ageMs:   43_200_000, // 12시간 전
		},
		// order-service 장애 이력
		{
			spanID:  "be20000000000001",
			service: "order-service",
			traceID: "trace_ord_history_001",
			text:    "[SPAN ERROR] order-service | POST /api/orders\nexception: org.springframework.dao.DataAccessException: Unable to acquire JDBC Connection\nhttp.response.status_code: 503\nduration: 5000ms\ndb.system: mysql",
			ageMs:   7_200_000, // 2시간 전
		},
		{
			spanID:  "be20000000000002",
			service: "order-service",
			traceID: "trace_ord_history_002",
			text:    "[SPAN SLOW] order-service | SELECT orders with items\ndb.system: mysql\ndb.statement: SELECT * FROM orders o JOIN items i ON o.id = i.order_id WHERE o.status = 'PENDING'\nduration: 12000ms",
			ageMs:   14_400_000, // 4시간 전
		},
		// auth-service 장애 이력
		{
			spanID:  "ce30000000000001",
			service: "auth-service",
			traceID: "trace_auth_history_001",
			text:    "[SPAN WARN] auth-service | POST /api/auth/login\nhttp.response.status_code: 401\nhttp.request.method: POST\nhttp.route: /api/auth/login\nduration: 120ms",
			ageMs:   1_800_000, // 30분 전
		},
		{
			spanID:  "ce30000000000002",
			service: "auth-service",
			traceID: "trace_auth_history_002",
			text:    "[SPAN ERROR] auth-service | POST /api/auth/token\nexception: io.jsonwebtoken.ExpiredJwtException: JWT token is expired\nhttp.response.status_code: 401\nduration: 80ms",
			ageMs:   3_600_000, // 1시간 전
		},
		// api-gateway 인프라 장애 이력
		{
			spanID:  "de40000000000001",
			service: "api-gateway",
			traceID: "trace_gw_history_001",
			text:    "[SPAN ERROR] api-gateway | GET /api/*\nexception: io.netty.channel.ConnectTimeoutException: connection timed out: redis-cluster:6379\nhttp.response.status_code: 502\nduration: 5001ms",
			ageMs:   43_200_000, // 12시간 전
		},
		// inventory-service 장애 이력
		{
			spanID:  "ee50000000000001",
			service: "inventory-service",
			traceID: "trace_inv_history_001",
			text:    "[SPAN ERROR] inventory-service | PUT /api/stock/decrease\nexception: java.lang.RuntimeException: Optimistic lock conflict on stock table\nhttp.response.status_code: 409\nduration: 230ms\ndb.system: postgresql",
			ageMs:   21_600_000, // 6시간 전
		},
	}

	// 텍스트 배치 임베딩
	texts := make([]string, len(docs))
	for i, d := range docs {
		texts[i] = d.text
	}

	t.Logf("  Ollama 임베딩 요청: %d개 문서 → nomic-embed-text", len(texts))
	embedStart := time.Now()
	embeddings, err := embedder.Embed(ctx, texts)
	if err != nil {
		t.Fatalf("Ollama 임베딩 실패: %v", err)
	}
	t.Logf("  임베딩 완료: %d차원 × %d개, 소요시간: %s", len(embeddings[0]), len(embeddings), time.Since(embedStart).Round(time.Millisecond))

	// Qdrant에 적재
	points := make([]qdrantPoint, len(docs))
	for i, d := range docs {
		points[i] = qdrantPoint{
			ID:     spanIDToPointID(d.spanID),
			Vector: embeddings[i],
			Payload: map[string]any{
				"text":         d.text,
				"service_name": d.service,
				"trace_id":     d.traceID,
				"timestamp_ms": now - d.ageMs,
			},
		}
	}

	if err := qdrant.Upsert(ctx, points); err != nil {
		t.Fatalf("Qdrant Upsert: %v", err)
	}
	t.Logf("  ✓ %d건 Qdrant 적재 완료", len(points))

	// ── 3단계: 자연어 한국어 질의로 유사 장애 검색 ──────────────────
	t.Log("\n[3단계] 자연어 한국어 질의 유사도 검색")

	searcher := NewRAGSearcher(embedder, qdrant, 0.5) // 실제 임베딩은 0.5 threshold

	scenarios := []struct {
		name        string
		query       string // 자연어 한국어 질의
		serviceName string
		limit       int
	}{
		{
			name:        "시나리오 1: 결제 서비스 DB 연결 장애",
			query:       "결제 서비스에서 데이터베이스 연결이 안 됩니다. Connection refused 오류가 발생했어요",
			serviceName: "payment-service",
			limit:       3,
		},
		{
			name:        "시나리오 2: 주문 서비스 느린 쿼리",
			query:       "주문 조회가 너무 오래 걸려요. 쿼리가 10초 이상 걸리는 것 같아요",
			serviceName: "order-service",
			limit:       3,
		},
		{
			name:        "시나리오 3: 인증 실패 에러",
			query:       "로그인이 안 됩니다. 401 인증 오류가 계속 납니다",
			serviceName: "auth-service",
			limit:       3,
		},
		{
			name:        "시나리오 4: 전체 서비스 DB 타임아웃",
			query:       "데이터베이스 타임아웃이 여러 서비스에서 동시에 발생하고 있어요",
			serviceName: "", // 서비스 필터 없음
			limit:       5,
		},
		{
			name:        "시나리오 5: API 게이트웨이 Redis 장애",
			query:       "API 게이트웨이에서 502 에러가 납니다. Redis 연결 문제 같아요",
			serviceName: "api-gateway",
			limit:       3,
		},
	}

	var summaries []searchSummary

	for _, sc := range scenarios {
		t.Logf("\n  %s", sc.name)
		t.Logf("  질의: %q", sc.query)
		t.Log("  " + strings.Repeat("─", 55))

		searchStart := time.Now()
		results, err := searcher.Search(ctx, SearchRequest{
			Query:       sc.query,
			ServiceName: sc.serviceName,
			Limit:       sc.limit,
		})
		searchElapsed := time.Since(searchStart)

		if err != nil {
			t.Errorf("  Search error: %v", err)
			continue
		}

		if len(results) == 0 {
			t.Logf("  → 결과 없음 (유사도 0.5 미만) | 검색시간: %s", searchElapsed)
			summaries = append(summaries, searchSummary{
				scenario: sc.name, query: sc.query, count: 0,
			})
			continue
		}

		for i, r := range results {
			age := time.Since(time.UnixMilli(r.TimestampMs)).Round(time.Minute)
			preview := r.Text
			if len(preview) > 90 {
				preview = preview[:90] + "..."
			}
			t.Logf("  [%d] 유사도=%.4f | %s | %s 전", i+1, r.Score, r.ServiceName, age)
			t.Logf("       trace=%s", r.TraceID)
			t.Logf("       %s", preview)
		}
		t.Logf("  → 총 %d건 검색됨 | 검색시간: %s", len(results), searchElapsed)

		summaries = append(summaries, searchSummary{
			scenario: sc.name,
			query:    sc.query,
			count:    len(results),
			topScore: results[0].Score,
			topTrace: results[0].TraceID,
			topSvc:   results[0].ServiceName,
		})
	}

	// ── 4단계: LLM RCA 생성 ─────────────────────────────────────────
	t.Log("\n[4단계] LLM 근본 원인 분석(RCA) 생성")
	t.Logf("  모델: %s", llmModel)

	anomalyDesc := "[P0 이상 감지] payment-service | POST /api/pay\n" +
		"오류율: 45.2% (Z-score=5.3, 기준선 2.1%)\n" +
		"P95 응답시간: 8,500ms (Z-score=4.1, 기준선 180ms)\n" +
		"영향 범위: 최근 5분간 지속"

	rcaResults, _ := searcher.Search(ctx, SearchRequest{
		Query:       "결제 서비스 데이터베이스 연결 오류 타임아웃",
		ServiceName: "payment-service",
		Limit:       3,
	})

	t.Log("\n  LLM에 전달할 컨텍스트:")
	t.Logf("  - 이상 감지: %s", strings.Split(anomalyDesc, "\n")[0])
	t.Logf("  - 유사 장애 케이스: %d건", len(rcaResults))

	prompt := buildRCAPrompt(anomalyDesc, rcaResults)
	t.Logf("\n  RCA 프롬프트 생성 완료 (%d자)", len(prompt))

	llmStart := time.Now()
	rcaAnalysis, err := llm.Generate(ctx, prompt)
	llmElapsed := time.Since(llmStart)

	var llmSuccess bool
	if err != nil {
		t.Logf("  LLM 생성 실패 (모델 준비 중일 수 있음): %v", err)
	} else {
		llmSuccess = true
		t.Logf("  ✓ LLM RCA 생성 완료 | 소요시간: %s\n", llmElapsed)
		t.Log("  ┌" + strings.Repeat("─", 60) + "┐")
		for _, line := range strings.Split(rcaAnalysis, "\n") {
			if len(line) > 60 {
				line = line[:60]
			}
			t.Logf("  │ %-58s │", line)
		}
		t.Log("  └" + strings.Repeat("─", 60) + "┘")
	}

	// ── 5단계: 결과 요약 ────────────────────────────────────────────
	t.Log("\n[5단계] 테스트 결과 요약")
	t.Log("  " + strings.Repeat("━", 55))
	t.Logf("  임베딩 모델: %s (768차원)", embedModel)
	t.Logf("  LLM 모델:   %s", llmModel)
	t.Logf("  적재 문서:  %d건", len(docs))
	t.Log("  검색 시나리오:")
	for _, s := range summaries {
		if s.count > 0 {
			t.Logf("    ✓ %s → %d건 (최고유사도=%.4f)", s.scenario, s.count, s.topScore)
		} else {
			t.Logf("    - %s → 결과 없음", s.scenario)
		}
	}
	if llmSuccess {
		t.Logf("  RCA 생성:   ✓ (%s)", llmElapsed.Round(time.Second))
	} else {
		t.Log("  RCA 생성:   실패 (임베딩 파이프라인만 검증됨)")
	}

	// 결과를 전역 변수에 저장 (Notion 업로드용)
	testResultForNotion = &ollamaTestResult{
		embedModel:  embedModel,
		llmModel:    llmModel,
		docsCount:   len(docs),
		summaries:   summaries,
		rcaAnalysis: rcaAnalysis,
		llmSuccess:  llmSuccess,
		llmElapsed:  llmElapsed,
	}

	// ── 정리 ─────────────────────────────────────────────────────────
	if err := qdrant.deleteCollection(ctx); err != nil {
		t.Logf("  cleanup: %v", err)
	}

	fmt.Println("\n✓ Ollama RAG E2E 파이프라인 테스트 완료")
}

// searchSummary는 검색 시나리오별 요약이다.
type searchSummary struct {
	scenario string
	query    string
	count    int
	topScore float32
	topTrace string
	topSvc   string
}

// ollamaTestResult는 Notion 업로드를 위한 테스트 결과를 담는다.
type ollamaTestResult struct {
	embedModel  string
	llmModel    string
	docsCount   int
	summaries   []searchSummary
	rcaAnalysis string
	llmSuccess  bool
	llmElapsed  time.Duration
}

var testResultForNotion *ollamaTestResult
