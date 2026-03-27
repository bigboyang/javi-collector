// Package rag는 APM 에러 데이터를 벡터 임베딩으로 변환해 Qdrant에 적재하고,
// 자연어 장애 질의를 위한 RAG(Retrieval Augmented Generation) 파이프라인을 제공한다.
//
// 아키텍처:
//
//	TailSamplingStore → Ingester → EmbedPipeline (channel)
//	                                    → EmbedClient (Ollama/OpenAI)
//	                                    → QdrantClient.Upsert
//
//	자연어 질의: RAGSearcher.Search → EmbedClient → Qdrant → LLM 컨텍스트
//
// 운영 설정:
//
//	EMBED_ENABLED=true/false       (기본: false, 활성화 시 Ollama 또는 OpenAI 필요)
//	EMBED_ENDPOINT=http://...      (Ollama: http://localhost:11434)
//	EMBED_MODEL=nomic-embed-text   (768차원, on-premise)
//	QDRANT_ENDPOINT=http://...     (기본: http://localhost:6333)
//	QDRANT_COLLECTION=apm_errors
package rag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kkc/javi-collector/internal/model"
)

// EmbedDocument는 임베딩 대상 문서다.
// span error + 연관 로그를 하나의 청크로 구성한다.
type EmbedDocument struct {
	SpanID      string
	TraceID     string
	ServiceName string
	TimestampMs int64
	Text        string            // 임베딩할 원문 텍스트
	Metadata    map[string]string // Qdrant 페이로드 (필터용)
}

// DocumentBuilder는 ERROR·WARN·SLOW span에서 EmbedDocument를 구성한다.
//   - ERROR: StatusCode=2
//   - WARN:  HTTP 4xx (http.response.status_code 또는 http.status_code 400–499)
//   - SLOW:  duration >= SlowMs (SlowMs=0이면 비활성화)
type DocumentBuilder struct {
	SlowMs int64 // 느린 스팬 임계값(ms). 0이면 비활성화.
}

// BuildFromSpan은 ERROR·WARN·SLOW span에서 임베딩 문서를 생성한다.
// 인덱싱 조건에 해당하지 않거나 텍스트가 너무 짧으면 nil을 반환한다.
func (b *DocumentBuilder) BuildFromSpan(sp *model.SpanData, correlatedLogs []*model.LogData) *EmbedDocument {
	durationMs := (sp.EndTimeNano - sp.StartTimeNano) / 1_000_000

	isError := sp.StatusCode == 2
	httpStatus := spanHTTPStatus(sp.Attributes)
	isWarn := httpStatus >= 400 && httpStatus < 500
	isSlow := b.SlowMs > 0 && durationMs >= b.SlowMs

	if !isError && !isWarn && !isSlow {
		return nil
	}

	label := spanTypeLabel(isError, isWarn, isSlow)

	var sb strings.Builder
	fmt.Fprintf(&sb, "[%s] %s | service: %s\n", label, sp.Name, sp.ServiceName)
	if sp.StatusMessage != "" {
		fmt.Fprintf(&sb, "status: %s\n", sp.StatusMessage)
	}
	if isSlow {
		fmt.Fprintf(&sb, "duration: %dms\n", durationMs)
	}
	if isWarn {
		fmt.Fprintf(&sb, "http.status: %d\n", httpStatus)
	}

	attrs := sp.Attributes
	if attrs == nil {
		attrs = map[string]any{}
	}

	if v := strVal(attrs["exception.type"]); v != "" {
		fmt.Fprintf(&sb, "exception: %s\n", v)
	}
	if v := strVal(attrs["exception.message"]); v != "" {
		fmt.Fprintf(&sb, "message: %s\n", v)
	}
	if v := strVal(attrs["exception.stacktrace"]); v != "" {
		// 스택트레이스 최대 2000자 (임베딩 토큰 절약)
		if len(v) > 2000 {
			v = v[:2000] + "...[truncated]"
		}
		fmt.Fprintf(&sb, "stacktrace:\n%s\n", v)
	}

	// 연관 ERROR 로그
	for _, l := range correlatedLogs {
		if l.SeverityNumber >= 17 { // ERROR 이상
			ts := time.UnixMilli(l.ReceivedAtMs).UTC().Format(time.RFC3339)
			fmt.Fprintf(&sb, "[%s] %s - %s\n", ts, l.SeverityText, l.Body)
		}
	}

	text := sb.String()
	if len(strings.TrimSpace(text)) < 20 {
		return nil // 너무 짧으면 임베딩 의미 없음
	}

	return &EmbedDocument{
		SpanID:      sp.SpanID,
		TraceID:     sp.TraceID,
		ServiceName: sp.ServiceName,
		TimestampMs: sp.ReceivedAtMs,
		Text:        text,
		Metadata: map[string]string{
			"service_name":   sp.ServiceName,
			"span_name":      sp.Name,
			"trace_id":       sp.TraceID,
			"status_message": sp.StatusMessage,
			"duration_ms":    fmt.Sprintf("%d", durationMs),
			"timestamp_ms":   fmt.Sprintf("%d", sp.ReceivedAtMs),
			"span_type":      strings.ToLower(label),
		},
	}
}

// spanHTTPStatus는 span attributes에서 HTTP 상태 코드를 추출한다.
// "http.response.status_code"(신규) → "http.status_code"(구형) 순으로 탐색한다.
func spanHTTPStatus(attrs map[string]any) int {
	for _, key := range []string{"http.response.status_code", "http.status_code"} {
		v := strVal(attrs[key])
		if v == "" {
			continue
		}
		var code int
		if _, err := fmt.Sscanf(v, "%d", &code); err == nil && code > 0 {
			return code
		}
	}
	return 0
}

// spanTypeLabel은 span 분류에 따른 레이블을 반환한다.
func spanTypeLabel(isError, isWarn, isSlow bool) string {
	switch {
	case isError:
		return "SPAN ERROR"
	case isSlow && isWarn:
		return "SPAN SLOW+WARN"
	case isSlow:
		return "SPAN SLOW"
	default:
		return "SPAN WARN"
	}
}

func strVal(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// ---- EmbedClient ----

// EmbedClient는 텍스트를 벡터로 변환하는 인터페이스다.
// Ollama, OpenAI, Cohere 등 교체 가능.
type EmbedClient interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// OllamaEmbedClient는 로컬 Ollama로 임베딩을 생성한다.
// nomic-embed-text (768차원), 외부 API 비용 없이 on-premise 운영.
//
// 사용:
//
//	docker run -d -p 11434:11434 ollama/ollama
//	ollama pull nomic-embed-text
type OllamaEmbedClient struct {
	endpoint string
	model    string
	client   *http.Client
}

func NewOllamaEmbedClient(endpoint, model string) *OllamaEmbedClient {
	return &OllamaEmbedClient{
		endpoint: endpoint,
		model:    model,
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *OllamaEmbedClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	type req struct {
		Model string   `json:"model"`
		Input []string `json:"input"`
	}
	type resp struct {
		Embeddings [][]float32 `json:"embeddings"`
	}

	body, _ := json.Marshal(req{Model: c.model, Input: texts})
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.endpoint+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("ollama embed: %w", err)
	}
	defer httpResp.Body.Close()

	var out resp
	if err := json.NewDecoder(httpResp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("ollama decode: %w", err)
	}
	if len(out.Embeddings) != len(texts) {
		return nil, fmt.Errorf("embedding count mismatch: got %d, want %d", len(out.Embeddings), len(texts))
	}
	return out.Embeddings, nil
}

// ---- QdrantClient ----

// QdrantClient는 Qdrant REST API를 통해 벡터를 저장/검색한다.
type QdrantClient struct {
	endpoint   string
	collection string
	client     *http.Client
}

func NewQdrantClient(endpoint, collection string) *QdrantClient {
	return &QdrantClient{
		endpoint:   endpoint,
		collection: collection,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

// EnsureCollection은 collection이 없으면 생성한다.
func (q *QdrantClient) EnsureCollection(ctx context.Context, dimension uint) error {
	body, _ := json.Marshal(map[string]any{
		"vectors": map[string]any{
			"size":     dimension,
			"distance": "Cosine",
		},
		"hnsw_config": map[string]any{"m": 16, "ef_construct": 100},
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut,
		q.endpoint+"/collections/"+q.collection, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := q.client.Do(req)
	if err != nil {
		return fmt.Errorf("qdrant create collection: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != 409 { // 409=already exists
		return fmt.Errorf("qdrant create collection: status %d", resp.StatusCode)
	}
	return nil
}

type qdrantPoint struct {
	ID      string            `json:"id"`
	Vector  []float32         `json:"vector"`
	Payload map[string]string `json:"payload"`
}

func (q *QdrantClient) Upsert(ctx context.Context, points []qdrantPoint) error {
	body, _ := json.Marshal(map[string]any{"points": points})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut,
		q.endpoint+"/collections/"+q.collection+"/points", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := q.client.Do(req)
	if err != nil {
		return fmt.Errorf("qdrant upsert: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("qdrant upsert: status %d", resp.StatusCode)
	}
	return nil
}

// ---- EmbedPipeline ----

// EmbedPipeline은 EmbedDocument를 비동기 배치로 처리해 Qdrant에 적재한다.
//
// 설계 원칙:
//   - 채널 기반 비동기: ingestion 핫패스를 블로킹하지 않는다.
//   - drop-on-full: 채널이 꽉 차면 즉시 드롭 (APM은 완전성보다 속도 우선)
//   - 배치 임베딩: API 호출 횟수를 최소화한다.
type EmbedPipeline struct {
	embedder EmbedClient
	qdrant   *QdrantClient
	ch       chan *EmbedDocument
	batchSz  int
	flushInt time.Duration
	wg       sync.WaitGroup
}

// NewEmbedPipeline은 EmbedPipeline을 생성한다.
// chanBuf: 채널 버퍼 크기, batchSz: 한 번에 처리할 문서 수, flushInterval: 타이머 flush 주기.
func NewEmbedPipeline(embedder EmbedClient, qdrant *QdrantClient, chanBuf, batchSz int, flushInterval time.Duration) *EmbedPipeline {
	return &EmbedPipeline{
		embedder: embedder,
		qdrant:   qdrant,
		ch:       make(chan *EmbedDocument, chanBuf),
		batchSz:  batchSz,
		flushInt: flushInterval,
	}
}

// Submit은 임베딩 대상 문서를 큐에 넣는다. 채널이 꽉 차면 드롭.
func (p *EmbedPipeline) Submit(doc *EmbedDocument) {
	select {
	case p.ch <- doc:
	default:
		slog.Warn("embed pipeline full, dropping document",
			"span_id", doc.SpanID, "service", doc.ServiceName)
	}
}

// Start는 배치 처리 goroutine을 시작한다.
func (p *EmbedPipeline) Start(ctx context.Context) {
	p.wg.Add(1)
	go p.run(ctx)
}

// Close는 채널을 닫고 goroutine 종료를 기다린다.
func (p *EmbedPipeline) Close() {
	close(p.ch)
	p.wg.Wait()
}

func (p *EmbedPipeline) run(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(p.flushInt)
	defer ticker.Stop()

	batch := make([]*EmbedDocument, 0, p.batchSz)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := p.flushBatch(ctx, batch); err != nil {
			slog.Error("embed pipeline flush failed", "err", err, "count", len(batch))
		}
		batch = batch[:0]
	}

	for {
		select {
		case doc, ok := <-p.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, doc)
			if len(batch) >= p.batchSz {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (p *EmbedPipeline) flushBatch(ctx context.Context, docs []*EmbedDocument) error {
	texts := make([]string, len(docs))
	for i, d := range docs {
		texts[i] = d.Text
	}

	embeddings, err := p.embedder.Embed(ctx, texts)
	if err != nil {
		return fmt.Errorf("embed: %w", err)
	}

	points := make([]qdrantPoint, len(docs))
	for i, doc := range docs {
		payload := make(map[string]string, len(doc.Metadata)+1)
		for k, v := range doc.Metadata {
			payload[k] = v
		}
		payload["text"] = doc.Text
		payload["trace_id"] = doc.TraceID

		points[i] = qdrantPoint{
			ID:      spanIDToPointID(doc.SpanID),
			Vector:  embeddings[i],
			Payload: payload,
		}
	}

	return p.qdrant.Upsert(ctx, points)
}

// spanIDToPointID는 16자리 hex span_id를 UUID 형식으로 변환한다.
func spanIDToPointID(spanID string) string {
	padded := fmt.Sprintf("%032s", spanID)
	if len(padded) < 32 {
		padded = strings.Repeat("0", 32-len(padded)) + padded
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		padded[0:8], padded[8:12], padded[12:16], padded[16:20], padded[20:32])
}

// ---- RAGSearcher ----

// SearchRequest는 자연어 검색 파라미터다.
type SearchRequest struct {
	Query       string // 자연어 질의 (예: "payment service database connection timeout")
	ServiceName string // 서비스 필터 (빈 문자열이면 전체)
	FromMs      int64  // 시작 시간 (0이면 필터 없음)
	Limit       int    // 결과 수 (기본 5)
}

// SearchResult는 RAG 검색 단건 결과다.
type SearchResult struct {
	TraceID     string
	ServiceName string
	Score       float32 // 코사인 유사도 [0, 1]
	Text        string  // 임베딩 원문
	TimestampMs int64
}

// RAGSearcher는 자연어 질의를 벡터 검색으로 변환한다.
type RAGSearcher struct {
	embedder       EmbedClient
	qdrant         *QdrantClient
	scoreThreshold float64 // 코사인 유사도 임계값 (기본 0.65)
}

func NewRAGSearcher(embedder EmbedClient, qdrant *QdrantClient, scoreThreshold float64) *RAGSearcher {
	if scoreThreshold <= 0 {
		scoreThreshold = 0.65
	}
	return &RAGSearcher{embedder: embedder, qdrant: qdrant, scoreThreshold: scoreThreshold}
}

// Search는 자연어 질의를 임베딩하고 유사 장애 케이스를 반환한다.
func (r *RAGSearcher) Search(ctx context.Context, req SearchRequest) ([]SearchResult, error) {
	if req.Limit <= 0 {
		req.Limit = 5
	}

	embeddings, err := r.embedder.Embed(ctx, []string{req.Query})
	if err != nil {
		return nil, fmt.Errorf("query embed: %w", err)
	}

	var must []map[string]any
	if req.ServiceName != "" {
		must = append(must, map[string]any{
			"key":   "service_name",
			"match": map[string]string{"value": req.ServiceName},
		})
	}
	if req.FromMs > 0 {
		must = append(must, map[string]any{
			"key":   "timestamp_ms",
			"range": map[string]int64{"gte": req.FromMs},
		})
	}

	searchBody := map[string]any{
		"vector":           embeddings[0],
		"limit":            req.Limit,
		"with_payload":     true,
		"score_threshold":  r.scoreThreshold,
	}
	if len(must) > 0 {
		searchBody["filter"] = map[string]any{"must": must}
	}

	body, _ := json.Marshal(searchBody)
	httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		r.qdrant.endpoint+"/collections/"+r.qdrant.collection+"/points/search",
		bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := r.qdrant.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("qdrant search: %w", err)
	}
	defer httpResp.Body.Close()

	var out struct {
		Result []struct {
			Score   float32           `json:"score"`
			Payload map[string]string `json:"payload"`
		} `json:"result"`
	}
	if err := json.NewDecoder(httpResp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("qdrant decode: %w", err)
	}

	results := make([]SearchResult, 0, len(out.Result))
	for _, item := range out.Result {
		var tsMs int64
		fmt.Sscanf(item.Payload["timestamp_ms"], "%d", &tsMs)
		results = append(results, SearchResult{
			TraceID:     item.Payload["trace_id"],
			ServiceName: item.Payload["service_name"],
			Score:       item.Score,
			Text:        item.Payload["text"],
			TimestampMs: tsMs,
		})
	}
	return results, nil
}

// FormatForLLM은 검색 결과를 LLM 프롬프트 컨텍스트로 변환한다.
func FormatForLLM(query string, results []SearchResult) string {
	var sb strings.Builder
	sb.WriteString("다음은 유사한 과거 장애 케이스입니다:\n\n")
	for i, r := range results {
		fmt.Fprintf(&sb, "=== 케이스 %d (유사도: %.2f) ===\n", i+1, r.Score)
		fmt.Fprintf(&sb, "서비스: %s | TraceID: %s\n", r.ServiceName, r.TraceID)
		sb.WriteString(r.Text)
		sb.WriteString("\n\n")
	}
	fmt.Fprintf(&sb, "=== 현재 문의 ===\n%s", query)
	return sb.String()
}
