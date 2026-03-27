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

	attrs := sp.Attributes
	if attrs == nil {
		attrs = map[string]any{}
	}

	var sb strings.Builder

	// === 헤더: 분류·서비스·span 종류·소요시간 ===
	fmt.Fprintf(&sb, "[%s] %s | service: %s\n", label, sp.Name, sp.ServiceName)
	fmt.Fprintf(&sb, "span_kind: %s | duration: %dms\n", spanKindLabel(sp.Kind), durationMs)
	if sp.StatusMessage != "" {
		fmt.Fprintf(&sb, "status: %s\n", sp.StatusMessage)
	}

	// === HTTP 컨텍스트 ===
	httpMethod := strVal(attrs["http.request.method"])
	if httpMethod == "" {
		httpMethod = strVal(attrs["http.method"])
	}
	httpURL := strVal(attrs["url.full"])
	if httpURL == "" {
		httpURL = strVal(attrs["http.url"])
	}
	httpRoute := strVal(attrs["http.route"])
	httpTarget := strVal(attrs["http.target"])

	if httpMethod != "" || httpURL != "" || httpRoute != "" || httpTarget != "" || httpStatus > 0 {
		sb.WriteString("--- HTTP ---\n")
		if httpMethod != "" {
			fmt.Fprintf(&sb, "http.method: %s\n", httpMethod)
		}
		if httpStatus > 0 {
			fmt.Fprintf(&sb, "http.status: %d\n", httpStatus)
		}
		if httpRoute != "" {
			fmt.Fprintf(&sb, "http.route: %s\n", httpRoute)
		} else if httpTarget != "" {
			fmt.Fprintf(&sb, "http.target: %s\n", httpTarget)
		}
		if httpURL != "" {
			if len(httpURL) > 200 {
				httpURL = httpURL[:200] + "...[truncated]"
			}
			fmt.Fprintf(&sb, "http.url: %s\n", httpURL)
		}
	}

	// === DB 컨텍스트 ===
	dbSystem := strVal(attrs["db.system"])
	dbName := strVal(attrs["db.name"])
	dbOperation := strVal(attrs["db.operation"])
	dbStatement := strVal(attrs["db.statement"])

	if dbSystem != "" || dbStatement != "" || dbName != "" {
		sb.WriteString("--- DB ---\n")
		if dbSystem != "" {
			fmt.Fprintf(&sb, "db.system: %s\n", dbSystem)
		}
		if dbName != "" {
			fmt.Fprintf(&sb, "db.name: %s\n", dbName)
		}
		if dbOperation != "" {
			fmt.Fprintf(&sb, "db.operation: %s\n", dbOperation)
		}
		if dbStatement != "" {
			if len(dbStatement) > 500 {
				dbStatement = dbStatement[:500] + "...[truncated]"
			}
			fmt.Fprintf(&sb, "db.statement: %s\n", dbStatement)
		}
	}

	// === RPC 컨텍스트 ===
	rpcSystem := strVal(attrs["rpc.system"])
	rpcService := strVal(attrs["rpc.service"])
	rpcMethod := strVal(attrs["rpc.method"])

	if rpcSystem != "" || rpcService != "" {
		sb.WriteString("--- RPC ---\n")
		if rpcSystem != "" {
			fmt.Fprintf(&sb, "rpc.system: %s\n", rpcSystem)
		}
		if rpcService != "" {
			fmt.Fprintf(&sb, "rpc.service: %s\n", rpcService)
		}
		if rpcMethod != "" {
			fmt.Fprintf(&sb, "rpc.method: %s\n", rpcMethod)
		}
	}

	// === Messaging 컨텍스트 ===
	msgSystem := strVal(attrs["messaging.system"])
	msgDest := strVal(attrs["messaging.destination.name"])
	if msgDest == "" {
		msgDest = strVal(attrs["messaging.destination"])
	}

	if msgSystem != "" || msgDest != "" {
		sb.WriteString("--- Messaging ---\n")
		if msgSystem != "" {
			fmt.Fprintf(&sb, "messaging.system: %s\n", msgSystem)
		}
		if msgDest != "" {
			fmt.Fprintf(&sb, "messaging.destination: %s\n", msgDest)
		}
	}

	// === Peer / 네트워크 ===
	peerService := strVal(attrs["peer.service"])
	netPeerName := strVal(attrs["net.peer.name"])
	netPeerPort := strVal(attrs["net.peer.port"])

	if peerService != "" {
		fmt.Fprintf(&sb, "peer.service: %s\n", peerService)
	}
	if netPeerName != "" {
		if netPeerPort != "" {
			fmt.Fprintf(&sb, "net.peer: %s:%s\n", netPeerName, netPeerPort)
		} else {
			fmt.Fprintf(&sb, "net.peer: %s\n", netPeerName)
		}
	}

	// === Exception ===
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

	// enriched metadata (Qdrant 필터용)
	metadata := map[string]string{
		"service_name":   sp.ServiceName,
		"span_name":      sp.Name,
		"trace_id":       sp.TraceID,
		"status_message": sp.StatusMessage,
		"duration_ms":    fmt.Sprintf("%d", durationMs),
		"timestamp_ms":   fmt.Sprintf("%d", sp.ReceivedAtMs),
		"span_type":      strings.ToLower(label),
		"span_kind":      spanKindLabel(sp.Kind),
	}
	if httpMethod != "" {
		metadata["http_method"] = httpMethod
	}
	if httpStatus > 0 {
		metadata["http_status"] = fmt.Sprintf("%d", httpStatus)
	}
	if httpRoute != "" {
		metadata["http_route"] = httpRoute
	}
	if dbSystem != "" {
		metadata["db_system"] = dbSystem
	}
	if rpcSystem != "" {
		metadata["rpc_system"] = rpcSystem
	}

	return &EmbedDocument{
		SpanID:      sp.SpanID,
		TraceID:     sp.TraceID,
		ServiceName: sp.ServiceName,
		TimestampMs: sp.ReceivedAtMs,
		Text:        text,
		Metadata:    metadata,
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

// spanKindLabel은 OTLP SpanKind 숫자를 사람이 읽기 쉬운 문자열로 변환한다.
// https://opentelemetry.io/docs/specs/otel/trace/api/#spankind
func spanKindLabel(kind int32) string {
	switch kind {
	case 1:
		return "INTERNAL"
	case 2:
		return "SERVER"
	case 3:
		return "CLIENT"
	case 4:
		return "PRODUCER"
	case 5:
		return "CONSUMER"
	default:
		return "UNSPECIFIED"
	}
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
	ID      string         `json:"id"`
	Vector  []float32      `json:"vector"`
	Payload map[string]any `json:"payload"` // any: string + int64 혼용 (range filter 지원)
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
		payload := make(map[string]any, len(doc.Metadata)+2)
		for k, v := range doc.Metadata {
			payload[k] = v
		}
		payload["text"] = doc.Text
		payload["trace_id"] = doc.TraceID
		// timestamp_ms를 int64로 저장 — Qdrant range filter는 integer/float 타입 필요.
		// 문자열로 저장하면 range 조건이 사전순 비교가 되어 TTL/검색 모두 오작동한다.
		payload["timestamp_ms"] = doc.TimestampMs

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
			Score   float32        `json:"score"`
			Payload map[string]any `json:"payload"` // int64 필드(timestamp_ms) 때문에 any 필요
		} `json:"result"`
	}
	if err := json.NewDecoder(httpResp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("qdrant decode: %w", err)
	}

	results := make([]SearchResult, 0, len(out.Result))
	for _, item := range out.Result {
		results = append(results, SearchResult{
			TraceID:     payloadStr(item.Payload, "trace_id"),
			ServiceName: payloadStr(item.Payload, "service_name"),
			Score:       item.Score,
			Text:        payloadStr(item.Payload, "text"),
			TimestampMs: payloadInt64(item.Payload, "timestamp_ms"),
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

// ---- LLMClient ----

// LLMClient는 텍스트를 생성하는 인터페이스다.
// Ollama, OpenAI 등 교체 가능.
type LLMClient interface {
	Generate(ctx context.Context, prompt string) (string, error)
}

// OllamaLLMClient는 로컬 Ollama로 텍스트를 생성한다.
// qwen2.5:3b, llama3.2:3b 등 소형 LLM을 on-premise에서 운영.
//
// 사용:
//
//	ollama pull qwen2.5:3b
type OllamaLLMClient struct {
	endpoint string
	model    string
	client   *http.Client
}

func NewOllamaLLMClient(endpoint, model string) *OllamaLLMClient {
	return &OllamaLLMClient{
		endpoint: endpoint,
		model:    model,
		client:   &http.Client{Timeout: 60 * time.Second},
	}
}

func (c *OllamaLLMClient) Generate(ctx context.Context, prompt string) (string, error) {
	type message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	type req struct {
		Model    string    `json:"model"`
		Messages []message `json:"messages"`
		Stream   bool      `json:"stream"`
	}
	type resp struct {
		Message message `json:"message"`
	}

	body, _ := json.Marshal(req{
		Model: c.model,
		Messages: []message{
			{Role: "system", Content: "당신은 APM 시스템의 근본 원인 분석(RCA) 전문가입니다. 주어진 이상 감지 정보와 과거 유사 사례를 바탕으로 간결하고 실용적인 RCA 분석을 한국어로 제공하세요. 200자 이내로 요약하세요."},
			{Role: "user", Content: prompt},
		},
		Stream: false,
	})

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.endpoint+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("ollama generate: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode >= 300 {
		return "", fmt.Errorf("ollama generate: status %d", httpResp.StatusCode)
	}

	var out resp
	if err := json.NewDecoder(httpResp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("ollama decode: %w", err)
	}
	return strings.TrimSpace(out.Message.Content), nil
}

// ---- RAGGenerator ----

// RAGGenerator는 Retrieval(유사 사례 검색) + Generation(LLM 분석)을 결합한다.
// RCA Engine이 이상 감지 시 호출해 LLM 기반 근본 원인 분석 텍스트를 생성한다.
type RAGGenerator struct {
	searcher *RAGSearcher
	llm      LLMClient
}

// NewRAGGenerator는 RAGGenerator를 생성한다.
func NewRAGGenerator(searcher *RAGSearcher, llm LLMClient) *RAGGenerator {
	return &RAGGenerator{searcher: searcher, llm: llm}
}

// Generate는 anomalyDesc와 유사 사례를 결합해 LLM RCA 분석을 반환한다.
//   - searchReq: Qdrant 유사 사례 검색 파라미터 (nil이면 검색 생략)
//   - anomalyDesc: 이상 감지 정보 요약 (LLM 입력에 포함)
func (g *RAGGenerator) Generate(ctx context.Context, anomalyDesc string, searchReq *SearchRequest) (string, error) {
	// 1. Retrieval: 유사 과거 사례 검색
	var results []SearchResult
	if searchReq != nil {
		var err error
		results, err = g.searcher.Search(ctx, *searchReq)
		if err != nil {
			slog.Warn("rag generator search failed, generating without context", "err", err)
		}
	}

	// 2. Prompt 구성
	prompt := buildRCAPrompt(anomalyDesc, results)

	// 3. LLM Generation
	return g.llm.Generate(ctx, prompt)
}

// buildRCAPrompt는 이상 감지 정보와 유사 사례를 LLM 프롬프트로 조합한다.
func buildRCAPrompt(anomalyDesc string, similar []SearchResult) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "## 현재 이상 감지\n%s\n\n", anomalyDesc)

	if len(similar) > 0 {
		sb.WriteString("## 과거 유사 사례\n")
		for i, r := range similar {
			summary := r.Text
			if len(summary) > 400 {
				summary = summary[:400] + "..."
			}
			fmt.Fprintf(&sb, "### 사례 %d (유사도: %.2f, 서비스: %s)\n%s\n\n",
				i+1, r.Score, r.ServiceName, summary)
		}
	}

	sb.WriteString("## 요청\n위 정보를 바탕으로 근본 원인과 권장 조치를 간결하게 분석하세요.")
	return sb.String()
}

// ---- payload 헬퍼 ----

// payloadStr은 map[string]any 페이로드에서 문자열 값을 추출한다.
func payloadStr(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// payloadInt64는 map[string]any 페이로드에서 int64 값을 추출한다.
// JSON 숫자는 float64로 디코딩되므로 float64 → int64 변환을 처리한다.
func payloadInt64(m map[string]any, key string) int64 {
	if v, ok := m[key]; ok {
		switch x := v.(type) {
		case float64:
			return int64(x)
		case int64:
			return x
		case string:
			var n int64
			fmt.Sscanf(x, "%d", &n)
			return n
		}
	}
	return 0
}

// ---- QdrantClient.DeleteByFilter ----

// DeleteByFilter는 필터 조건에 맞는 포인트를 일괄 삭제한다.
// TTL Janitor가 오래된 포인트를 정리할 때 사용한다.
//
// filter 예시 (timestamp_ms < cutoffMs):
//
//	map[string]any{
//	  "must": []map[string]any{
//	    {"key": "timestamp_ms", "range": map[string]any{"lt": cutoffMs}},
//	  },
//	}
func (q *QdrantClient) DeleteByFilter(ctx context.Context, filter map[string]any) error {
	body, _ := json.Marshal(map[string]any{"filter": filter})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		q.endpoint+"/collections/"+q.collection+"/points/delete",
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := q.client.Do(req)
	if err != nil {
		return fmt.Errorf("qdrant delete: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("qdrant delete: status %d", resp.StatusCode)
	}
	return nil
}

// ---- QdrantJanitor ----

// QdrantJanitor는 주기적으로 오래된 벡터 포인트를 삭제해 컬렉션 크기를 제한한다.
//
// 동작:
//   - 기동 즉시 1회 실행 후, interval마다 반복한다.
//   - retentionDays보다 오래된 포인트(timestamp_ms 기준)를 삭제한다.
//   - ctx 취소 시 즉시 종료 (graceful shutdown 보장).
type QdrantJanitor struct {
	qdrant        *QdrantClient
	retentionDays int
	interval      time.Duration
}

// NewQdrantJanitor는 QdrantJanitor를 생성한다.
//   - retentionDays: 포인트 보관 기간(일). 0 이하이면 30일로 설정.
//   - interval: 정리 실행 주기. 0 이하이면 6시간으로 설정.
func NewQdrantJanitor(qdrant *QdrantClient, retentionDays int, interval time.Duration) *QdrantJanitor {
	if retentionDays <= 0 {
		retentionDays = 30
	}
	if interval <= 0 {
		interval = 6 * time.Hour
	}
	return &QdrantJanitor{qdrant: qdrant, retentionDays: retentionDays, interval: interval}
}

// Start는 백그라운드 goroutine으로 janitor를 실행한다.
// ctx가 취소되면 종료한다.
func (j *QdrantJanitor) Start(ctx context.Context) {
	go func() {
		// 기동 즉시 1회 실행해 이전 재기동에서 누적된 포인트를 정리한다.
		j.runOnce(ctx)

		ticker := time.NewTicker(j.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				j.runOnce(ctx)
			}
		}
	}()
}

func (j *QdrantJanitor) runOnce(ctx context.Context) {
	cutoffMs := time.Now().AddDate(0, 0, -j.retentionDays).UnixMilli()
	filter := map[string]any{
		"must": []map[string]any{
			{
				"key":   "timestamp_ms",
				"range": map[string]any{"lt": cutoffMs},
			},
		},
	}
	if err := j.qdrant.DeleteByFilter(ctx, filter); err != nil {
		slog.Error("qdrant janitor eviction failed",
			"err", err,
			"cutoff", time.UnixMilli(cutoffMs).UTC().Format(time.RFC3339),
		)
		return
	}
	slog.Info("qdrant janitor eviction done",
		"retention_days", j.retentionDays,
		"cutoff", time.UnixMilli(cutoffMs).UTC().Format(time.RFC3339),
	)
}
