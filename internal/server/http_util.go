package server

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

// ---- 헬퍼 ----

// readProtoBody는 HTTP 요청 body를 읽어 반환한다.
// Content-Encoding: gzip이거나 gzip 매직 바이트(0x1f 0x8b)로 시작하면 압축을 해제한다.
// 일부 OTel exporter가 gzip 압축은 하지만 Content-Encoding 헤더를 누락하는 경우를 방어한다.
// gzip.Reader는 sync.Pool에서 재사용해 GC 부담을 줄인다.
// zip-bomb 방어: 압축 전/후 모두 maxBodyBytes로 제한한다.
func readProtoBody(r *http.Request) ([]byte, error) {
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		return decompressGzip(io.LimitReader(r.Body, maxBodyBytes))
	}

	b, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		return nil, err
	}

	// Fallback: gzip 매직 바이트(0x1f 0x8b) 스니핑.
	// Content-Encoding 헤더 없이 gzip 전송하는 클라이언트 대응.
	// 해당 바이트를 proto.Unmarshal에 그대로 전달하면 wire type 7 오류 발생.
	if len(b) >= 2 && b[0] == 0x1f && b[1] == 0x8b {
		return decompressGzip(bytes.NewReader(b))
	}

	return b, nil
}

// decompressGzip은 gzip 스트림을 해제해 원본 bytes를 반환한다.
func decompressGzip(r io.Reader) ([]byte, error) {
	gz := gzipReaderPool.Get().(*gzip.Reader)
	if err := gz.Reset(r); err != nil {
		gzipReaderPool.Put(gz)
		return nil, err
	}
	defer func() {
		gz.Close()
		gzipReaderPool.Put(gz)
	}()
	return io.ReadAll(io.LimitReader(gz, maxBodyBytes))
}

func isJSON(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	return len(ct) >= len(jsonContentType) && ct[:len(jsonContentType)] == jsonContentType
}

func queryOffset(r *http.Request) int {
	s := r.URL.Query().Get("offset")
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func queryLimit(r *http.Request) int {
	s := r.URL.Query().Get("limit")
	if s == "" {
		return defaultLimit
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return defaultLimit
	}
	return n
}

func queryInt64(r *http.Request, key string) int64 {
	s := r.URL.Query().Get(key)
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// queryStatusCode는 ?status=ok|error|unset 을 int32로 변환한다.
// 지정되지 않으면 -1(필터 없음)을 반환한다.
func queryStatusCode(r *http.Request) int32 {
	switch r.URL.Query().Get("status") {
	case "unset":
		return 0
	case "ok":
		return 1
	case "error":
		return 2
	default:
		return -1
	}
}

// setCORSHeaders는 대시보드(브라우저)에서 직접 쿼리할 수 있도록 CORS 헤더를 설정한다.
func setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func storeSize(s any) int {
	if sz, ok := s.(sizer); ok {
		return sz.Size()
	}
	return -1
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("json encode error", "err", err)
	}
}
