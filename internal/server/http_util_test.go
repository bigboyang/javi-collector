package server

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"testing"
)

// http.go 분할 검증: 공용 헬퍼(http_util.go)와 SQL 검증(http_query.go)이
// 이동 후에도 동일하게 동작하는지 고정한다. 순수 함수라 서버 없이 검증한다.

func reqWithQuery(rawQuery string) *http.Request {
	return httptest.NewRequest(http.MethodGet, "/?"+rawQuery, nil)
}

func TestQueryLimit(t *testing.T) {
	cases := []struct {
		query string
		want  int
	}{
		{"", defaultLimit},
		{"limit=", defaultLimit},
		{"limit=50", 50},
		{"limit=0", defaultLimit},
		{"limit=-5", defaultLimit},
		{"limit=abc", defaultLimit},
	}
	for _, c := range cases {
		if got := queryLimit(reqWithQuery(c.query)); got != c.want {
			t.Errorf("queryLimit(%q) = %d, want %d", c.query, got, c.want)
		}
	}
}

func TestQueryOffset(t *testing.T) {
	cases := []struct {
		query string
		want  int
	}{
		{"", 0},
		{"offset=10", 10},
		{"offset=0", 0},
		{"offset=-1", 0},
		{"offset=xyz", 0},
	}
	for _, c := range cases {
		if got := queryOffset(reqWithQuery(c.query)); got != c.want {
			t.Errorf("queryOffset(%q) = %d, want %d", c.query, got, c.want)
		}
	}
}

func TestQueryInt64(t *testing.T) {
	cases := []struct {
		query string
		key   string
		want  int64
	}{
		{"from=1700000000000", "from", 1700000000000},
		{"", "from", 0},
		{"from=bad", "from", 0},
		{"to=42", "to", 42},
	}
	for _, c := range cases {
		if got := queryInt64(reqWithQuery(c.query), c.key); got != c.want {
			t.Errorf("queryInt64(%q, %q) = %d, want %d", c.query, c.key, got, c.want)
		}
	}
}

func TestQueryStatusCode(t *testing.T) {
	cases := []struct {
		query string
		want  int32
	}{
		{"status=unset", 0},
		{"status=ok", 1},
		{"status=error", 2},
		{"", -1},
		{"status=weird", -1},
	}
	for _, c := range cases {
		if got := queryStatusCode(reqWithQuery(c.query)); got != c.want {
			t.Errorf("queryStatusCode(%q) = %d, want %d", c.query, got, c.want)
		}
	}
}

func TestIsJSON(t *testing.T) {
	cases := []struct {
		ct   string
		want bool
	}{
		{"application/json", true},
		{"application/json; charset=utf-8", true},
		{"application/x-protobuf", false},
		{"", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		if c.ct != "" {
			r.Header.Set("Content-Type", c.ct)
		}
		if got := isJSON(r); got != c.want {
			t.Errorf("isJSON(%q) = %v, want %v", c.ct, got, c.want)
		}
	}
}

func TestValidateRawSQL(t *testing.T) {
	valid := []string{
		"SELECT 1",
		"select service_name, count() from apm.spans group by service_name",
	}
	for _, sql := range valid {
		if err := validateRawSQL(sql); err != nil {
			t.Errorf("validateRawSQL(%q) unexpected error: %v", sql, err)
		}
	}

	invalid := []string{
		"",                              // 빈 쿼리
		"SHOW TABLES",                   // SELECT 아님
		"SELECT * FROM x; DROP TABLE y", // 위험 키워드
		"SELECT * FROM x WHERE 1=1 DELETE",
		"INSERT INTO x VALUES (1)",
	}
	for _, sql := range invalid {
		if err := validateRawSQL(sql); err == nil {
			t.Errorf("validateRawSQL(%q) = nil, want error", sql)
		}
	}
}

func TestReadProtoBody_Gzip(t *testing.T) {
	payload := []byte("hello-otlp-payload")

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(payload); err != nil {
		t.Fatal(err)
	}
	gz.Close()
	compressed := buf.Bytes()

	// 1. Content-Encoding: gzip 헤더 경로
	r1 := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(compressed))
	r1.Header.Set("Content-Encoding", "gzip")
	got, err := readProtoBody(r1)
	if err != nil {
		t.Fatalf("readProtoBody (header) error: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("readProtoBody (header) = %q, want %q", got, payload)
	}

	// 2. 헤더 없이 gzip 매직 바이트(0x1f 0x8b) 스니핑 경로
	r2 := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(compressed))
	got2, err := readProtoBody(r2)
	if err != nil {
		t.Fatalf("readProtoBody (sniff) error: %v", err)
	}
	if !bytes.Equal(got2, payload) {
		t.Errorf("readProtoBody (sniff) = %q, want %q", got2, payload)
	}

	// 3. 평문은 그대로 통과
	r3 := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(payload))
	got3, err := readProtoBody(r3)
	if err != nil {
		t.Fatalf("readProtoBody (plain) error: %v", err)
	}
	if !bytes.Equal(got3, payload) {
		t.Errorf("readProtoBody (plain) = %q, want %q", got3, payload)
	}
}
