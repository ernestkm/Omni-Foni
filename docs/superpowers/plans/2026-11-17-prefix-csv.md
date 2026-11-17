# Reports Prefix Grouping and CSV Export Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `PREFIX` grouping to `Summarize` and `format=csv` to all five report HTTP endpoints.

**Architecture:** The prefix grouping is a new `left(destination, n)` key in the existing ClickHouse summary query, validated at the gRPC/HTTP edge. CSV is a presentation concern handled entirely in the reports HTTP facade: a shared streaming helper pages the existing `API` methods and writes `encoding/csv`, so tenant scoping and filters stay single-sourced.

**Tech Stack:** Go 1.27, ClickHouse (clickhouse-go v2), gRPC/protobuf, stdlib `net/http` and `encoding/csv`.

## Global Constraints

- Money is integer minor units (1/10000 currency); CSV writes raw integers, never floats or formatting.
- No new services, ports, environment variables or dependencies.
- Regenerate stubs only with `make protoc` from the repo root (protoc and plugins live in `/root/go/bin`); never hand-edit `gen/`.
- Tests hit the real dev stack: Postgres, ClickHouse and Kafka must be up (`make up`).
- `make test` runs `go vet ./... && go test ./...` per module; it must exit 0.
- `make smoke` must print `smoke: PASS`.
- Test suites truncate dev data; run live verification before `make test`.
- Commits: lowercase conventional messages exactly as given per task.

---

### Task 1: Prefix grouping

**Files:**
- Modify: `proto/reports/v1/reports.proto`
- Regenerate: `services/reports/gen/reportspb/reports.pb.go`, `services/reports/gen/reportspb/reports_grpc.pb.go`
- Modify: `services/reports/store.go:34-41`, `services/reports/store.go:183-207`
- Modify: `services/reports/grpc.go:155-185` (`parseGroup`, `Summarize`)
- Modify: `services/reports/http.go:283-296` (`groupFromString`), `services/reports/http.go:227-282` (`Summarize` handler)
- Test: `services/reports/store_test.go`, `services/reports/grpc_test.go`, `services/reports/http_test.go`

**Interfaces:**
- Consumes: `Store.Summarize(ctx context.Context, f Filter, g GroupBy) ([]SummaryRow, error)`.
- Produces: `GroupPrefix GroupBy = "prefix"`; `Store.Summarize(ctx context.Context, f Filter, g GroupBy, prefixLen int) ([]SummaryRow, error)`; `pb.GroupBy_PREFIX`; `pb.SummarizeRequest.PrefixLen`.

- [ ] **Step 1: Write the failing store test**

Append to `services/reports/store_test.go`:

```go
func TestSummarizePrefix(t *testing.T) {
	reset(t)
	day := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	seed(t,
		testRow("c1", "a1", "27820001", "trunk-1", "normal", day, 60, 10),
		testRow("c2", "a1", "27820002", "trunk-1", "normal", day, 30, 5),
		testRow("c3", "a2", "27", "trunk-2", "normal", day, 10, 3),
		testRow("c4", "a2", "3555999", "trunk-2", "normal", day, 20, 2),
	)
	f := Filter{From: day, To: day.Add(24 * time.Hour)}
	rows, err := store.Summarize(context.Background(), f, GroupPrefix, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %+v", rows)
	}
	if rows[0].Key != "278" || rows[0].AmountMinor != 15 || rows[0].Calls != 2 ||
		rows[1].Key != "27" || rows[2].Key != "355" {
		t.Fatalf("prefix rows = %+v", rows)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd services/reports && go test -run TestSummarizePrefix ./...`
Expected: build failure `too many arguments in call to store.Summarize` and `undefined: GroupPrefix`.

- [ ] **Step 3: Add the proto contract**

In `proto/reports/v1/reports.proto`, replace the `GroupBy` enum body:

```proto
enum GroupBy {
  GROUP_BY_UNSPECIFIED = 0;
  DAY = 1;
  DESTINATION = 2;
  ACCOUNT = 3;
  TRUNK = 4;
  PREFIX = 5;
}
```

Replace `SummarizeRequest` with:

```proto
message SummarizeRequest {
  CdrFilter filter = 1;
  GroupBy group_by = 2;
  int32 prefix_len = 3; // only for PREFIX; 0 = default 3, valid 1..15
}
```

Run `make protoc` from the repo root, then confirm: `grep -c "GroupBy_PREFIX\|GetPrefixLen" services/reports/gen/reportspb/*.go` (expect nonzero).

- [ ] **Step 4: Implement the store change**

In `services/reports/store.go`, add `GroupPrefix` to the constants:

```go
const (
	GroupDay         GroupBy = "day"
	GroupDestination GroupBy = "destination"
	GroupAccount     GroupBy = "account"
	GroupTrunk       GroupBy = "trunk"
	GroupPrefix      GroupBy = "prefix"
)
```

Add the prefix case to `keyExpr`:

```go
	case GroupTrunk:
		return "trunk_id", nil
	case GroupPrefix:
		return "left(destination, ?)", nil
```

Replace `Summarize`'s signature and argument assembly:

```go
func (s *Store) Summarize(ctx context.Context, f Filter, g GroupBy, prefixLen int) ([]SummaryRow, error) {
	key, err := keyExpr(g)
	if err != nil {
		return nil, err
	}
	where, args := f.where()
	if g == GroupPrefix {
		args = append([]any{uint8(prefixLen)}, args...)
	}
	q := `SELECT ` + key + ` AS k, count(), sum(charged_seconds), sum(amount_minor)
		FROM reports.cdrs FINAL
		WHERE ` + where + `
		GROUP BY k
		ORDER BY sum(amount_minor) DESC, k ASC`
```

The rest of the function body is unchanged.

- [ ] **Step 5: Update the existing store test call sites**

In `services/reports/store_test.go`, inside `TestSummarizeGroupsAndTotals`, change the three calls to pass `0`:

```go
	byDay, err := store.Summarize(ctx, f, GroupDay, 0)
```

```go
	byDest, err := store.Summarize(ctx, f, GroupDestination, 0)
```

```go
	if _, err := store.Summarize(ctx, f, GroupBy("bogus"), 0); err == nil {
```

- [ ] **Step 6: Run the store tests**

Run: `cd services/reports && go test -run 'TestSummarize' ./...`
Expected: PASS.

- [ ] **Step 7: Write the failing gRPC test**

Append to `services/reports/grpc_test.go`:

```go
func TestSummarizePrefixLen(t *testing.T) {
	reset(t)
	day := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	seed(t,
		testRow("s1", "a1", "27820001", "trunk-1", "normal", day, 60, 10),
		testRow("s2", "a1", "3555999", "trunk-1", "normal", day, 30, 5),
	)
	srv := &reportsServer{store: store}
	filter := &pb.CdrFilter{From: "2026-11-12", To: "2026-11-13"}

	def, err := srv.Summarize(context.Background(), &pb.SummarizeRequest{
		Filter: filter, GroupBy: pb.GroupBy_PREFIX})
	if err != nil {
		t.Fatal(err)
	}
	if len(def.GetRows()) != 2 || def.GetRows()[0].GetKey() != "278" {
		t.Fatalf("default prefix = %+v", def.GetRows())
	}

	two, err := srv.Summarize(context.Background(), &pb.SummarizeRequest{
		Filter: filter, GroupBy: pb.GroupBy_PREFIX, PrefixLen: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(two.GetRows()) != 2 || two.GetRows()[0].GetKey() != "27" {
		t.Fatalf("prefix_len 2 = %+v", two.GetRows())
	}

	for _, n := range []int32{-1, 16} {
		if _, err := srv.Summarize(context.Background(), &pb.SummarizeRequest{
			Filter: filter, GroupBy: pb.GroupBy_PREFIX, PrefixLen: n}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("prefix_len %d err = %v, want InvalidArgument", n, err)
		}
	}
}
```

- [ ] **Step 8: Run it to verify it fails**

Run: `cd services/reports && go test -run TestSummarizePrefixLen ./...`
Expected: FAIL — `pb.GroupBy_PREFIX` is defined after codegen, but `Summarize` does not understand it yet (`InvalidArgument` or wrong keys).

- [ ] **Step 9: Implement the gRPC change**

In `services/reports/grpc.go`, extend the constants:

```go
const (
	defaultLimit     = 100
	maxLimit         = 1000
	defaultPrefixLen = 3
	maxPrefixLen     = 15
)
```

Add the prefix case to `parseGroup`:

```go
	case pb.GroupBy_TRUNK:
		return GroupTrunk, nil
	case pb.GroupBy_PREFIX:
		return GroupPrefix, nil
	}
```

Add the validator after `parseGroup`:

```go
func parsePrefixLen(g GroupBy, n int32) (int, error) {
	if g != GroupPrefix {
		return 0, nil
	}
	if n == 0 {
		return defaultPrefixLen, nil
	}
	if n < 1 || n > maxPrefixLen {
		return 0, errors.New("prefix_len must be 1..15")
	}
	return int(n), nil
}
```

In `Summarize`, after the `parseGroup` block add:

```go
	prefixLen, err := parsePrefixLen(group, req.GetPrefixLen())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
```

Add a span attribute next to `group_by`:

```go
	span.SetAttributes(attribute.Int("prefix_len", prefixLen))
```

Change the store call:

```go
	rows, err := s.store.Summarize(ctx, f, group, prefixLen)
```

- [ ] **Step 10: Run the gRPC tests**

Run: `cd services/reports && go test -run 'TestSummarize' ./...`
Expected: PASS.

- [ ] **Step 11: Write the failing HTTP test**

Append to `services/reports/http_test.go`:

```go
func TestHTTPPrefixGrouping(t *testing.T) {
	reset(t)
	day := time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)
	seed(t, testRow("h1", "a1", "27820001", "trunk-1", "normal", day, 60, 10))
	srv := facade(t)

	code, body := get(t, srv.URL+"/reports/summary?group_by=prefix&prefix_len=2&from=2026-11-17&to=2026-11-18", "reseller")
	if code != http.StatusOK {
		t.Fatalf("code = %d (%v)", code, body)
	}
	rows := body["rows"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["key"] != "27" {
		t.Fatalf("rows = %v", rows)
	}

	if code, _ := get(t, srv.URL+"/reports/summary?group_by=prefix&prefix_len=abc&from=2026-11-17&to=2026-11-18", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("bad prefix_len = %d, want 400", code)
	}
}
```

- [ ] **Step 12: Run it to verify it fails**

Run: `cd services/reports && go test -run TestHTTPPrefixGrouping ./...`
Expected: FAIL — `group_by=prefix` returns 400 `invalid group_by`.

- [ ] **Step 13: Implement the HTTP change**

In `services/reports/http.go`, add the prefix case to `groupFromString`:

```go
	case "trunk":
		return pb.GroupBy_TRUNK, true
	case "prefix":
		return pb.GroupBy_PREFIX, true
	}
```

In the `Summarize` handler, after `GroupBy: group,` in the request literal, read `prefix_len` (place this block after the request literal, before the `account_id` intersect block):

```go
	if v := q.Get("prefix_len"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid prefix_len")
			return
		}
		req.PrefixLen = int32(n)
	}
```

- [ ] **Step 14: Run the module tests**

Run: `cd services/reports && go vet ./... && go test ./...`
Expected: `ok omnifoni/reports`.

- [ ] **Step 15: Commit**

```bash
git add proto/reports/v1/reports.proto services/reports
git commit -m "feat(reports): add prefix grouping to summary"
```

---

### Task 2: CSV export for CDR list and summary

**Files:**
- Modify: `services/reports/http.go` (imports, helper, `ListCdrs`, `Summarize`)
- Test: `services/reports/http_test.go`

**Interfaces:**
- Consumes: `API.ListCdrs`, `API.Summarize`, `grpcStatus`, `writeErr`, `writeJSON`.
- Produces: `csvRequested(r *http.Request) (bool, bool)`, `streamCSV(w, r, report, header, fetch, tail)`, `writeCSVHeaderOnly(w, report, header)`, `csvMaxRows = 10000`, `csvPageSize = 1000`.

- [ ] **Step 1: Write the failing CSV tests**

Append to `services/reports/http_test.go`:

```go
func getRaw(t *testing.T, url, token string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, string(b)
}

func TestHTTPCSVCDRsAndSummary(t *testing.T) {
	reset(t)
	day := time.Date(2026, 11, 17, 8, 0, 0, 0, time.UTC)
	seed(t,
		testRow("csv1", "a1", "27820001", "trunk-1", "normal", day, 60, 10),
		testRow("csv2", "a1", "27820002", "trunk-1", "normal", day.Add(time.Hour), 30, 5),
	)
	srv := facade(t)
	window := "from=2026-11-17&to=2026-11-18"

	code, hdr, body := getRaw(t, srv.URL+"/reports/cdrs?"+window+"&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("cdrs = %d (%s)", code, body)
	}
	if ct := hdr.Get("Content-Type"); ct != "text/csv; charset=utf-8" {
		t.Fatalf("content-type = %q", ct)
	}
	if cd := hdr.Get("Content-Disposition"); !strings.Contains(cd, `filename="reports-cdrs-`) {
		t.Fatalf("content-disposition = %q", cd)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines = %d (%q)", len(lines), body)
	}
	wantHeader := "call_id,account_id,destination,cli,started_at_ms,ended_at_ms,charged_seconds,end_reason,trunk_id,rate_per_sec,amount_minor,rate_missing"
	if lines[0] != wantHeader {
		t.Fatalf("header = %q", lines[0])
	}
	if !strings.HasPrefix(lines[1], "csv2,a1,27820002,") || !strings.HasSuffix(lines[1], ",5,false") {
		t.Fatalf("row = %q", lines[1])
	}

	code, _, body = getRaw(t, srv.URL+"/reports/summary?group_by=day&"+window+"&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("summary = %d (%s)", code, body)
	}
	lines = strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 3 || lines[0] != "key,calls,seconds,amount_minor" {
		t.Fatalf("summary lines = %q", lines)
	}
	if lines[2] != "TOTAL,2,90,15" {
		t.Fatalf("totals row = %q", lines[2])
	}

	code, hdr, body = getRaw(t, srv.URL+"/reports/cdrs?"+window+"&format=json", "reseller")
	if code != http.StatusOK || !strings.HasPrefix(hdr.Get("Content-Type"), "application/json") ||
		!strings.Contains(body, `"cdrs"`) {
		t.Fatalf("json = %d %q %q", code, hdr.Get("Content-Type"), body)
	}

	if code, _, _ := getRaw(t, srv.URL+"/reports/cdrs?"+window+"&format=bogus", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("bogus format = %d, want 400", code)
	}
}
```

Add a fake API that proves the cap (append to `services/reports/http_test.go`):

```go
type pagingAPI struct {
	API
	fetched []string
}

func (p *pagingAPI) ListCdrs(_ context.Context, req *pb.ListCdrsRequest) (*pb.ListCdrsResponse, error) {
	p.fetched = append(p.fetched, req.GetCursor())
	rows := make([]*pb.Cdr, 0, 1000)
	for i := 0; i < 1000; i++ {
		rows = append(rows, &pb.Cdr{CallId: fmt.Sprintf("%s-%04d", req.GetCursor(), i), AccountId: "a1"})
	}
	next := ""
	if req.GetCursor() != "p9" {
		next = "p" + strconv.Itoa(len(p.fetched))
	}
	return &pb.ListCdrsResponse{Cdrs: rows, NextCursor: next}, nil
}

func TestHTTPCSVCap(t *testing.T) {
	api := &pagingAPI{}
	mux := http.NewServeMux()
	h := &httpServer{api: api, auth: NewAuthenticator(fakeIdentity(t).URL)}
	mux.HandleFunc("GET /reports/cdrs", h.ListCdrs)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	code, _, body := getRaw(t, srv.URL+"/reports/cdrs?from=2026-11-17&to=2026-11-18&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 10001 {
		t.Fatalf("lines = %d, want 10001 (header + 10000 rows)", len(lines))
	}
	if len(api.fetched) != 10 {
		t.Fatalf("fetched %d pages, want 10", len(api.fetched))
	}
}
```

Add these imports to `services/reports/http_test.go`:

```go
	"context"
	"io"
	"strconv"
	"strings"

	pb "omnifoni/reports/gen/reportspb"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd services/reports && go test -run 'TestHTTPCSV' ./...`
Expected: FAIL — `undefined: getRaw` is resolved by the test file, but the requests return JSON, so `content-type` and line-count assertions fail.

- [ ] **Step 3: Implement the CSV helper**

In `services/reports/http.go`, extend the imports:

```go
import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "omnifoni/reports/gen/reportspb"
)
```

Add the helper block after `intersect`:

```go
const (
	csvMaxRows  = 10000
	csvPageSize = 1000
)

func csvRequested(r *http.Request) (bool, bool) {
	switch r.URL.Query().Get("format") {
	case "", "json":
		return false, true
	case "csv":
		return true, true
	}
	return false, false
}

func csvHeaders(w http.ResponseWriter, report string) {
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition",
		fmt.Sprintf(`attachment; filename="reports-%s-%s.csv"`, report, time.Now().UTC().Format("2006-01-02")))
}

func writeCSVHeaderOnly(w http.ResponseWriter, report string, header []string) {
	csvHeaders(w, report)
	cw := csv.NewWriter(w)
	_ = cw.Write(header)
	cw.Flush()
}

// streamCSV fetches the first page before writing anything, so argument, auth
// and source errors still produce a normal JSON status. Later failures abort
// the response after logging. tail, when non-nil, is written after the rows.
func streamCSV(w http.ResponseWriter, r *http.Request, report string, header []string,
	fetch func(cursor string) ([][]string, string, error), tail func() []string) {
	rows, next, err := fetch("")
	if err != nil {
		writeErr(w, grpcStatus(err), err.Error())
		return
	}
	csvHeaders(w, report)
	cw := csv.NewWriter(w)
	_ = cw.Write(header)
	written := 0
	for {
		for _, row := range rows {
			if written >= csvMaxRows {
				break
			}
			_ = cw.Write(row)
			written++
		}
		cw.Flush()
		if written >= csvMaxRows || next == "" {
			break
		}
		rows, next, err = fetch(next)
		if err != nil {
			slog.ErrorContext(r.Context(), "csv stream aborted", "report", report, "err", err)
			return
		}
	}
	if tail != nil {
		_ = cw.Write(tail())
		cw.Flush()
	}
}
```

- [ ] **Step 4: Add the CDR CSV header and branch**

Add near `cdrJSON`:

```go
var cdrCSVHeader = []string{
	"call_id", "account_id", "destination", "cli", "started_at_ms", "ended_at_ms",
	"charged_seconds", "end_reason", "trunk_id", "rate_per_sec", "amount_minor", "rate_missing",
}
```

In `ListCdrs`, after the `ids` error check and before `if len(ids) == 0`, add:

```go
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
```

Replace both empty-result early returns in `ListCdrs` (the empty scope and the empty intersect) with:

```go
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "cdrs", cdrCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, listJSON{Cdrs: []cdrJSON{}})
		return
	}
```

```go
		if len(req.Filter.AccountIds) == 0 {
			if csvOut {
				writeCSVHeaderOnly(w, "cdrs", cdrCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, listJSON{Cdrs: []cdrJSON{}})
			return
		}
```

Immediately before the existing `resp, err := h.api.ListCdrs(...)` line, add:

```go
	if csvOut {
		streamCSV(w, r, "cdrs", cdrCSVHeader, func(cursor string) ([][]string, string, error) {
			req.Cursor = cursor
			req.Limit = csvPageSize
			resp, err := h.api.ListCdrs(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			rows := make([][]string, 0, len(resp.GetCdrs()))
			for _, c := range resp.GetCdrs() {
				rows = append(rows, []string{
					c.GetCallId(), c.GetAccountId(), c.GetDestination(), c.GetCli(),
					strconv.FormatInt(c.GetStartedAtMs(), 10), strconv.FormatInt(c.GetEndedAtMs(), 10),
					strconv.FormatInt(c.GetChargedSeconds(), 10), c.GetEndReason(), c.GetTrunkId(),
					c.GetRatePerSec(), strconv.FormatInt(c.GetAmountMinor(), 10),
					strconv.FormatBool(c.GetRateMissing()),
				})
			}
			return rows, resp.GetNextCursor(), nil
		}, nil)
		return
	}
```

- [ ] **Step 5: Add the summary CSV header and branch**

Add near `summaryJSON`:

```go
var summaryCSVHeader = []string{"key", "calls", "seconds", "amount_minor"}
```

In `Summarize`, after the `ids` error check and before `if len(ids) == 0`, add the same `csvRequested` block:

```go
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
```

Replace both empty-result early returns in `Summarize` with:

```go
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "summary", summaryCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, summaryJSON{Rows: []rowJSON{}, Totals: rowJSON{}})
		return
	}
```

```go
		if len(req.Filter.AccountIds) == 0 {
			if csvOut {
				writeCSVHeaderOnly(w, "summary", summaryCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, summaryJSON{Rows: []rowJSON{}, Totals: rowJSON{}})
			return
		}
```

Immediately before the existing `resp, err := h.api.Summarize(...)` line, add:

```go
	if csvOut {
		var totals rowJSON
		streamCSV(w, r, "summary", summaryCSVHeader, func(string) ([][]string, string, error) {
			resp, err := h.api.Summarize(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			totals = rowJSON{
				Calls: resp.GetTotals().GetCalls(), Seconds: resp.GetTotals().GetSeconds(),
				AmountMinor: resp.GetTotals().GetAmountMinor(),
			}
			rows := make([][]string, 0, len(resp.GetRows()))
			for _, row := range resp.GetRows() {
				rows = append(rows, []string{
					row.GetKey(), strconv.FormatInt(row.GetCalls(), 10),
					strconv.FormatInt(row.GetSeconds(), 10), strconv.FormatInt(row.GetAmountMinor(), 10),
				})
			}
			return rows, "", nil
		}, func() []string {
			return []string{"TOTAL", strconv.FormatInt(totals.Calls, 10),
				strconv.FormatInt(totals.Seconds, 10), strconv.FormatInt(totals.AmountMinor, 10)}
		})
		return
	}
```

- [ ] **Step 6: Run the tests**

Run: `cd services/reports && go test -run 'TestHTTP' ./...`
Expected: PASS, including `TestHTTPCSVCap`.

- [ ] **Step 7: Run the module tests and commit**

Run: `cd services/reports && go vet ./... && go test ./...`
Expected: `ok omnifoni/reports`.

```bash
git add services/reports/http.go services/reports/http_test.go
git commit -m "feat(reports): add csv export for cdr reports"
```

---

### Task 3: CSV export for money reports

**Files:**
- Modify: `services/reports/http.go` (`Payments`, `Charges`, `LowBalance`)
- Test: `services/reports/http_test.go`

**Interfaces:**
- Consumes: `csvRequested`, `streamCSV`, `writeCSVHeaderOnly` from Task 2; `API.ListPayments`, `API.ListCharges`, `API.ListLowBalance`.
- Produces: CSV on `/reports/payments`, `/reports/charges`, `/reports/low-balance`.

- [ ] **Step 1: Write the failing tests**

Append to `services/reports/http_test.go`:

```go
func TestHTTPCSVMoneyReports(t *testing.T) {
	srv := facade(t)
	q := "from=2026-11-17&to=2026-11-18"

	code, _, body := getRaw(t, srv.URL+"/reports/payments?"+q+"&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("payments = %d (%s)", code, body)
	}
	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 2 || lines[0] != "payment_id,event_id,account_id,gateway,amount_minor,status,created_at_ms" {
		t.Fatalf("payments lines = %q", lines)
	}
	if lines[1] != "7,evt,a1,stripe,500,recorded,1000" {
		t.Fatalf("payments row = %q", lines[1])
	}

	code, _, body = getRaw(t, srv.URL+"/reports/charges?"+q+"&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("charges = %d (%s)", code, body)
	}
	lines = strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 2 || lines[0] != "session_id,account_id,charged_seconds,amount_minor,balance_after,last_charged_at_ms" {
		t.Fatalf("charges lines = %q", lines)
	}
	if lines[1] != "s1,a1,3,-4,996,2000" {
		t.Fatalf("charges row = %q", lines[1])
	}

	code, _, body = getRaw(t, srv.URL+"/reports/low-balance?threshold=-5&format=csv", "reseller")
	if code != http.StatusOK {
		t.Fatalf("low balance = %d (%s)", code, body)
	}
	lines = strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 2 || lines[0] != "account_id,kind,balance,credit_limit,currency" {
		t.Fatalf("low balance lines = %q", lines)
	}
	if lines[1] != "a1,prepaid,0,0,ZAR" {
		t.Fatalf("low balance row = %q", lines[1])
	}

	if code, _, _ := getRaw(t, srv.URL+"/reports/payments?"+q+"&format=bogus", "reseller"); code != http.StatusBadRequest {
		t.Fatalf("bogus format = %d, want 400", code)
	}
	if code, _, _ := getRaw(t, srv.URL+"/reports/charges?"+q+"&format=csv", "user"); code != http.StatusForbidden {
		t.Fatalf("user csv = %d, want 403", code)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd services/reports && go test -run TestHTTPCSVMoneyReports ./...`
Expected: FAIL — money endpoints still return JSON.

- [ ] **Step 3: Implement the payments CSV branch**

Add near `paymentsJSON`:

```go
var paymentCSVHeader = []string{
	"payment_id", "event_id", "account_id", "gateway", "amount_minor", "status", "created_at_ms",
}
```

In `Payments`, after the `ids` error check and before `if len(ids) == 0`, add the `csvRequested` block:

```go
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
```

Replace both empty-result early returns with:

```go
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "payments", paymentCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, paymentsJSON{Payments: []paymentJSON{}})
		return
	}
```

```go
		if len(req.Filter.AccountIds) == 0 {
			if csvOut {
				writeCSVHeaderOnly(w, "payments", paymentCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, paymentsJSON{Payments: []paymentJSON{}})
			return
		}
```

Immediately before `resp, err := h.api.ListPayments(...)`, add:

```go
	if csvOut {
		streamCSV(w, r, "payments", paymentCSVHeader, func(cursor string) ([][]string, string, error) {
			req.Cursor = cursor
			req.Limit = csvPageSize
			resp, err := h.api.ListPayments(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			rows := make([][]string, 0, len(resp.GetPayments()))
			for _, p := range resp.GetPayments() {
				rows = append(rows, []string{
					strconv.FormatInt(p.GetPaymentId(), 10), p.GetEventId(), p.GetAccountId(),
					p.GetGateway(), strconv.FormatInt(p.GetAmountMinor(), 10), p.GetStatus(),
					strconv.FormatInt(p.GetCreatedAtMs(), 10),
				})
			}
			return rows, resp.GetNextCursor(), nil
		}, nil)
		return
	}
```

- [ ] **Step 4: Implement the charges CSV branch**

Add near `chargesJSON`:

```go
var chargeCSVHeader = []string{
	"session_id", "account_id", "charged_seconds", "amount_minor", "balance_after", "last_charged_at_ms",
}
```

In `Charges`, after the `ids` error check and before `if len(ids) == 0`, add:

```go
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
```

Replace both empty-result early returns with:

```go
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "charges", chargeCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, chargesJSON{Charges: []chargeJSON{}})
		return
	}
```

```go
		if len(req.Filter.AccountIds) == 0 {
			if csvOut {
				writeCSVHeaderOnly(w, "charges", chargeCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, chargesJSON{Charges: []chargeJSON{}})
			return
		}
```

Immediately before `resp, err := h.api.ListCharges(...)`, add:

```go
	if csvOut {
		streamCSV(w, r, "charges", chargeCSVHeader, func(cursor string) ([][]string, string, error) {
			req.Cursor = cursor
			req.Limit = csvPageSize
			resp, err := h.api.ListCharges(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			rows := make([][]string, 0, len(resp.GetCharges()))
			for _, c := range resp.GetCharges() {
				rows = append(rows, []string{
					c.GetSessionId(), c.GetAccountId(),
					strconv.FormatInt(c.GetChargedSeconds(), 10), strconv.FormatInt(c.GetAmountMinor(), 10),
					strconv.FormatInt(c.GetBalanceAfter(), 10), strconv.FormatInt(c.GetLastChargedAtMs(), 10),
				})
			}
			return rows, resp.GetNextCursor(), nil
		}, nil)
		return
	}
```

- [ ] **Step 5: Implement the low-balance CSV branch**

Add near `lowBalanceJSON`:

```go
var balanceCSVHeader = []string{"account_id", "kind", "balance", "credit_limit", "currency"}
```

In `LowBalance`, after the `ids` error check and before `if len(ids) == 0`, add:

```go
	csvOut, ok := csvRequested(r)
	if !ok {
		writeErr(w, http.StatusBadRequest, "invalid format")
		return
	}
```

Replace both empty-result early returns with:

```go
	if len(ids) == 0 {
		if csvOut {
			writeCSVHeaderOnly(w, "low-balance", balanceCSVHeader)
			return
		}
		writeJSON(w, http.StatusOK, lowBalanceJSON{Accounts: []balanceJSON{}})
		return
	}
```

```go
		if len(req.AccountIds) == 0 {
			if csvOut {
				writeCSVHeaderOnly(w, "low-balance", balanceCSVHeader)
				return
			}
			writeJSON(w, http.StatusOK, lowBalanceJSON{Accounts: []balanceJSON{}})
			return
		}
```

Immediately before `resp, err := h.api.ListLowBalance(...)`, add:

```go
	if csvOut {
		streamCSV(w, r, "low-balance", balanceCSVHeader, func(cursor string) ([][]string, string, error) {
			req.Cursor = cursor
			req.Limit = csvPageSize
			resp, err := h.api.ListLowBalance(r.Context(), req)
			if err != nil {
				return nil, "", err
			}
			rows := make([][]string, 0, len(resp.GetAccounts()))
			for _, a := range resp.GetAccounts() {
				rows = append(rows, []string{
					a.GetAccountId(), a.GetKind(), strconv.FormatInt(a.GetBalance(), 10),
					strconv.FormatInt(a.GetCreditLimit(), 10), a.GetCurrency(),
				})
			}
			return rows, resp.GetNextCursor(), nil
		}, nil)
		return
	}
```

- [ ] **Step 6: Run the tests**

Run: `cd services/reports && go vet ./... && go test ./...`
Expected: `ok omnifoni/reports`.

- [ ] **Step 7: Commit**

```bash
git add services/reports/http.go services/reports/http_test.go
git commit -m "feat(reports): add csv export for money reports"
```

---

### Task 4: Smoke assertion, docs, live verification and slice close

**Files:**
- Modify: `ops/smoke.sh`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: deployed reports HTTP facade on `:50062`, identity DB.
- Produces: verified slice acceptance.

- [ ] **Step 1: Add the CSV assertion to smoke**

In `ops/smoke.sh`, immediately after the `for s in ...` service loop and before the `(cd services/reconciliation ...)` probe line, add:

```bash
tid=$(docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "SELECT tenant_id FROM tenants WHERE name='smoke' LIMIT 1")
if [ -z "$tid" ]; then
  tid=$(docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "INSERT INTO tenants(name) VALUES ('smoke') RETURNING tenant_id")
fi
uid=$(docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "INSERT INTO users(tenant_id,email,password_hash,role) VALUES ('$tid','smoke@local','x','admin') ON CONFLICT (email) DO UPDATE SET tenant_id=EXCLUDED.tenant_id RETURNING user_id")
docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "INSERT INTO sessions(token,user_id,expires_at) VALUES ('tok-smoke','$uid',now()+interval '1 hour') ON CONFLICT (token) DO UPDATE SET expires_at=now()+interval '1 hour'" >/dev/null
hdr=$(curl -fsS -H 'Authorization: Bearer tok-smoke' "http://localhost:50062/reports/cdrs?from=2026-01-01&to=2026-01-02&tenant_id=$tid&format=csv" | head -1)
[ "$hdr" = "call_id,account_id,destination,cli,started_at_ms,ended_at_ms,charged_seconds,end_reason,trunk_id,rate_per_sec,amount_minor,rate_missing" ] || fail "reports csv"
echo "ok: reports csv"
```

- [ ] **Step 2: Update AGENTS.md**

Append to the P5 slice-two bullet in `AGENTS.md` (same paragraph, new sentence at the end):

```markdown
P5 slice three: `Summarize` gains `PREFIX` grouping with `prefix_len` (default 3, range 1–15) keyed on `left(destination, n)`; every report endpoint accepts `format=csv` (header uses the JSON field names, summary appends a `TOTAL` row, 10,000-data-row cap via internal 1000-row pages with no cursor).
```

- [ ] **Step 3: Rebuild and restart the reports service**

Run:

```bash
docker compose build reports && docker compose up -d reports
```

Expected: image built, container started.

- [ ] **Step 4: Live-verify prefix grouping and CSV**

Run (uses the smoke session from Step 1):

```bash
cd /root/Omni-Foní
tid=$(docker exec omni-fon-postgres-1 psql -U postgres -d identity -qAt -c "SELECT tenant_id FROM tenants WHERE name='smoke' LIMIT 1")
/root/go/bin/grpcurl -plaintext -d '{"filter":{"from":"2026-11-01","to":"2026-12-01"},"group_by":"PREFIX","prefix_len":2}' localhost:50061 reports.v1.Reports/Summarize
curl -s -H 'Authorization: Bearer tok-smoke' "http://localhost:50062/reports/summary?group_by=prefix&prefix_len=2&from=2026-11-01&to=2026-12-01&tenant_id=$tid&format=csv"
curl -s -o /dev/null -w '%{http_code}\n' -H 'Authorization: Bearer tok-smoke' "http://localhost:50062/reports/cdrs?from=2026-11-01&to=2026-12-01&tenant_id=$tid&format=bogus"
```

Expected: summary keys are 2-digit prefixes; the CSV starts with `key,calls,seconds,amount_minor` and ends with a `TOTAL` line; the last request prints `400`.

- [ ] **Step 5: Run the full gate**

Run:

```bash
cd /root/Omni-Foní
make test > /tmp/make-test-prefix-csv.log 2>&1; echo "exit=$?"
grep -cE '^ok' /tmp/make-test-prefix-csv.log
grep -E '^FAIL' /tmp/make-test-prefix-csv.log | head
make smoke 2>&1 | tail -6
```

Expected: `exit=0`, `12` modules `ok`, no `FAIL` lines, and smoke ends with `ok: reports csv` and `smoke: PASS`.

- [ ] **Step 6: Commit**

```bash
git add ops/smoke.sh AGENTS.md
git commit -m "docs(reports): close the prefix and csv slice"
```
