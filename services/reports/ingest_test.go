package main

import (
	"context"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	cataloguepb "omnifoni/catalogue/gen/cataloguepb"
	mediationpb "omnifoni/mediation/gen/mediationpb"
)

func catalogueClient(t *testing.T) cataloguepb.CatalogueClient {
	t.Helper()
	conn, err := grpc.NewClient("localhost:50053", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return cataloguepb.NewCatalogueClient(conn)
}

func putRate(t *testing.T, prefix, rate string) {
	t.Helper()
	if _, err := catalogueClient(t).PutRate(context.Background(),
		&cataloguepb.PutRateRequest{Prefix: prefix, RatePerSec: rate}); err != nil {
		t.Fatal(err)
	}
}

func testCdr(id, dest string, start time.Time, secs int64) *mediationpb.CallCdr {
	return &mediationpb.CallCdr{
		CallId: id, AccountId: "acc-reports", Destination: dest, Cli: "2721000",
		StartedAtMs: start.UnixMilli(), EndedAtMs: start.Add(time.Duration(secs) * time.Second).UnixMilli(),
		ChargedSeconds: secs, EndReason: "normal", TrunkId: "trunk-1",
	}
}

func record(t *testing.T, c *mediationpb.CallCdr) *kgo.Record {
	t.Helper()
	b, err := proto.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Value: b, Key: []byte(c.GetCallId())}
}

func onlyRow(t *testing.T, account string, start time.Time) CdrRow {
	t.Helper()
	rows, err := store.ListCdrs(context.Background(),
		Filter{AccountIDs: []string{account}, From: start.Add(-time.Hour), To: start.Add(time.Hour)}, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	return rows[0]
}

func TestHandleRatesAndInserts(t *testing.T) {
	reset(t)
	putRate(t, "99771", "0.012345")
	start := time.Date(2026, 11, 12, 8, 0, 0, 0, time.UTC)
	ing := NewIngester(store, catalogueClient(t))
	if err := ing.Handle(context.Background(), record(t, testCdr("ing-1", "99771123", start, 61))); err != nil {
		t.Fatal(err)
	}
	row := onlyRow(t, "acc-reports", start)
	if row.RatePerSec != "0.012345" || row.AmountMinor != 1 || row.RateMissing {
		t.Fatalf("row = %+v, want rate 0.012345 amount 1 missing false", row)
	}
	if row.TrunkID != "trunk-1" || row.EndReason != "normal" {
		t.Fatalf("row = %+v, want trunk/end_reason preserved", row)
	}
}

func TestHandleMissingRateStillInserts(t *testing.T) {
	reset(t)
	start := time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)
	ing := NewIngester(store, catalogueClient(t))
	if err := ing.Handle(context.Background(), record(t, testCdr("ing-2", "77700011122", start, 30))); err != nil {
		t.Fatal(err)
	}
	row := onlyRow(t, "acc-reports", start)
	if !row.RateMissing || row.AmountMinor != 0 || row.RatePerSec != "0" {
		t.Fatalf("row = %+v, want rate_missing true amount 0 rate 0", row)
	}
}

func TestHandleReplayIsIdempotent(t *testing.T) {
	reset(t)
	putRate(t, "99772", "0.5")
	start := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	ing := NewIngester(store, catalogueClient(t))
	rec := record(t, testCdr("ing-3", "99772001", start, 3))
	if err := ing.Handle(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := ing.Handle(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	row := onlyRow(t, "acc-reports", start)
	if row.AmountMinor != 2 { // 3 * 0.5 = 1.5 -> 2
		t.Fatalf("amount = %d, want 2", row.AmountMinor)
	}
}

func TestHandleSkipsInvalid(t *testing.T) {
	reset(t)
	start := time.Date(2026, 11, 12, 11, 0, 0, 0, time.UTC)
	ing := NewIngester(store, catalogueClient(t))
	bad := testCdr("ing-4", "99771000", start, 10)
	bad.AccountId = ""
	if err := ing.Handle(context.Background(), record(t, bad)); err != nil {
		t.Fatalf("invalid cdr returned error: %v", err)
	}
	rows, err := store.ListCdrs(context.Background(),
		Filter{From: start.Add(-time.Hour), To: start.Add(time.Hour)}, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows = %d, want 0", len(rows))
	}
}

func TestHandleRetriesOnCatalogueOutage(t *testing.T) {
	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	ing := NewIngester(store, cataloguepb.NewCatalogueClient(conn))
	start := time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC)
	if err := ing.Handle(context.Background(), record(t, testCdr("ing-5", "99773000", start, 10))); err == nil {
		t.Fatal("Handle with unreachable catalogue = nil error, want retryable error")
	}
}
