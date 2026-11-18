package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

var store *Store

func testDSN() string {
	if s := os.Getenv("REPORTS_TEST_CLICKHOUSE"); s != "" {
		return s
	}
	return "clickhouse://omnifoni:dev-only@localhost:9000"
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	store, err = Open(ctx, testDSN())
	if err != nil {
		fmt.Fprintln(os.Stderr, "clickhouse:", err)
		os.Exit(1)
	}
	if err := store.Migrate(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	schedDB, err = OpenSched(ctx, schedTestDSN())
	if err != nil {
		fmt.Fprintln(os.Stderr, "sched db:", err)
		os.Exit(1)
	}
	if err := schedDB.Migrate(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "sched migrate:", err)
		os.Exit(1)
	}
	code := m.Run()
	schedDB.Close()
	os.Exit(code)
}

func schedTestDSN() string {
	if s := os.Getenv("REPORTS_TEST_SCHED_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/reports_sched"
}

func reset(t *testing.T) {
	t.Helper()
	if err := store.db.Exec(context.Background(), "TRUNCATE TABLE reports.cdrs"); err != nil {
		t.Fatal(err)
	}
}

func testRow(id, acct, dest, trunk, reason string, start time.Time, secs, amount int64) CdrRow {
	return CdrRow{
		CallID: id, AccountID: acct, Destination: dest, CLI: "2721000",
		StartedAt: start, EndedAt: start.Add(time.Duration(secs) * time.Second),
		ChargedSeconds: secs, EndReason: reason, TrunkID: trunk,
		RatePerSec: "0.012345", AmountMinor: amount,
	}
}

func seed(t *testing.T, rows ...CdrRow) {
	t.Helper()
	if err := store.Insert(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
}

func TestInsertDedupsReplays(t *testing.T) {
	reset(t)
	start := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	seed(t, testRow("c1", "a1", "2782000", "trunk-1", "normal", start, 60, 10))
	time.Sleep(2 * time.Millisecond) // ensure a newer ingested_at version
	seed(t, testRow("c1", "a1", "2782000", "trunk-1", "normal", start, 60, 20))

	rows, err := store.ListCdrs(context.Background(),
		Filter{AccountIDs: []string{"a1"}, From: start.Add(-time.Hour), To: start.Add(time.Hour)}, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].AmountMinor != 20 {
		t.Fatalf("amount = %d, want 20 (newest version)", rows[0].AmountMinor)
	}
}

func TestListFiltersAndPagination(t *testing.T) {
	reset(t)
	start := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	seed(t,
		testRow("c1", "a1", "27820001", "trunk-1", "normal", start.Add(1*time.Minute), 60, 10),
		testRow("c2", "a1", "27820002", "trunk-1", "normal", start.Add(2*time.Minute), 60, 20),
		testRow("c3", "a1", "27820003", "trunk-1", "normal", start.Add(3*time.Minute), 60, 30),
		testRow("c4", "a2", "27820004", "trunk-1", "normal", start.Add(2*time.Minute), 60, 40),
		testRow("c5", "a1", "3555999", "trunk-2", "balance_exhausted", start.Add(2*time.Minute), 60, 50),
	)
	ctx := context.Background()
	base := Filter{AccountIDs: []string{"a1"}, From: start, To: start.Add(24 * time.Hour)}

	page1, err := store.ListCdrs(ctx, base, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 || page1[0].CallID != "c3" || page1[1].CallID != "c5" {
		t.Fatalf("page1 = %v, want [c3 c5]", ids(page1))
	}
	page2, err := store.ListCdrs(ctx, base, 2, &Cursor{StartedAt: page1[1].StartedAt, CallID: page1[1].CallID})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 || page2[0].CallID != "c2" || page2[1].CallID != "c1" {
		t.Fatalf("page2 = %v, want [c2 c1]", ids(page2))
	}

	prefix := base
	prefix.DestinationPrefix = "2782"
	rows, _ := store.ListCdrs(ctx, prefix, 10, nil)
	if len(rows) != 3 {
		t.Fatalf("prefix rows = %d, want 3", len(rows))
	}

	trunk := base
	trunk.TrunkID = "trunk-2"
	rows, _ = store.ListCdrs(ctx, trunk, 10, nil)
	if len(rows) != 1 || rows[0].CallID != "c5" {
		t.Fatalf("trunk rows = %v, want [c5]", ids(rows))
	}

	reason := base
	reason.EndReason = "balance_exhausted"
	rows, _ = store.ListCdrs(ctx, reason, 10, nil)
	if len(rows) != 1 || rows[0].CallID != "c5" {
		t.Fatalf("reason rows = %v, want [c5]", ids(rows))
	}

	window := base
	window.To = start.Add(2 * time.Minute) // half-open: c2/c3/c5 at 09:02 excluded
	rows, _ = store.ListCdrs(ctx, window, 10, nil)
	if len(rows) != 1 || rows[0].CallID != "c1" {
		t.Fatalf("window rows = %v, want [c1]", ids(rows))
	}
}

func ids(rows []CdrRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.CallID
	}
	return out
}

func TestSummarizeGroupsAndTotals(t *testing.T) {
	reset(t)
	day1 := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)
	seed(t,
		testRow("c1", "a1", "27820001", "trunk-1", "normal", day1, 60, 10),
		testRow("c2", "a1", "3555999", "trunk-2", "normal", day1, 30, 5),
		testRow("c3", "a2", "27820001", "trunk-1", "normal", day2, 60, 7),
	)
	ctx := context.Background()
	f := Filter{From: day1, To: day2.Add(24 * time.Hour)}

	byDay, err := store.Summarize(ctx, f, GroupDay, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(byDay) != 2 || byDay[0].Key != "2026-11-10" || byDay[0].AmountMinor != 15 ||
		byDay[0].Calls != 2 || byDay[0].Seconds != 90 || byDay[1].Key != "2026-11-11" {
		t.Fatalf("byDay = %+v", byDay)
	}

	byDest, err := store.Summarize(ctx, f, GroupDestination, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(byDest) != 2 || byDest[0].Key != "27820001" || byDest[0].AmountMinor != 17 {
		t.Fatalf("byDest = %+v", byDest)
	}

	if _, err := store.Summarize(ctx, f, GroupBy("bogus"), 0); err == nil {
		t.Fatal("Summarize with unknown group = nil error, want error")
	}
}

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
