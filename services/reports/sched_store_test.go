package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

var schedDB *SchedStore

func TestMainSched(t *testing.T) {
	if schedDB == nil {
		t.Fatal("sched store not initialized (see store_test.go TestMain)")
	}
}

func schedReset(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := schedDB.pool.Exec(ctx, "TRUNCATE report_artifacts, schedules"); err != nil {
		t.Fatal(err)
	}
}

func newTestSchedule(tenant string) Schedule {
	return Schedule{
		TenantID: tenant, AccountIDs: []string{"a1"}, ReportKind: "cdrs",
		WindowKind: "previous_month", GroupBy: "none", PrefixLen: 3,
		Frequency: "daily", AtHour: 6, AtMinute: 30, Enabled: true, CreatedBy: "tester",
	}
}

func TestSchedStoreCRUD(t *testing.T) {
	schedReset(t)
	ctx := context.Background()
	created, err := schedDB.CreateSchedule(ctx, newTestSchedule("t1"))
	if err != nil {
		t.Fatal(err)
	}
	if created.ScheduleID == "" || created.CreatedAt.IsZero() {
		t.Fatalf("bad created schedule: %+v", created)
	}
	got, err := schedDB.GetSchedule(ctx, created.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if got.TenantID != "t1" || got.ReportKind != "cdrs" || !got.Enabled || got.Weekday != nil {
		t.Fatalf("bad round trip: %+v", got)
	}
	got.ReportKind = "summary"
	got.GroupBy = "prefix"
	got.PrefixLen = 5
	if err := schedDB.UpdateSchedule(ctx, got); err != nil {
		t.Fatal(err)
	}
	again, _ := schedDB.GetSchedule(ctx, created.ScheduleID)
	if again.ReportKind != "summary" || again.PrefixLen != 5 {
		t.Fatalf("update not applied: %+v", again)
	}
	list, err := schedDB.ListSchedules(ctx, "t1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %v, %v", list, err)
	}
	enabled, err := schedDB.ListEnabledSchedules(ctx)
	if err != nil || len(enabled) != 1 {
		t.Fatalf("enabled = %v, %v", enabled, err)
	}
	if _, err := schedDB.GetSchedule(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing schedule err = %v", err)
	}
	if err := schedDB.DeleteSchedule(ctx, created.ScheduleID); err != nil {
		t.Fatal(err)
	}
	if _, err := schedDB.GetSchedule(ctx, created.ScheduleID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("after delete err = %v", err)
	}
}

func TestSchedStoreArtifacts(t *testing.T) {
	schedReset(t)
	ctx := context.Background()
	sch, err := schedDB.CreateSchedule(ctx, newTestSchedule("t1"))
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	a1, inserted, err := schedDB.InsertArtifact(ctx, Artifact{
		ScheduleID: sch.ScheduleID, SlotKey: "2026-11-17T06:30Z",
		PeriodFrom: from, PeriodTo: from.Add(24 * time.Hour), RowCount: 2,
	})
	if err != nil || !inserted {
		t.Fatalf("insert = %+v, %v, %v", a1, inserted, err)
	}
	if a1.ArtifactID == "" || a1.CreatedAt.IsZero() {
		t.Fatalf("bad artifact: %+v", a1)
	}
	_, inserted, err = schedDB.InsertArtifact(ctx, Artifact{
		ScheduleID: sch.ScheduleID, SlotKey: "2026-11-17T06:30Z",
		PeriodFrom: from, PeriodTo: from.Add(24 * time.Hour), RowCount: 99,
	})
	if err != nil || inserted {
		t.Fatalf("duplicate slot inserted: %v, %v", inserted, err)
	}
	list, err := schedDB.ListArtifacts(ctx, sch.ScheduleID)
	if err != nil || len(list) != 1 {
		t.Fatalf("list = %v, %v", list, err)
	}
	if _, _, err := schedDB.GetArtifact(ctx, sch.ScheduleID, a1.ArtifactID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := schedDB.GetArtifact(ctx, sch.ScheduleID, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("cross-artifact err = %v", err)
	}
}

func TestSchedStoreCascadeAndPrune(t *testing.T) {
	schedReset(t)
	ctx := context.Background()
	sch, err := schedDB.CreateSchedule(ctx, newTestSchedule("t1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := schedDB.InsertArtifact(ctx, Artifact{
		ScheduleID: sch.ScheduleID, SlotKey: "old", PeriodFrom: time.Now().Add(-48 * time.Hour),
		PeriodTo: time.Now(), RowCount: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := schedDB.pool.Exec(ctx,
		"UPDATE report_artifacts SET created_at = now() - interval '100 days' WHERE schedule_id = $1", sch.ScheduleID); err != nil {
		t.Fatal(err)
	}
	n, err := schedDB.PruneArtifacts(ctx, time.Now().AddDate(0, 0, -90))
	if err != nil || n != 1 {
		t.Fatalf("prune = %d, %v", n, err)
	}
	if err := schedDB.DeleteSchedule(ctx, sch.ScheduleID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := schedDB.pool.QueryRow(ctx, "SELECT count(*) FROM report_artifacts").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("cascade left %d artifacts", count)
	}
}