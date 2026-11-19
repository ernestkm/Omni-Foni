package main

import (
	"context"
	"testing"
	"time"

	schedulingpb "omnifoni/reports/gen/schedulingpb"
)

type fakePublisher struct {
	events []*schedulingpb.ReportReadyEvent
}

func (f *fakePublisher) PublishReportReady(_ context.Context, ev *schedulingpb.ReportReadyEvent) error {
	f.events = append(f.events, ev)
	return nil
}

func TestDueSchedule(t *testing.T) {
	monday := 1
	tuesday := 2
	dom := 31
	day17 := 17
	now := time.Date(2026, 11, 17, 6, 30, 0, 0, time.UTC) // Tuesday
	cases := []struct {
		name string
		s    Schedule
		want bool
	}{
		{"daily hit", Schedule{Enabled: true, Frequency: "daily", AtHour: 6, AtMinute: 30}, true},
		{"daily wrong minute", Schedule{Enabled: true, Frequency: "daily", AtHour: 6, AtMinute: 31}, false},
		{"disabled", Schedule{Enabled: false, Frequency: "daily", AtHour: 6, AtMinute: 30}, false},
		{"weekly hit (Tuesday=2)", Schedule{Enabled: true, Frequency: "weekly", AtHour: 6, AtMinute: 30, Weekday: &tuesday}, true},
		{"weekly miss (Monday)", Schedule{Enabled: true, Frequency: "weekly", AtHour: 6, AtMinute: 30, Weekday: &monday}, false},
		{"weekly miss (day number)", Schedule{Enabled: true, Frequency: "weekly", AtHour: 6, AtMinute: 30, Weekday: &dom}, false},
		{"monthly hit", Schedule{Enabled: true, Frequency: "monthly", AtHour: 6, AtMinute: 30, DayOfMonth: &day17}, true},
		{"monthly miss (Nov has 30 days)", Schedule{Enabled: true, Frequency: "monthly", AtHour: 6, AtMinute: 30, DayOfMonth: &dom}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := due(tc.s, now); got != tc.want {
				t.Fatalf("due = %v, want %v", got, tc.want)
			}
		})
	}
	feb := time.Date(2026, 2, 28, 6, 30, 0, 0, time.UTC)
	if !due(Schedule{Enabled: true, Frequency: "monthly", AtHour: 6, AtMinute: 30, DayOfMonth: &dom}, feb) {
		t.Fatal("31st should fire on the last day of February")
	}
}

func TestResolvePeriod(t *testing.T) {
	now := time.Date(2026, 11, 17, 6, 30, 0, 0, time.UTC)
	from, to, err := resolvePeriod("last_day", now)
	if err != nil || !from.Equal(now.Add(-24*time.Hour)) || !to.Equal(now) {
		t.Fatalf("last_day = %v, %v, %v", from, to, err)
	}
	from, to, err = resolvePeriod("last_week", now)
	if err != nil || !from.Equal(now.Add(-7*24*time.Hour)) || !to.Equal(now) {
		t.Fatalf("last_week = %v, %v, %v", from, to, err)
	}
	from, to, err = resolvePeriod("last_month", now)
	if err != nil || !from.Equal(now.Add(-30*24*time.Hour)) || !to.Equal(now) {
		t.Fatalf("last_month = %v, %v, %v", from, to, err)
	}
	from, to, err = resolvePeriod("previous_month", now)
	if err != nil || !from.Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) ||
		!to.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("previous_month = %v, %v, %v", from, to, err)
	}
	if _, _, err := resolvePeriod("bogus", now); err == nil {
		t.Fatal("expected error for unknown window")
	}
}

func TestRunDueSchedulesCreatesOneArtifact(t *testing.T) {
	schedReset(t)
	reset(t)
	ctx := context.Background()
	sch, err := schedDB.CreateSchedule(ctx, Schedule{
		TenantID: "t1", AccountIDs: []string{"a1"}, ReportKind: "cdrs",
		WindowKind: "last_day", GroupBy: "none", PrefixLen: 3,
		Frequency: "daily", AtHour: 6, AtMinute: 30, Enabled: true, CreatedBy: "tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 17, 6, 30, 0, 0, time.UTC)
	start := now.Add(-2 * time.Hour)
	seed(t, testRow("r1", "a1", "27820001", "trunk-1", "normal", start, 60, 10))
	pub := &fakePublisher{}
	signer := NewArtifactSigner("test-secret", "http://reports.test", time.Hour)
	s := NewScheduler(schedDB, &reportsServer{store: store}, pub, signer)
	if err := s.RunDueSchedules(ctx, now); err != nil {
		t.Fatal(err)
	}
	list, err := schedDB.ListArtifacts(ctx, sch.ScheduleID)
	if err != nil || len(list) != 1 {
		t.Fatalf("artifacts = %v, %v", list, err)
	}
	if list[0].RowCount != 1 {
		t.Fatalf("row_count = %d", list[0].RowCount)
	}
	if len(pub.events) != 1 || pub.events[0].GetScheduleId() != sch.ScheduleID ||
		pub.events[0].GetArtifactId() != list[0].ArtifactID || pub.events[0].GetRowCount() != 1 {
		t.Fatalf("events = %+v", pub.events)
	}
	if err := s.RunDueSchedules(ctx, now); err != nil {
		t.Fatal(err)
	}
	again, _ := schedDB.ListArtifacts(ctx, sch.ScheduleID)
	if len(again) != 1 {
		t.Fatalf("duplicate artifact on same slot: %d", len(again))
	}
	if len(pub.events) != 1 {
		t.Fatalf("duplicate event on same slot: %d", len(pub.events))
	}
}

func TestRunScheduleNowUsesManualSlot(t *testing.T) {
	schedReset(t)
	reset(t)
	ctx := context.Background()
	sch, err := schedDB.CreateSchedule(ctx, Schedule{
		TenantID: "t1", AccountIDs: []string{"a1"}, ReportKind: "cdrs",
		WindowKind: "last_day", GroupBy: "none", PrefixLen: 3,
		Frequency: "daily", AtHour: 0, AtMinute: 0, Enabled: true, CreatedBy: "tester",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	seed(t, testRow("r1", "a1", "27820001", "trunk-1", "normal", now.Add(-time.Minute), 60, 10))
	signer := NewArtifactSigner("test-secret", "http://reports.test", time.Hour)
	s := NewScheduler(schedDB, &reportsServer{store: store}, &fakePublisher{}, signer)
	a, err := s.RunScheduleNow(ctx, sch)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.SlotKey) < 7 || a.SlotKey[:7] != "manual-" {
		t.Fatalf("slot = %q", a.SlotKey)
	}
	if a.RowCount != 1 {
		t.Fatalf("row_count = %d", a.RowCount)
	}
	if _, body, err := schedDB.GetArtifact(ctx, sch.ScheduleID, a.ArtifactID); err != nil {
		t.Fatal(err)
	} else if body == "" {
		t.Fatal("empty csv body")
	}
}
