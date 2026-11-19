package main

import (
	"strconv"
	"strings"
	"testing"
	"time"

	schedulingpb "omnifoni/notifications/gen/schedulingpb"
)

func validEvent(now time.Time) *schedulingpb.ReportReadyEvent {
	exp := now.Add(time.Hour).UnixMilli()
	return &schedulingpb.ReportReadyEvent{
		ScheduleId: "s1", ArtifactId: "a1", TenantId: "t1", ReportKind: "cdrs",
		PeriodFrom: "2026-11-01T00:00:00Z", PeriodTo: "2026-11-02T00:00:00Z",
		RowCount: 7, CreatedAtMs: now.UnixMilli(),
		RecipientEmail: "ops@example.test",
		ArtifactUrl:    "http://localhost:50062/scheduled-reports/s1/artifacts/a1/download?exp=" + strconv.FormatInt(exp, 10) + "&sig=abc",
		ExpiresAtMs:    exp,
	}
}

func TestNotificationFromEvent(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	ev := validEvent(now)
	n := notificationFromEvent(ev, now)
	if n.DedupeKey != "report-ready:a1:ops@example.test" {
		t.Fatalf("dedupe = %q", n.DedupeKey)
	}
	if n.Channel != "email" || n.Recipient != "ops@example.test" || n.Status != "pending" {
		t.Fatalf("notification = %+v", n)
	}
	if !strings.Contains(n.Body, ev.GetArtifactUrl()) {
		t.Fatalf("body missing url: %q", n.Body)
	}
}

func TestNotificationFromEventTerminalValidation(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		edit func(*schedulingpb.ReportReadyEvent)
		want string
	}{
		{"missing recipient", func(e *schedulingpb.ReportReadyEvent) { e.RecipientEmail = "" }, "missing recipient_email"},
		{"malformed url", func(e *schedulingpb.ReportReadyEvent) { e.ArtifactUrl = "not a url" }, "malformed artifact_url"},
		{"missing signature", func(e *schedulingpb.ReportReadyEvent) {
			e.ArtifactUrl = "http://localhost:50062/scheduled-reports/s1/artifacts/a1/download?exp=1"
		}, "missing artifact_url signature"},
		{"expiry mismatch", func(e *schedulingpb.ReportReadyEvent) { e.ExpiresAtMs++ }, "artifact_url expiry mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := validEvent(now)
			tc.edit(ev)
			n := notificationFromEvent(ev, now)
			if n.Status != "failed" || n.Attempts != 1 || !strings.Contains(n.LastError, tc.want) {
				t.Fatalf("notification = %+v", n)
			}
		})
	}
}

func TestRetryDelay(t *testing.T) {
	if got := retryDelay(1, time.Minute); got != time.Minute {
		t.Fatalf("attempt 1 = %s", got)
	}
	if got := retryDelay(3, time.Minute); got != 4*time.Minute {
		t.Fatalf("attempt 3 = %s", got)
	}
	if got := retryDelay(20, time.Minute); got != 32*time.Minute {
		t.Fatalf("attempt 20 = %s", got)
	}
}
