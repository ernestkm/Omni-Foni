package main

import (
	"testing"

	"google.golang.org/protobuf/proto"

	schedulingpb "omnifoni/reports/gen/schedulingpb"
)

func TestReportReadyEventRoundTrip(t *testing.T) {
	in := &schedulingpb.ReportReadyEvent{
		ScheduleId: "s1", ArtifactId: "a1", TenantId: "t1", ReportKind: "cdrs",
		PeriodFrom: "2026-11-01T00:00:00Z", PeriodTo: "2026-11-02T00:00:00Z",
		RowCount: 42, CreatedAtMs: 1763740800000,
	}
	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out schedulingpb.ReportReadyEvent
	if err := proto.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetScheduleId() != "s1" || out.GetArtifactId() != "a1" || out.GetTenantId() != "t1" ||
		out.GetReportKind() != "cdrs" || out.GetRowCount() != 42 || out.GetCreatedAtMs() != 1763740800000 {
		t.Fatalf("round trip mismatch: %+v", &out)
	}
}
