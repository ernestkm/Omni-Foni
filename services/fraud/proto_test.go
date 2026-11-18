package main

import (
	"testing"

	"google.golang.org/protobuf/proto"

	fraudpb "omnifoni/fraud/gen/fraudpb"
)

func TestFraudProtoRoundTrip(t *testing.T) {
	in := &fraudpb.FraudAlert{
		EventId: "e-1",
		Alert: &fraudpb.Alert{
			AlertId:     "a-1",
			CallId:      "c-1",
			AccountId:   "acc-1",
			RiskScore:   70,
			RiskLevel:   "medium",
			Reasons:     []string{"risk_prefix"},
			RuleVersion: "v1",
			CreatedAtMs: 123,
			Status:      "new",
		},
	}
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out := &fraudpb.FraudAlert{}
	if err := proto.Unmarshal(raw, out); err != nil {
		t.Fatal(err)
	}
	if out.GetAlert().GetCallId() != "c-1" || out.GetAlert().GetRiskScore() != 70 {
		t.Fatalf("bad round trip: %+v", out)
	}
	if out.GetEventId() != "e-1" {
		t.Fatalf("bad event id: %+v", out)
	}
}
