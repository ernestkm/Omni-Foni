package main

import (
	"testing"

	pb "omnifoni/mediation/gen/mediationpb"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		c    *pb.CallCdr
		want string
	}{
		{"valid zero-second call", &pb.CallCdr{CallId: "c1", AccountId: "a1", EndReason: "normal"}, ""},
		{"missing call id", &pb.CallCdr{AccountId: "a1", EndReason: "normal"}, "missing_call_id"},
		{"missing account id", &pb.CallCdr{CallId: "c1", EndReason: "normal"}, "missing_account_id"},
		{"negative seconds", &pb.CallCdr{CallId: "c1", AccountId: "a1", ChargedSeconds: -1, EndReason: "normal"}, "negative_charged_seconds"},
		{"unknown reason", &pb.CallCdr{CallId: "c1", AccountId: "a1", EndReason: "weird"}, "unknown_end_reason"},
	}
	for _, tc := range cases {
		if got := Validate(tc.c); got != tc.want {
			t.Errorf("%s: Validate = %q, want %q", tc.name, got, tc.want)
		}
	}
}
