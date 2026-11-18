package main

import (
	"testing"

	sbcpb "omnifoni/sbc/gen/sbcpb"
)

func TestCheckCallRequestAccountID(t *testing.T) {
	req := &sbcpb.CheckCallRequest{
		CallId:      "c-1",
		AccountId:   "acc-1",
		Cli:         "2710001",
		Destination: "2782001",
		TrunkId:     "t1",
	}
	if req.GetAccountId() != "acc-1" {
		t.Fatalf("account id missing from request: %+v", req)
	}
}
