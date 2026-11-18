package main

import (
	"context"
	"testing"

	fraudpb "omnifoni/fraud/gen/fraudpb"
)

func reserveDirect(t *testing.T, s *Store, callID, account, destination, cli string, cfg Config) (*fraudpb.CheckCallResponse, *fraudpb.Alert) {
	t.Helper()
	resp, alert, err := s.Reserve(context.Background(), &fraudpb.CheckCallRequest{
		CallId: callID, AccountId: account, Destination: destination, Cli: cli,
	}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return resp, alert
}

func TestHardVelocityRuleAndEndCall(t *testing.T) {
	s := newTestStore(t)
	cfg := Config{AlertThreshold: 101, SoftAccountCalls: 5, SoftPrefixCalls: 2}
	insertControl(t, s, "acc-velocity", intPtr(1), nil, true)

	first, _ := reserveDirect(t, s, "v1", "acc-velocity", "2782001", "cli-a", cfg)
	if !first.GetAllowed() || first.GetReason() != "" {
		t.Fatalf("first call denied: %+v", first)
	}
	second, _ := reserveDirect(t, s, "v2", "acc-velocity", "2782002", "cli-b", cfg)
	if second.GetAllowed() || second.GetReason() != "fraud_velocity_account" {
		t.Fatalf("want account velocity deny, got %+v", second)
	}
	repeat, _ := reserveDirect(t, s, "v1", "acc-velocity", "2782001", "cli-a", cfg)
	if !repeat.GetAllowed() || repeat.GetReason() != "" {
		t.Fatalf("repeated check must be allowed: %+v", repeat)
	}
	if err := s.EndCall(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	if err := s.EndCall(context.Background(), "v1"); err != nil {
		t.Fatalf("idempotent EndCall failed: %v", err)
	}
	third, _ := reserveDirect(t, s, "v3", "acc-velocity", "2782003", "cli-c", cfg)
	if !third.GetAllowed() {
		t.Fatalf("after release denied: %+v", third)
	}
}

func TestDestinationAndCLIBlocks(t *testing.T) {
	cfg := Config{AlertThreshold: 101, SoftAccountCalls: 5, SoftPrefixCalls: 2}
	s := newTestStore(t)
	if _, err := s.pool.Exec(context.Background(),
		`INSERT INTO prefix_blocks(account_id, prefix) VALUES ('acc-dest', '9001')`); err != nil {
		t.Fatal(err)
	}
	res, _ := reserveDirect(t, s, "d1", "acc-dest", "9001999", "cli", cfg)
	if res.GetAllowed() || res.GetReason() != "fraud_destination" {
		t.Fatalf("want fraud_destination, got %+v", res)
	}

	s = newTestStore(t)
	if _, err := s.pool.Exec(context.Background(),
		`INSERT INTO cli_blocks(account_id, prefix) VALUES ('acc-cli', '1888')`); err != nil {
		t.Fatal(err)
	}
	res, _ = reserveDirect(t, s, "c1", "acc-cli", "2782001", "1888555", cfg)
	if res.GetAllowed() || res.GetReason() != "fraud_cli" {
		t.Fatalf("want fraud_cli, got %+v", res)
	}
}

func TestPrefixVelocityBlock(t *testing.T) {
	s := newTestStore(t)
	cfg := Config{AlertThreshold: 101, SoftAccountCalls: 5, SoftPrefixCalls: 2}
	insertControl(t, s, "acc-prefix", nil, intPtr(2), true)
	first, _ := reserveDirect(t, s, "p1", "acc-prefix", "9001001", "cli-a", cfg)
	second, _ := reserveDirect(t, s, "p2", "acc-prefix", "9001002", "cli-b", cfg)
	if !first.GetAllowed() || !second.GetAllowed() {
		t.Fatalf("setup calls denied: %+v %+v", first, second)
	}
	third, _ := reserveDirect(t, s, "p3", "acc-prefix", "9001003", "cli-c", cfg)
	if third.GetAllowed() || third.GetReason() != "fraud_velocity_prefix" {
		t.Fatalf("want fraud_velocity_prefix, got %+v", third)
	}
}

func TestAlertInsertedOnThreshold(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.pool.Exec(context.Background(),
		`INSERT INTO risk_prefixes(prefix, weight) VALUES ('900', 100)`); err != nil {
		t.Fatal(err)
	}
	cfg := Config{AlertThreshold: 60, SoftAccountCalls: 5, SoftPrefixCalls: 2}
	resp, alert := reserveDirect(t, s, "a1", "acc-alert", "9001234", "cli", cfg)
	if !resp.GetAllowed() || resp.GetRiskScore() != 100 || resp.GetRiskLevel() != "high" {
		t.Fatalf("bad score response: %+v", resp)
	}
	if alert == nil {
		t.Fatal("alert was not inserted")
	}
	if alert.GetCallId() != "a1" || alert.GetStatus() != "new" || alert.GetRuleVersion() != RuleVersion {
		t.Fatalf("bad alert: %+v", alert)
	}
	if len(alert.GetReasons()) == 0 || alert.GetReasons()[0] != "risk_prefix" {
		t.Fatalf("bad reasons: %v", alert.GetReasons())
	}
}

func TestAlertsCallIDUnique(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insert := func(account string) error {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO alerts(call_id, account_id, risk_score, risk_level, reasons, rule_version)
			 VALUES ('dup', $1, 100, 'high', '[]', 'v1')`, account)
		return err
	}
	if err := insert("acc-a"); err != nil {
		t.Fatal(err)
	}
	if err := insert("acc-b"); err == nil {
		t.Fatal("duplicate alert call_id must fail")
	}
}

func TestEmptyDestinationDoesNotDoubleCountUsage(t *testing.T) {
	s := newTestStore(t)
	cfg := Config{AlertThreshold: 101, SoftAccountCalls: 5, SoftPrefixCalls: 2}
	insertControl(t, s, "acc-empty", intPtr(2), nil, true)

	first, _ := reserveDirect(t, s, "e1", "acc-empty", "", "cli-a", cfg)
	if !first.GetAllowed() {
		t.Fatalf("first empty-destination call denied: %+v", first)
	}
	second, _ := reserveDirect(t, s, "e2", "acc-empty", "", "cli-b", cfg)
	if !second.GetAllowed() {
		t.Fatalf("second empty-destination call denied: %+v", second)
	}
	third, _ := reserveDirect(t, s, "e3", "acc-empty", "", "cli-c", cfg)
	if third.GetAllowed() || third.GetReason() != "fraud_velocity_account" {
		t.Fatalf("want velocity deny after two calls, got %+v", third)
	}
}
