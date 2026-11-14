package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	sbcpb "omnifoni/sbc/gen/sbcpb"
)

// Integration tests run against the dev sbc DB (make up).
func dsn() string {
	if s := os.Getenv("SBC_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/sbc"
}

type sbc struct {
	t    *testing.T
	c    sbcpb.SbcClient
	pool *pgxpool.Pool
}

func startServer(t *testing.T) *sbc {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE active_calls, trunk_limits, barring_rules, auth_failures`); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	sbcpb.RegisterSbcServer(g, NewSBCServer(pool))
	go g.Serve(lis)
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &sbc{t: t, c: sbcpb.NewSbcClient(conn), pool: pool}
}

// setLimit installs a trunk limit directly in the DB (limits admin comes with
// the portal later).
func (s *sbc) setLimit(trunk string, maxCalls int) {
	s.t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`INSERT INTO trunk_limits (trunk_id, max_calls) VALUES ($1, $2)
		 ON CONFLICT (trunk_id) DO UPDATE SET max_calls = $2`, trunk, maxCalls); err != nil {
		s.t.Fatal(err)
	}
}

// addRule inserts a barring rule.
func (s *sbc) addRule(matchType, prefix, action string) {
	s.t.Helper()
	if _, err := s.pool.Exec(context.Background(),
		`INSERT INTO barring_rules (match_type, prefix, action) VALUES ($1, $2, $3)`,
		matchType, prefix, action); err != nil {
		s.t.Fatal(err)
	}
}

func (s *sbc) check(callID, cli, dest, trunk, ip string) *sbcpb.CheckCallResponse {
	s.t.Helper()
	res, err := s.c.CheckCall(context.Background(), &sbcpb.CheckCallRequest{
		CallId: callID, Cli: cli, Destination: dest, TrunkId: trunk, SourceIp: ip})
	if err != nil {
		s.t.Fatal(err)
	}
	return res
}

// TestTrunkLimitEnforced: the third concurrent call on a 2-call trunk is
// denied with trunk_limit_exceeded; ending a call frees the slot.
func TestTrunkLimitEnforced(t *testing.T) {
	s := startServer(t)
	s.setLimit("t1", 2)
	for i := 1; i <= 2; i++ {
		if res := s.check(fmt.Sprintf("call-%d", i), "cli", "3555123", "t1", ""); !res.Allowed {
			t.Fatalf("call %d denied: %+v", i, res)
		}
	}
	res := s.check("call-3", "cli", "3555123", "t1", "")
	if res.Allowed || res.Reason != "trunk_limit_exceeded" {
		t.Fatalf("want trunk_limit_exceeded, got %+v", res)
	}
	// a different trunk is unaffected
	if res := s.check("call-x", "cli", "3555123", "t2", ""); !res.Allowed {
		t.Fatalf("other trunk denied: %+v", res)
	}
	// release one slot; the next start fits again
	if _, err := s.c.EndCall(context.Background(), &sbcpb.EndCallRequest{CallId: "call-1"}); err != nil {
		t.Fatal(err)
	}
	if res := s.check("call-4", "cli", "3555123", "t1", ""); !res.Allowed {
		t.Fatalf("after release denied: %+v", res)
	}
}

// TestConcurrentStartsRespectLimit: 10 parallel starts against a 5-call trunk
// yield exactly 5 allowed (the slot reservation races safely).
func TestConcurrentStartsRespectLimit(t *testing.T) {
	s := startServer(t)
	s.setLimit("tc", 5)

	var mu sync.Mutex
	allowed := 0
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := s.c.CheckCall(context.Background(), &sbcpb.CheckCallRequest{
				CallId: fmt.Sprintf("cc-%d", i), Cli: "cli", Destination: "3555", TrunkId: "tc"})
			if err != nil {
				return
			}
			if res.Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if allowed != 5 {
		t.Fatalf("allowed %d, want exactly 5", allowed)
	}
}

// TestBarringRules: longest-prefix destination and cli deny rules.
func TestBarringRules(t *testing.T) {
	s := startServer(t)
	s.addRule("destination", "900", "deny")
	s.addRule("destination", "9001", "deny")
	s.addRule("cli", "1888", "deny")

	if res := s.check("b1", "cli", "9001234", "t1", ""); res.Allowed || res.Reason != "barring_destination" {
		t.Fatalf("dest 900: %+v", res)
	}
	if res := s.check("b2", "cli", "9001999", "t1", ""); res.Allowed || res.Reason != "barring_destination" {
		t.Fatalf("dest 9001 (longest): %+v", res)
	}
	if res := s.check("b3", "1888555", "3555123", "t1", ""); res.Allowed || res.Reason != "barring_cli" {
		t.Fatalf("cli 1888: %+v", res)
	}
	// trunk limits do not apply to unconfigured trunks
	if res := s.check("b4", "cli", "3555123", "t1", ""); !res.Allowed {
		t.Fatalf("unbarred call denied: %+v", res)
	}
}

// TestIntrusionDetection: five auth failures inside the window make the SBC
// deny that source automatically; other sources unaffected.
func TestIntrusionDetection(t *testing.T) {
	s := startServer(t)
	for i := 0; i < 5; i++ {
		if _, err := s.c.ReportAuthFailure(context.Background(),
			&sbcpb.ReportAuthFailureRequest{SourceIp: "10.0.0.66"}); err != nil {
			t.Fatal(err)
		}
	}
	if res := s.check("atk-1", "cli", "3555123", "t1", "10.0.0.66"); res.Allowed || res.Reason != "ip_blocked" {
		t.Fatalf("attacker: %+v", res)
	}
	if res := s.check("ok-1", "cli", "3555123", "t1", "10.0.0.77"); !res.Allowed {
		t.Fatalf("innocent denied: %+v", res)
	}
	// failures older than the window stop blocking
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE auth_failures SET failed_at = now() - interval '5 minutes'
		 WHERE source_ip = '10.0.0.66'`); err != nil {
		t.Fatal(err)
	}
	if res := s.check("decay-1", "cli", "3555123", "t1", "10.0.0.66"); !res.Allowed {
		t.Fatalf("old failures still blocking: %+v", res)
	}
}
