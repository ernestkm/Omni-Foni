package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	routingpb "omnifoni/routing/gen/routingpb"
)

// Integration tests run against the dev routing DB (make up).
func dsn() string {
	if s := os.Getenv("ROUTING_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/routing"
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func startServer(t *testing.T) routingpb.RoutingClient {
	t.Helper()
	// Real server on an ephemeral port, real Postgres behind it.
	pool, err := pgxpool.New(context.Background(), dsn())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `TRUNCATE rules, lcr_routes, trunks`); err != nil {
		t.Fatal(err)
	}
	lis, err := newListener()
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	routingpb.RegisterRoutingServer(g, NewRoutingServer(pool))
	go g.Serve(lis)
	t.Cleanup(g.Stop)

	conn, err := grpc.NewClient("passthrough:///"+lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return routingpb.NewRoutingClient(conn)
}

func TestLongestPrefixWins(t *testing.T) {
	c := startServer(t)
	ctx := context.Background()
	for _, r := range []*routingpb.PutRuleRequest{
		{Prefix: "011", TrunkId: "trunk-a"},
		{Prefix: "0114", TrunkId: "trunk-b"},
	} {
		if _, err := c.PutRule(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		number, want string
	}{
		{"011441234", "trunk-b"}, // longest prefix
		{"01199999", "trunk-a"},  // shorter prefix fallback
		{"0044123", ""},          // no rule
	} {
		res, err := c.Resolve(ctx, &routingpb.ResolveRequest{Number: tc.number})
		if err != nil {
			t.Fatalf("%s: %v", tc.number, err)
		}
		if res.TrunkId != tc.want {
			t.Fatalf("%s: trunk %q, want %q", tc.number, res.TrunkId, tc.want)
		}
	}
}

// TestPutRuleUpserts: re-putting a prefix retargets its trunk.
func TestPutRuleUpserts(t *testing.T) {
	c := startServer(t)
	ctx := context.Background()
	if _, err := c.PutRule(ctx, &routingpb.PutRuleRequest{Prefix: "49", TrunkId: "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutRule(ctx, &routingpb.PutRuleRequest{Prefix: "49", TrunkId: "b"}); err != nil {
		t.Fatal(err)
	}
	res, err := c.Resolve(ctx, &routingpb.ResolveRequest{Number: "49170"})
	if err != nil {
		t.Fatal(err)
	}
	if res.TrunkId != "b" {
		t.Fatalf("trunk %q, want b (upsert failed)", res.TrunkId)
	}
}

// TestPutRuleValidation: empty fields are rejected with InvalidArgument.
func TestPutRuleValidation(t *testing.T) {
	c := startServer(t)
	_, err := c.PutRule(context.Background(), &routingpb.PutRuleRequest{Prefix: "", TrunkId: "a"})
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("code %v, want InvalidArgument", code)
	}
}

func newListener() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }

// TestLcrCheapestValidTrunk: among candidate trunks for a prefix, LCR picks
// the cheapest enabled one; disabled trunks are skipped; ties break on
// longest prefix then trunk id.
func TestLcrCheapestValidTrunk(t *testing.T) {
	c := startServer(t)
	ctx := context.Background()
	for _, tr := range []*routingpb.PutTrunkRequest{
		{TrunkId: "cheap", Enabled: true},
		{TrunkId: "cheapest", Enabled: false}, // disabled, must be skipped
		{TrunkId: "dear", Enabled: true},
	} {
		if _, err := c.PutTrunk(ctx, tr); err != nil {
			t.Fatal(err)
		}
	}
	for _, r := range []*routingpb.PutLcrRouteRequest{
		{Prefix: "3555", TrunkId: "cheapest", CostPerSec: 1},
		{Prefix: "3555", TrunkId: "cheap", CostPerSec: 5},
		{Prefix: "3555", TrunkId: "dear", CostPerSec: 9},
		{Prefix: "35551", TrunkId: "dear", CostPerSec: 5}, // longer prefix, cheaper than cheap's 5 → tie-break by cost then length
	} {
		if _, err := c.PutLcrRoute(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		number, trunk string
		cost          int64
	}{
		{"3555999", "cheap", 5}, // cheapest disabled → next cheapest
		{"3555199", "dear", 5},  // longer prefix cost 5 vs short prefix cost 5 → cost ties, longer prefix wins
	} {
		res, err := c.ResolveLcr(ctx, &routingpb.ResolveRequest{Number: tc.number})
		if err != nil {
			t.Fatalf("%s: %v", tc.number, err)
		}
		if res.TrunkId != tc.trunk || res.CostPerSec != tc.cost {
			t.Fatalf("%s: got %s/%d, want %s/%d", tc.number, res.TrunkId, res.CostPerSec, tc.trunk, tc.cost)
		}
	}
}

// TestLcrThousandRoutes: the P4 acceptance shape — a 1000-route synthetic
// dataset resolves correctly through one query per call.
func TestLcrThousandRoutes(t *testing.T) {
	c := startServer(t)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		prefix := fmt.Sprintf("0%04d", i)          // 0000..0999
		trunk := fmt.Sprintf("trunk-%03d", i%50)   // 50 trunks
		cost := int64(100 - i%97)                  // deterministic varied costs
		for _, r := range []*routingpb.PutTrunkRequest{{TrunkId: trunk, Enabled: true}} {
			if _, err := c.PutTrunk(ctx, r); err != nil && i < 50 {
				t.Fatal(err)
			}
		}
		if _, err := c.PutLcrRoute(ctx, &routingpb.PutLcrRouteRequest{
			Prefix: prefix, TrunkId: trunk, CostPerSec: cost}); err != nil {
			t.Fatal(err)
		}
	}
	// resolve 00123: only candidate prefix 00123, cost 100-123%97=74
	res, err := c.ResolveLcr(ctx, &routingpb.ResolveRequest{Number: "00123555"})
	if err != nil {
		t.Fatal(err)
	}
	if res.TrunkId != "trunk-023" || res.CostPerSec != 74 {
		t.Fatalf("got %s/%d, want trunk-023/74", res.TrunkId, res.CostPerSec)
	}
	// unknown prefix family: empty response, no error
	res, err = c.ResolveLcr(ctx, &routingpb.ResolveRequest{Number: "9999"})
	if err != nil {
		t.Fatal(err)
	}
	if res.TrunkId != "" {
		t.Fatalf("unexpected trunk %q", res.TrunkId)
	}
}
