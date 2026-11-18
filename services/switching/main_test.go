package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	cataloguepb "omnifoni/catalogue/gen/cataloguepb"
	chargingpb "omnifoni/charging/gen/chargingpb"
	routingpb "omnifoni/routing/gen/routingpb"
	sbcpb "omnifoni/sbc/gen/sbcpb"
	pb "omnifoni/switching/gen/switchingpb"
)

// Integration tests need the dev data layer: charging on :50051 and its
// Postgres DB (make up).
func chargingAddr() string {
	if s := os.Getenv("SWITCHING_CHARGING_ADDR"); s != "" {
		return s
	}
	return "localhost:50051"
}

func chargingDSN() string {
	if s := os.Getenv("CHARGING_DSN"); s != "" {
		return s
	}
	return "postgres://postgres:dev-only@localhost:5432/charging"
}

var (
	charging chargingpb.ChargingClient
	db       *pgxpool.Pool
)

func TestMain(m *testing.M) {
	var err error
	ccc, err := grpc.NewClient(chargingAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "charging client:", err)
		os.Exit(1)
	}
	charging = chargingpb.NewChargingClient(ccc)
	db, err = pgxpool.New(context.Background(), chargingDSN())
	if err != nil {
		fmt.Fprintln(os.Stderr, "db:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// startSwitching serves an in-process switching server backed by the given
// charging endpoint and returns a client connection to it. pub may be nil.
func startSwitchingWithCDR(t *testing.T, chargingEndpoint string, pub CDRPublisher) *grpc.ClientConn {
	return startSwitchingFull(t, chargingEndpoint, pub, nil)
}

// startSwitchingFull additionally wires a dial-plan resolver when non-nil
// and an SBC checker when non-nil.
func startSwitchingFull(t *testing.T, chargingEndpoint string, pub CDRPublisher, res RouteResolver) *grpc.ClientConn {
	return startSwitchingAll(t, chargingEndpoint, pub, res, nil)
}

func startSwitchingAll(t *testing.T, chargingEndpoint string, pub CDRPublisher, res RouteResolver, sbc SbcChecker) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient(chargingEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	reg := NewRegistryWithCDRs(pub)
	if res != nil {
		reg = reg.WithRouting(res)
	}
	if sbc != nil {
		reg = reg.WithSBC(sbc)
	}
	pb.RegisterSwitchingServer(srv, &switchingServer{reg: reg, cc: cc})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	client, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func startSwitching(t *testing.T, chargingEndpoint string) *grpc.ClientConn {
	return startSwitchingWithCDR(t, chargingEndpoint, nil)
}

// seed creates an account and one rate row; dest must start with prefix.
func seed(t *testing.T, account string, balanceMinor int64, prefix, rate string) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.Exec(ctx, `TRUNCATE ledger, sessions`); err != nil {
		t.Fatal(err)
	}
	// Rates live in Catalogue (P1 slice three); seed via its gRPC like the
	// charging tests do. The stale charging.rates table is a decoy.
	if _, err := catalogue(t).PutRate(ctx, &cataloguepb.PutRateRequest{Prefix: prefix, RatePerSec: rate}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx,
		`INSERT INTO accounts (account_id, kind, balance) VALUES ($1, 'prepaid', $2)
		 ON CONFLICT (account_id) DO UPDATE SET balance = $2, kind = 'prepaid', credit_limit = 0`,
		account, balanceMinor); err != nil {
		t.Fatal(err)
	}
}

func catalogue(t *testing.T) cataloguepb.CatalogueClient {
	t.Helper()
	addr := os.Getenv("CATALOGUE_ADDR")
	if addr == "" {
		addr = "localhost:50053"
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cataloguepb.NewCatalogueClient(cc)
}

func startCall(t *testing.T, client *grpc.ClientConn, account, dest string) *pb.StartCallResponse {
	t.Helper()
	resp, err := pb.NewSwitchingClient(client).StartCall(context.Background(),
		&pb.StartCallRequest{AccountId: account, Destination: dest})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// collect streams events until the done event appears or the deadline hits.
func collect(t *testing.T, client *grpc.ClientConn, callID string, deadline time.Duration) []*pb.CallEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	stream, err := pb.NewSwitchingClient(client).CallEvents(ctx, &pb.CallEventsRequest{CallId: callID})
	if err != nil {
		t.Fatal(err)
	}
	var evs []*pb.CallEvent
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			return evs
		}
		if ctx.Err() != nil {
			return evs
		}
		if err != nil {
			t.Fatal(err)
		}
		evs = append(evs, ev)
		if ev.Type == pb.CallEvent_TERMINATED || ev.Type == pb.CallEvent_RELEASED {
			return evs
		}
	}
}

// TestExhaustionTermination: balance 5 at 1 minor/second; the call must be
// terminated with reason balance_exhausted by the 6s tick, and the charged
// amount must equal the seeded balance exactly.
func TestExhaustionTermination(t *testing.T) {
	seed(t, "acc-exh", 5, "3555", "1.000000")
	client := startSwitching(t, chargingAddr())

	resp := startCall(t, client, "acc-exh", "35551234567")
	if !resp.Allowed {
		t.Fatalf("denied: %+v", resp)
	}

	evs := collect(t, client, resp.CallId, 12*time.Second)
	if len(evs) == 0 || evs[len(evs)-1].Type != pb.CallEvent_TERMINATED {
		t.Fatalf("no termination event: %+v", evs)
	}
	last := evs[len(evs)-1]
	if last.Reason != "balance_exhausted" {
		t.Fatalf("reason %q, want balance_exhausted", last.Reason)
	}
	// Charged seconds = tick count; cost per tick is 1 minor, so the account
	// must be drained to zero, never negative.
	if last.ChargedSeconds < 5 || last.ChargedSeconds > 7 {
		t.Fatalf("charged seconds %d, want 5..7", last.ChargedSeconds)
	}
	var balance int64
	if err := db.QueryRow(context.Background(),
		`SELECT balance FROM accounts WHERE account_id='acc-exh'`).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance != 0 {
		t.Fatalf("balance %d, want 0 (drained, never negative)", balance)
	}
}

// TestFailClosedOnOcsOutage: unreachable OCS denies the call outright.
func TestFailClosedOnOcsOutage(t *testing.T) {
	client := startSwitching(t, "127.0.0.1:1") // nothing listens here
	resp := startCall(t, client, "acc-x", "35551234567")
	if resp.Allowed || resp.Reason != "ocs_unavailable" {
		t.Fatalf("want ocs_unavailable deny, got %+v", resp)
	}
}

// TestStopCallReleases: a stopped call emits RELEASED and the OCS session is
// released (no further charges).
func TestStopCallReleases(t *testing.T) {
	seed(t, "acc-stop", 1000, "3555", "1.000000")
	client := startSwitching(t, chargingAddr())

	resp := startCall(t, client, "acc-stop", "35551234567")
	if !resp.Allowed {
		t.Fatalf("denied: %+v", resp)
	}
	time.Sleep(1500 * time.Millisecond) // let a tick or two run
	stopCall(t, client, resp.CallId)
	waitReleased(t, client, resp.CallId)
	var released bool
	if err := db.QueryRow(context.Background(),
		`SELECT released FROM sessions WHERE session_id = $1`, resp.SessionId).Scan(&released); err != nil {
		t.Fatal(err)
	}
	if !released {
		t.Fatal("session not released in OCS")
	}
}

// TestDeniedCallNotCreated: an OCS deny produces no call and no events stream.
func TestDeniedCallNotCreated(t *testing.T) {
	seed(t, "acc-deny", 0, "3555", "1.000000")
	client := startSwitching(t, chargingAddr())
	resp := startCall(t, client, "acc-deny", "35551234567")
	if resp.Allowed || resp.Reason != "insufficient_balance" {
		t.Fatalf("want insufficient_balance, got %+v", resp)
	}
	if resp.CallId != "" {
		t.Fatalf("no call should be created, got %q", resp.CallId)
	}
}

// routingClientFor connects to the dev routing service for rule seeding and
// as a real resolver implementation.
func routingClientFor(t *testing.T) routingpb.RoutingClient {
	t.Helper()
	addr := os.Getenv("SWITCHING_ROUTING_ADDR")
	if addr == "" {
		addr = "localhost:50057"
	}
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return routingpb.NewRoutingClient(cc)
}

// seedLcr installs an LCR candidate (and enables its trunk) via routing gRPC.
func seedLcr(t *testing.T, rc routingpb.RoutingClient, prefix, trunk string, cost int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := rc.PutTrunk(ctx, &routingpb.PutTrunkRequest{TrunkId: trunk, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := rc.PutLcrRoute(ctx, &routingpb.PutLcrRouteRequest{Prefix: prefix, TrunkId: trunk, CostPerSec: cost}); err != nil {
		t.Fatal(err)
	}
}

// TestRoutedCallAllowed: a cheapest-valid-trunk candidate lets the call through.
func TestRoutedCallAllowed(t *testing.T) {
	seed(t, "acc-route", 1000, "3555", "1.000000")
	rc := routingClientFor(t)
	seedLcr(t, rc, "3555", "trunk-a", 2)
	client := startSwitchingFull(t, chargingAddr(), nil, routingClient{rc})
	resp := startCall(t, client, "acc-route", "35551234567")
	if !resp.Allowed {
		t.Fatalf("denied: %+v", resp)
	}
}

// TestUnroutedDenied: no dial-plan rule denies with no_route, no call created.
func TestUnroutedDenied(t *testing.T) {
	seed(t, "acc-noroute", 1000, "3555", "1.000000")
	rc := routingClientFor(t)
	client := startSwitchingFull(t, chargingAddr(), nil, routingClient{rc})
	resp := startCall(t, client, "acc-noroute", "9988776655")
	if resp.Allowed || resp.Reason != "no_route" {
		t.Fatalf("want no_route deny, got %+v", resp)
	}
	if resp.CallId != "" {
		t.Fatalf("no call should be created, got %q", resp.CallId)
	}
}

// TestRoutingOutageFailClosed: unreachable Routing denies the call outright.
func TestRoutingOutageFailClosed(t *testing.T) {
	seed(t, "acc-routeout", 1000, "3555", "1.000000")
	dead, err := grpc.NewClient("passthrough:///127.0.0.1:1",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dead.Close() })
	client := startSwitchingFull(t, chargingAddr(), nil, routingClient{routingpb.NewRoutingClient(dead)})
	resp := startCall(t, client, "acc-routeout", "35551234567")
	if resp.Allowed || resp.Reason != "routing_unavailable" {
		t.Fatalf("want routing_unavailable deny, got %+v", resp)
	}
}

// fakeSbc is an in-process SbcChecker with a concurrent-call limit.
type fakeSbc struct {
	limit         int
	err           error
	mu            sync.Mutex
	active        int
	lastAccountID string
}

func (f *fakeSbc) CheckCall(ctx context.Context, req *sbcpb.CheckCallRequest) (*sbcpb.CheckCallResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastAccountID = req.GetAccountId()
	if f.err != nil {
		return nil, f.err
	}
	if f.active >= f.limit {
		return &sbcpb.CheckCallResponse{Allowed: false, Reason: "trunk_limit_exceeded"}, nil
	}
	f.active++
	return &sbcpb.CheckCallResponse{Allowed: true}, nil
}

func (f *fakeSbc) EndCall(ctx context.Context, req *sbcpb.EndCallRequest) (*sbcpb.EndCallResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.active--
	return &sbcpb.EndCallResponse{}, nil
}

// TestSbcLimitEnforced: a second concurrent call on a 1-call trunk is denied
// through the SBC playbook; ending the first call frees the slot.
func TestSbcLimitEnforced(t *testing.T) {
	seed(t, "acc-sbclimit", 1000, "3555", "1.000000")
	rc := routingClientFor(t)
	seedLcr(t, rc, "3555", "trunk-sbcl", 2)
	fsb := &fakeSbc{limit: 1}
	client := startSwitchingAll(t, chargingAddr(), nil,
		routingClient{rc}, fsb)

	first := startCall(t, client, "acc-sbclimit", "35551234567")
	if !first.Allowed {
		t.Fatalf("first call denied: %+v", first)
	}
	second := startCall(t, client, "acc-sbclimit", "35551234567")
	if second.Allowed || second.Reason != "trunk_limit_exceeded" {
		t.Fatalf("want trunk_limit_exceeded deny, got %+v", second)
	}
	stopCall(t, client, first.CallId)
	waitReleased(t, client, first.CallId)
	third := startCall(t, client, "acc-sbclimit", "35551234567")
	if !third.Allowed {
		t.Fatalf("after release denied: %+v", third)
	}
}

// TestSbcOutageFailClosed: SBC errors deny with sbc_unavailable.
func TestSbcOutageFailClosed(t *testing.T) {
	seed(t, "acc-sbcout", 1000, "3555", "1.000000")
	rc := routingClientFor(t)
	seedLcr(t, rc, "3555", "trunk-sbco", 2)
	client := startSwitchingAll(t, chargingAddr(), nil,
		routingClient{rc}, &fakeSbc{limit: 1, err: errors.New("sbc down")})
	resp := startCall(t, client, "acc-sbcout", "35551234567")
	if resp.Allowed || resp.Reason != "sbc_unavailable" {
		t.Fatalf("want sbc_unavailable deny, got %+v", resp)
	}
}

// TestSbcReceivesAccountID: switching forwards the StartCall account id to the
// SBC CheckCall request so fraud checks can be scoped to the caller.
func TestSbcReceivesAccountID(t *testing.T) {
	seed(t, "acc-sbc-account", 1000, "3555", "1.000000")
	rc := routingClientFor(t)
	seedLcr(t, rc, "3555", "trunk-sbc-account", 2)
	fsb := &fakeSbc{limit: 1}
	client := startSwitchingAll(t, chargingAddr(), nil, routingClient{rc}, fsb)
	resp := startCall(t, client, "acc-sbc-account", "35551234567")
	if !resp.Allowed {
		t.Fatalf("call denied: %+v", resp)
	}
	stopCall(t, client, resp.CallId)
	fsb.mu.Lock()
	defer fsb.mu.Unlock()
	if fsb.lastAccountID != "acc-sbc-account" {
		t.Fatalf("SBC account id = %q", fsb.lastAccountID)
	}
}

// stopCall stops a call, expecting success.
func stopCall(t *testing.T, client *grpc.ClientConn, callID string) {
	t.Helper()
	st, err := pb.NewSwitchingClient(client).StopCall(context.Background(),
		&pb.StopCallRequest{CallId: callID})
	if err != nil {
		t.Fatal(err)
	}
	if !st.Stopped {
		t.Fatal("StopCall returned false")
	}
}

// waitReleased blocks until the call streams its terminal event.
func waitReleased(t *testing.T, client *grpc.ClientConn, callID string) {
	t.Helper()
	evs := collect(t, client, callID, 10*time.Second)
	if len(evs) == 0 || (evs[len(evs)-1].Type != pb.CallEvent_RELEASED &&
		evs[len(evs)-1].Type != pb.CallEvent_TERMINATED) {
		t.Fatalf("no terminal event: %+v", evs)
	}
}
