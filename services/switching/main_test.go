package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	chargingpb "omnifoni/charging/gen/chargingpb"
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
	t.Helper()
	cc, err := grpc.NewClient(chargingEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	pb.RegisterSwitchingServer(srv, &switchingServer{reg: NewRegistryWithCDRs(pub), cc: cc})
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
	if _, err := db.Exec(ctx,
		`INSERT INTO rates (prefix, rate_per_sec) VALUES ($1, $2::numeric)
		 ON CONFLICT (prefix) DO UPDATE SET rate_per_sec = $2::numeric`, prefix, rate); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx,
		`INSERT INTO accounts (account_id, kind, balance) VALUES ($1, 'prepaid', $2)
		 ON CONFLICT (account_id) DO UPDATE SET balance = $2, kind = 'prepaid', credit_limit = 0`,
		account, balanceMinor); err != nil {
		t.Fatal(err)
	}
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
	st, err := pb.NewSwitchingClient(client).StopCall(context.Background(),
		&pb.StopCallRequest{CallId: resp.CallId})
	if err != nil {
		t.Fatal(err)
	}
	if !st.Stopped {
		t.Fatal("StopCall returned false")
	}
	evs := collect(t, client, resp.CallId, 10*time.Second)
	if len(evs) == 0 || evs[len(evs)-1].Type != pb.CallEvent_RELEASED {
		t.Fatalf("no release event: %+v", evs)
	}
	var released bool
	err = db.QueryRow(context.Background(),
		`SELECT released FROM sessions WHERE session_id = $1`, resp.SessionId).Scan(&released)
	if err != nil {
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
