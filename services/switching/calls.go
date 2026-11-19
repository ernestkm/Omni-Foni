package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	chargingpb "omnifoni/charging/gen/chargingpb"
	mediationpb "omnifoni/mediation/gen/mediationpb"
	"omnifoni/observability"
	routingpb "omnifoni/routing/gen/routingpb"
	sbcpb "omnifoni/sbc/gen/sbcpb"
	pb "omnifoni/switching/gen/switchingpb"
)

// routingClient adapts the Routing gRPC client to RouteResolver. The call
// path uses LCR: cheapest enabled trunk wins; plain dial-plan Resolve stays
// available to other callers.
type routingClient struct {
	rc routingpb.RoutingClient
}

func (r routingClient) Resolve(ctx context.Context, number string) (string, string, error) {
	res, err := r.rc.ResolveLcr(ctx, &routingpb.ResolveRequest{Number: number})
	if err != nil {
		return "", "", err
	}
	return res.GetTrunkId(), res.GetTargetUri(), nil
}

// Registry tracks live calls and their event streams.
type Registry struct {
	mu     sync.Mutex
	calls  map[string]*call
	cdrPub CDRPublisher // nil in tests without Kafka
	// routing is optional: nil skips dial-plan resolution (dev/tests);
	// production always wires one and unrouted numbers deny.
	routing RouteResolver
	// sbc is optional: nil skips the barring/limit/blocking playbook
	// (dev/tests); production wiring sets SWITCHING_SBC_ADDR and every call
	// passes through it fail-closed.
	sbc SbcChecker
}

// RouteResolver maps a dialed number to a trunk and its SIP target. An empty
// trunk means no rule matched; an empty target means the trunk cannot be
// relayed to.
type RouteResolver interface {
	Resolve(ctx context.Context, number string) (trunkID, targetURI string, err error)
}

// SbcChecker is the SBC playbook surface the call path needs.
type SbcChecker interface {
	CheckCall(ctx context.Context, req *sbcpb.CheckCallRequest) (*sbcpb.CheckCallResponse, error)
	EndCall(ctx context.Context, req *sbcpb.EndCallRequest) (*sbcpb.EndCallResponse, error)
}

// sbcClient adapts the SBC gRPC client.
type sbcClient struct {
	sc sbcpb.SbcClient
}

func (s sbcClient) CheckCall(ctx context.Context, req *sbcpb.CheckCallRequest) (*sbcpb.CheckCallResponse, error) {
	return s.sc.CheckCall(ctx, req)
}

func (s sbcClient) EndCall(ctx context.Context, req *sbcpb.EndCallRequest) (*sbcpb.EndCallResponse, error) {
	return s.sc.EndCall(ctx, req)
}

// CDRPublisher emits the final call record for a finished call.
type CDRPublisher interface {
	PublishCDR(ctx context.Context, c *mediationpb.CallCdr) error
}

type call struct {
	id, sessionID string
	stop          chan struct{}
	stopOnce      sync.Once

	accountID, destination, cli string
	trunkID                     string
	targetURI                   string
	start                       time.Time
	maxSeconds                  int64 // OCS authorize cap, immutable
	chargedSeconds              int64 // updated under mu on every tick
	traceCtx                    context.Context

	mu     sync.Mutex
	cond   *sync.Cond
	events []*pb.CallEvent
	done   bool
}

func NewRegistry() *Registry {
	return &Registry{calls: map[string]*call{}}
}

func NewRegistryWithCDRs(pub CDRPublisher) *Registry {
	r := NewRegistry()
	r.cdrPub = pub
	return r
}

// WithRouting installs dial-plan resolution; production wiring.
func (r *Registry) WithRouting(res RouteResolver) *Registry {
	r.routing = res
	return r
}

// WithSBC installs the SBC playbook (barring, trunk limits, source
// blocking); production wiring.
func (r *Registry) WithSBC(s SbcChecker) *Registry {
	r.sbc = s
	return r
}

func (r *Registry) callTrunkID(callID string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.calls[callID]
	if c == nil {
		return ""
	}
	return c.trunkID
}

// StartCall resolves the dial plan, runs the SBC playbook, then authorizes
// with the OCS. Unrouted numbers, barred calls, trunk-limit rejections,
// routing/SBC/OCS outages, and denials are all fail-closed: no call is
// created.
func (r *Registry) StartCall(ctx context.Context, cc *grpc.ClientConn, req *pb.StartCallRequest) (*pb.StartCallResponse, error) {
	ctx, span := otel.Tracer("omnifoni/switching").Start(ctx, "switching.start_call",
		trace.WithAttributes(
			attribute.String("account", req.GetAccountId()),
			attribute.String("destination", observability.Mask(req.GetDestination()))))
	defer span.End()
	callID := uuid.New().String()
	var trunk, target string
	if r.routing != nil {
		var err error
		trunk, target, err = r.routing.Resolve(ctx, req.GetDestination())
		switch {
		case err != nil:
			slog.ErrorContext(ctx, "routing unreachable, fail-closed", "call", req.GetDestination(), "err", err)
			return &pb.StartCallResponse{Allowed: false, Reason: "routing_unavailable"}, nil
		case trunk == "":
			return &pb.StartCallResponse{Allowed: false, Reason: "no_route"}, nil
		}
	}
	// Release the trunk slot reserved by CheckCall on every path where the
	// call does not start; once handed over, the live call owns it.
	slotOwned := false
	releaseSlot := func() {
		if r.sbc != nil && !slotOwned {
			_, _ = r.sbc.EndCall(context.Background(), &sbcpb.EndCallRequest{CallId: callID})
		}
	}
	defer releaseSlot()
	if r.sbc != nil && trunk != "" {
		sres, err := r.sbc.CheckCall(ctx, &sbcpb.CheckCallRequest{
			CallId: callID, Cli: req.GetCli(), Destination: req.GetDestination(),
			TrunkId: trunk, SourceIp: req.GetSourceIp(),
			AccountId: req.GetAccountId(),
		})
		switch {
		case err != nil:
			slog.ErrorContext(ctx, "sbc unreachable, fail-closed", "call", callID, "err", err)
			return &pb.StartCallResponse{Allowed: false, Reason: "sbc_unavailable"}, nil
		case !sres.GetAllowed():
			return &pb.StartCallResponse{Allowed: false, Reason: sres.GetReason()}, nil
		}
	}
	cauth, err := chargingpb.NewChargingClient(cc).Authorize(ctx, &chargingpb.AuthorizeRequest{
		AccountId: req.GetAccountId(), Destination: req.GetDestination(), Cli: req.GetCli(),
	})
	if err != nil {
		slog.ErrorContext(ctx, "ocs unreachable, fail-closed", "account", req.GetAccountId(), "err", err)
		return &pb.StartCallResponse{Allowed: false, Reason: "ocs_unavailable"}, nil
	}
	if !cauth.GetAllow() {
		return &pb.StartCallResponse{Allowed: false, Reason: cauth.GetReason()}, nil
	}

	c := &call{
		id: callID, sessionID: cauth.GetSessionId(),
		stop:        make(chan struct{}),
		accountID:   req.GetAccountId(),
		destination: req.GetDestination(),
		cli:         req.GetCli(),
		trunkID:     trunk,
		targetURI:   target,
		start:       time.Now(),
		maxSeconds:  cauth.GetMaxSeconds(),
		traceCtx:    trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx)),
	}
	c.cond = sync.NewCond(&c.mu)
	r.mu.Lock()
	r.calls[c.id] = c
	r.mu.Unlock()
	// the live call owns the slot now; it releases at the end
	slotOwned = true
	go func() {
		c.doneWait()
		releaseSlot()
	}()

	span.SetAttributes(attribute.String("call_id", callID), attribute.String("trunk_id", trunk))
	c.emit(&pb.CallEvent{CallId: c.id, Type: pb.CallEvent_STARTED})
	go r.runCall(cc, c)

	return &pb.StartCallResponse{Allowed: true, CallId: c.id,
		SessionId: cauth.GetSessionId(), MaxSeconds: cauth.GetMaxSeconds(),
		TargetUri: target}, nil
}

// runCall charges every whole second until StopCall, denial, or OCS failure.
// Termination follows balance exhaustion by at most one tick (P1 gate).
func (r *Registry) runCall(cc *grpc.ClientConn, c *call) {
	defer func() {
		c.mu.Lock()
		c.done = true
		c.cond.Broadcast()
		c.mu.Unlock()
		// ponytail: ended calls stay in the registry so late subscribers can
		// replay events; add eviction when memory matters
	}()

	charger := chargingpb.NewChargingClient(cc)
	total := int64(0)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			_, _ = charger.Release(context.Background(),
				&chargingpb.ReleaseRequest{SessionId: c.sessionID})
			c.emit(&pb.CallEvent{CallId: c.id, Type: pb.CallEvent_RELEASED,
				ChargedSeconds: total})
			r.publishCDR(c, "normal", total)
			return
		case <-ticker.C:
			total++
			c.mu.Lock()
			c.chargedSeconds = total
			c.mu.Unlock()
			resp, err := charger.Charge(context.Background(), &chargingpb.ChargeRequest{
				SessionId: c.sessionID, TotalSeconds: total})
			switch {
			case err != nil:
				slog.ErrorContext(c.logCtx(), "ocs charge failed, terminating", "call", c.id, "err", err)
				c.emit(&pb.CallEvent{CallId: c.id, Type: pb.CallEvent_TERMINATED,
					Reason: "ocs_unavailable", ChargedSeconds: total})
				r.publishCDR(c, "ocs_unavailable", total)
				return
			case resp.GetDenied():
				c.emit(&pb.CallEvent{CallId: c.id, Type: pb.CallEvent_TERMINATED,
					Reason: "balance_exhausted", ChargedSeconds: total})
				r.publishCDR(c, "balance_exhausted", total)
				return
			}
		}
	}
}

// StopCall ends the call and releases the reservation.
func (r *Registry) StopCall(id string) bool {
	r.mu.Lock()
	c := r.calls[id]
	r.mu.Unlock()
	if c == nil {
		return false
	}
	c.stopOnce.Do(func() { close(c.stop) })
	return true
}

func (c *call) emit(ev *pb.CallEvent) {
	c.mu.Lock()
	c.events = append(c.events, ev)
	c.cond.Broadcast()
	c.mu.Unlock()
}

// doneWait blocks until the call has finished.
func (c *call) doneWait() {
	c.mu.Lock()
	for !c.done {
		c.cond.Wait()
	}
	c.mu.Unlock()
}

// Stream sends buffered events, then follows live until the call ends.
func (c *call) Stream(send func(*pb.CallEvent) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	i := 0
	for {
		for i < len(c.events) {
			ev := c.events[i]
			i++
			c.mu.Unlock()
			err := send(ev)
			c.mu.Lock()
			if err != nil {
				return err
			}
		}
		if c.done {
			return nil
		}
		c.cond.Wait()
	}
}

func (c *call) logCtx() context.Context {
	if c.traceCtx != nil {
		return c.traceCtx
	}
	return context.Background()
}

func (r *Registry) publishCDR(c *call, endReason string, total int64) {
	if r.cdrPub == nil {
		return // ponytail: nil in tests without Kafka; production always wires one
	}
	ctx := c.traceCtx
	if ctx == nil {
		ctx = context.Background()
	}
	err := r.cdrPub.PublishCDR(ctx, buildCDR(c, endReason, total, time.Now()))
	if err != nil {
		slog.ErrorContext(ctx, "cdr publish failed", "call", c.id, "err", err)
	}
}
