package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	chargingpb "omnifoni/charging/gen/chargingpb"
	pb "omnifoni/switching/gen/switchingpb"
)

// Registry tracks live calls and their event streams.
type Registry struct {
	mu    sync.Mutex
	calls map[string]*call
}

type call struct {
	id, sessionID string
	stop          chan struct{}
	stopOnce      sync.Once

	mu     sync.Mutex
	cond   *sync.Cond
	events []*pb.CallEvent
	done   bool
}

func NewRegistry() *Registry {
	return &Registry{calls: map[string]*call{}}
}

// StartCall authorizes with the OCS, then decrements live. Denied calls and
// OCS errors are fail-closed: no call is created.
func (r *Registry) StartCall(ctx context.Context, cc *grpc.ClientConn, req *pb.StartCallRequest) (*pb.StartCallResponse, error) {
	cauth, err := chargingpb.NewChargingClient(cc).Authorize(ctx, &chargingpb.AuthorizeRequest{
		AccountId: req.GetAccountId(), Destination: req.GetDestination(), Cli: req.GetCli(),
	})
	if err != nil {
		slog.Error("ocs unreachable, fail-closed", "account", req.GetAccountId(), "err", err)
		return &pb.StartCallResponse{Allowed: false, Reason: "ocs_unavailable"}, nil
	}
	if !cauth.GetAllow() {
		return &pb.StartCallResponse{Allowed: false, Reason: cauth.GetReason()}, nil
	}

	c := &call{
		id: uuid.New().String(), sessionID: cauth.GetSessionId(),
		stop: make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)
	r.mu.Lock()
	r.calls[c.id] = c
	r.mu.Unlock()

	c.emit(&pb.CallEvent{CallId: c.id, Type: pb.CallEvent_STARTED})
	go r.runCall(cc, c)

	return &pb.StartCallResponse{Allowed: true, CallId: c.id,
		SessionId: cauth.GetSessionId(), MaxSeconds: cauth.GetMaxSeconds()}, nil
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
			return
		case <-ticker.C:
			total++
			resp, err := charger.Charge(context.Background(), &chargingpb.ChargeRequest{
				SessionId: c.sessionID, TotalSeconds: total})
			switch {
			case err != nil:
				slog.Error("ocs charge failed, terminating", "call", c.id, "err", err)
				c.emit(&pb.CallEvent{CallId: c.id, Type: pb.CallEvent_TERMINATED,
					Reason: "ocs_unavailable", ChargedSeconds: total})
				return
			case resp.GetDenied():
				c.emit(&pb.CallEvent{CallId: c.id, Type: pb.CallEvent_TERMINATED,
					Reason: "balance_exhausted", ChargedSeconds: total})
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
