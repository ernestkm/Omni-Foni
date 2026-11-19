package main

import (
	"io"
	"net/http"
	"strings"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type entry struct {
	switchingID string
	hEntry      string
	hID         string
}

// store maps a Kamailio Call-ID to the live switching call it started.
type store struct {
	mu sync.Mutex
	m  map[string]entry
}

func newStore() *store { return &store{m: map[string]entry{}} }

func (s *store) get(id string) (entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[id]
	return e, ok
}

func (s *store) put(id string, e entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[id] = e
}

func (s *store) take(id string) (entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[id]
	delete(s.m, id)
	return e, ok
}

func (s *store) all() map[string]entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]entry, len(s.m))
	for k, v := range s.m {
		out[k] = v
	}
	return out
}

type sipedgeServer struct {
	accounts map[string]string
	facade   facade
	store    *store
}

func (s *sipedgeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/invite":
		s.invite(w, r)
	case "/bye":
		s.bye(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *sipedgeServer) invite(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
	parts := strings.Split(strings.TrimSpace(string(body)), "|")
	if len(parts) != 7 {
		http.Error(w, "bad invite body", http.StatusBadRequest)
		return
	}
	ci, srcIP, from, _, ruri, hEntry, hID := parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6]
	ctx, span := otel.Tracer("omnifoni/sipedge").Start(r.Context(), "sipedge.invite",
		trace.WithAttributes(attribute.String("source_ip", srcIP)))
	defer span.End()

	account, ok := s.accounts[srcIP]
	if !ok {
		span.SetAttributes(attribute.String("decision", "ip_blocked"))
		http.Error(w, "ip_blocked", http.StatusForbidden)
		return
	}
	start, err := s.facade.start(ctx, startReq{
		AccountID: account, Destination: userPart(ruri),
		CLI: userPart(from), SourceIP: srcIP,
	})
	if err != nil {
		span.SetAttributes(attribute.String("decision", "policy_unavailable"))
		http.Error(w, "policy_unavailable", http.StatusServiceUnavailable)
		return
	}
	if !start.Allowed {
		span.SetAttributes(attribute.String("decision", start.Reason))
		http.Error(w, start.Reason, sipCode(start.Reason))
		return
	}
	if start.TargetURI == "" {
		span.SetAttributes(attribute.String("decision", "no_target_uri"))
		http.Error(w, "no_target_uri", http.StatusServiceUnavailable)
		return
	}
	span.SetAttributes(attribute.String("decision", "allowed"),
		attribute.String("call_id", start.CallID))
	s.store.put(ci, entry{switchingID: start.CallID, hEntry: hEntry, hID: hID})
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(start.TargetURI))
}

func (s *sipedgeServer) bye(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("omnifoni/sipedge").Start(r.Context(), "sipedge.bye")
	defer span.End()
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1024))
	id := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(body)), "BYE "))
	if e, ok := s.store.take(id); ok {
		span.SetAttributes(attribute.String("call_id", e.switchingID))
		_, _ = s.facade.stop(ctx, e.switchingID)
	}
	w.WriteHeader(http.StatusOK)
}
