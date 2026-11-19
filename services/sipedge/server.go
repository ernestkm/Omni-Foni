package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"omnifoni/observability"
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

type callLock struct {
	token chan struct{}
	refs  int
}

type callLocks struct {
	mu    sync.Mutex
	locks map[string]*callLock
}

func newCallLocks() *callLocks { return &callLocks{locks: map[string]*callLock{}} }

func (l *callLocks) hold(id string) func() {
	l.mu.Lock()
	c, ok := l.locks[id]
	if !ok {
		c = &callLock{token: make(chan struct{}, 1)}
		l.locks[id] = c
	}
	c.refs++
	l.mu.Unlock()

	c.token <- struct{}{}
	return func() {
		<-c.token
		l.mu.Lock()
		c.refs--
		if c.refs == 0 && l.locks[id] == c {
			delete(l.locks, id)
		}
		l.mu.Unlock()
	}
}

const (
	decisionTTL  = 64 * time.Second
	maxDecisions = 4096
)

type decision struct {
	status int
	body   string
}

type cachedDecision struct {
	decision  decision
	createdAt time.Time
	expiresAt time.Time
}

type decisionCache struct {
	mu    sync.Mutex
	items map[string]cachedDecision
}

func newDecisionCache() *decisionCache {
	return &decisionCache{items: map[string]cachedDecision{}}
}

func (c *decisionCache) get(id string) (decision, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	item, ok := c.items[id]
	if !ok || time.Now().After(item.expiresAt) {
		delete(c.items, id)
		return decision{}, false
	}
	return item.decision, true
}

func (c *decisionCache) put(id string, d decision) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if _, exists := c.items[id]; !exists && len(c.items) >= maxDecisions {
		c.evictLocked(now)
	}
	c.items[id] = cachedDecision{decision: d, createdAt: now, expiresAt: now.Add(decisionTTL)}
}

func (c *decisionCache) evictLocked(now time.Time) {
	for id, item := range c.items {
		if now.After(item.expiresAt) {
			delete(c.items, id)
		}
	}
	for len(c.items) >= maxDecisions {
		var oldestID string
		var oldestAt time.Time
		found := false
		for id, item := range c.items {
			if !found || item.createdAt.Before(oldestAt) {
				oldestID, oldestAt, found = id, item.createdAt, true
			}
		}
		if !found {
			return
		}
		delete(c.items, oldestID)
	}
}

type sipedgeServer struct {
	accounts   map[string]string
	facade     facade
	store      *store
	decisions  *decisionCache
	cacheOnce  sync.Once
	cancels    *decisionCache
	cancelOnce sync.Once
	locks      *callLocks
	locksOnce  sync.Once
}

func (s *sipedgeServer) cache() *decisionCache {
	s.cacheOnce.Do(func() {
		if s.decisions == nil {
			s.decisions = newDecisionCache()
		}
	})
	return s.decisions
}

func (s *sipedgeServer) cancelCache() *decisionCache {
	s.cancelOnce.Do(func() {
		if s.cancels == nil {
			s.cancels = newDecisionCache()
		}
	})
	return s.cancels
}

func (s *sipedgeServer) keyedLocks() *callLocks {
	s.locksOnce.Do(func() {
		if s.locks == nil {
			s.locks = newCallLocks()
		}
	})
	return s.locks
}

func (s *sipedgeServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /invite", s)
	mux.Handle("POST /bye", s)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	return observability.Middleware(mux)
}

func writeDecision(w http.ResponseWriter, d decision) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(d.status)
	_, _ = io.WriteString(w, d.body)
}

func (s *sipedgeServer) reply(ci string, w http.ResponseWriter, d decision) {
	s.cache().put(ci, d)
	writeDecision(w, d)
}

func (s *sipedgeServer) deny(ci string, w http.ResponseWriter, span trace.Span, reason string) {
	token := safeReason(reason)
	d := decision{status: sipCode(reason), body: token}
	span.SetAttributes(attribute.String("decision", token))
	s.reply(ci, w, d)
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
		trace.WithAttributes(attribute.String("source_ip", srcIP), attribute.String("call_id", ci)))
	defer span.End()

	unlock := s.keyedLocks().hold(ci)
	defer unlock()

	if d, ok := s.cache().get(ci); ok {
		span.SetAttributes(attribute.Bool("decision.replay", true))
		writeDecision(w, d)
		return
	}
	if _, ok := s.cancelCache().get(ci); ok {
		s.deny(ci, w, span, "canceled")
		return
	}
	account, ok := s.accounts[srcIP]
	if !ok {
		s.deny(ci, w, span, "ip_blocked")
		return
	}
	start, err := s.facade.start(ctx, startReq{
		AccountID: account, Destination: userPart(ruri),
		CLI: userPart(from), SourceIP: srcIP,
	})
	if err != nil {
		s.deny(ci, w, span, "ocs_unavailable")
		return
	}
	if !start.Allowed {
		s.deny(ci, w, span, start.Reason)
		return
	}
	if start.TargetURI == "" {
		s.deny(ci, w, span, "no_target_uri")
		return
	}
	if start.MaxSeconds <= 0 {
		s.deny(ci, w, span, "internal_error")
		return
	}
	d := decision{status: http.StatusOK, body: fmt.Sprintf("%d|%s", start.MaxSeconds, start.TargetURI)}
	span.SetAttributes(attribute.String("decision", "allowed"),
		attribute.String("switching_call_id", start.CallID))
	s.store.put(ci, entry{switchingID: start.CallID, hEntry: hEntry, hID: hID})
	s.reply(ci, w, d)
}

func (s *sipedgeServer) bye(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("omnifoni/sipedge").Start(r.Context(), "sipedge.bye")
	defer span.End()
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1024))
	id := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(body)), "BYE "))
	unlock := s.keyedLocks().hold(id)
	defer unlock()
	s.cancelCache().put(id, decision{status: http.StatusForbidden, body: "canceled"})
	if e, ok := s.store.take(id); ok {
		span.SetAttributes(attribute.String("call_id", e.switchingID))
		_, _ = s.facade.stop(ctx, e.switchingID)
	}
	w.WriteHeader(http.StatusOK)
}
