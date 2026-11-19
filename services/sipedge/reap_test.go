package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type fakeKam struct {
	ended [][2]string
	err   error
}

func (f *fakeKam) endDialog(_ context.Context, hEntry, hID string) error {
	f.ended = append(f.ended, [2]string{hEntry, hID})
	return f.err
}

func stopCountingFacade(t *testing.T, failFirst int) (*facadeClient, *atomic.Int64) {
	t.Helper()
	stops := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/live":
			_, _ = io.WriteString(w, `{"calls":[]}`)
		case "/stop":
			n := stops.Add(1)
			if int(n) <= failFirst {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, "facade down")
				return
			}
			fmt.Fprintf(w, `{"stopped":%t}`, int(n) > failFirst)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return &facadeClient{base: srv.URL, http: srv.Client()}, stops
}

func TestClaimPendingStopIsSingleAndIdentityBound(t *testing.T) {
	st := newStore()
	st.put("ci", entry{switchingID: "old", stopFailed: true})
	if !st.claimPendingStop("ci", "old") {
		t.Fatal("first claim failed")
	}
	if st.claimPendingStop("ci", "old") {
		t.Fatal("duplicate claim succeeded")
	}
	st.put("ci", entry{switchingID: "new", stopFailed: true})
	if st.claimPendingStop("ci", "old") {
		t.Fatal("stale claim succeeded")
	}
	if !st.claimPendingStop("ci", "new") {
		t.Fatal("replacement claim failed")
	}
}

func TestRetryStopsDoesNotRemoveReplacementEntry(t *testing.T) {
	st := newStore()
	st.put("ci", entry{switchingID: "old", stopFailed: true})
	r := &reaper{store: st, stop: func(context.Context, string) (bool, error) {
		st.put("ci", entry{switchingID: "new"})
		return true, nil
	}}
	r.retryStops(context.Background())
	e, ok := st.get("ci")
	if !ok {
		t.Fatal("replacement entry was removed")
	}
	if e.switchingID != "new" || e.stopFailed {
		t.Fatalf("unexpected replacement entry: %+v", e)
	}
}

func TestReaperRetriesPendingStopUntilSuccess(t *testing.T) {
	facade, stops := stopCountingFacade(t, 1)
	st := newStore()
	st.put("ci-pending", entry{switchingID: "sw-pending", hEntry: "5", hID: "6", stopFailed: true})
	kam := &fakeKam{}
	r := newReaper(facade, nil, st)
	r.kam = kam

	_ = r.tick(context.Background())
	if got := stops.Load(); got != 1 {
		t.Fatalf("want one stop retry, got %d", got)
	}
	if _, ok := st.get("ci-pending"); !ok {
		t.Fatal("pending entry dropped after failed retry")
	}

	_ = r.tick(context.Background())
	if got := stops.Load(); got != 2 {
		t.Fatalf("want two stop retries, got %d", got)
	}
	if _, ok := st.get("ci-pending"); ok {
		t.Fatal("pending entry not removed after successful retry")
	}
	if len(kam.ended) != 0 {
		t.Fatalf("pending-stop dialog must not be ended: %+v", kam.ended)
	}
}

func TestReaperRetriesPendingStopWhenLiveFails(t *testing.T) {
	facade, stops := stopCountingFacade(t, 0)
	st := newStore()
	st.put("ci-pending", entry{switchingID: "sw-pending", stopFailed: true})
	r := newReaper(facade, nil, st)
	r.live = func(context.Context) (map[string]bool, error) { return nil, errors.New("facade down") }
	r.kam = &fakeKam{}

	_ = r.tick(context.Background())
	if got := stops.Load(); got != 1 {
		t.Fatalf("want pending stop retried despite live failure, got %d", got)
	}
	if _, ok := st.get("ci-pending"); ok {
		t.Fatal("pending entry not removed after successful retry")
	}
}

func TestReaperDoesNotEndCallStoredDuringLiveFetch(t *testing.T) {
	st := newStore()
	kam := &fakeKam{}
	r := &reaper{store: st, kam: kam, live: func(context.Context) (map[string]bool, error) {
		st.put("ci-new", entry{switchingID: "sw-new", hEntry: "11", hID: "12"})
		return map[string]bool{}, nil
	}}
	if err := r.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(kam.ended) != 0 {
		t.Fatalf("reaper ended a just-started dialog: %+v", kam.ended)
	}
	if _, ok := st.get("ci-new"); !ok {
		t.Fatal("just-started call was untracked")
	}
}

func TestReaperUsesRecheckedDialogCoordinates(t *testing.T) {
	st := newStore()
	st.put("ci-move", entry{switchingID: "sw-move", hEntry: "3", hID: "4"})
	kam := &fakeKam{}
	r := &reaper{store: st, kam: kam, live: func(context.Context) (map[string]bool, error) {
		st.put("ci-move", entry{switchingID: "sw-move", hEntry: "21", hID: "22"})
		return map[string]bool{}, nil
	}}
	if err := r.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(kam.ended) != 1 || kam.ended[0] != [2]string{"21", "22"} {
		t.Fatalf("want rechecked dialog coordinates [21 22], got %+v", kam.ended)
	}
}

func TestReaperTickEndsGoneCalls(t *testing.T) {
	st := newStore()
	st.put("ci-1", entry{switchingID: "sw-1", hEntry: "7", hID: "9"})
	st.put("ci-2", entry{switchingID: "sw-2", hEntry: "8", hID: "10"})

	r := &reaper{store: st, kam: &fakeKam{}, live: func(ctx context.Context) (map[string]bool, error) {
		return map[string]bool{"sw-2": true}, nil
	}}
	if err := r.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(r.kam.(*fakeKam).ended) != 1 || r.kam.(*fakeKam).ended[0] != [2]string{"7", "9"} {
		t.Fatalf("unexpected ends: %+v", r.kam.(*fakeKam).ended)
	}
	if _, ok := st.get("ci-1"); ok {
		t.Fatal("gone call still tracked")
	}
	if _, ok := st.get("ci-2"); !ok {
		t.Fatal("live call was untracked")
	}
}
