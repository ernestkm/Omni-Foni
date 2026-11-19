package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"omnifoni/observability"
)

func TestEndDialogTreatsNotFoundAsGone(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	k := &kamRPC{base: srv.URL, http: srv.Client()}
	if err := k.endDialog(context.Background(), "7", "9"); err != nil {
		t.Fatalf("404 should mean gone, got %v", err)
	}
}

func TestFacadeClientPropagatesRequestID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(r.Header.Get("x-request-id"))
	}))
	defer srv.Close()

	f := &facadeClient{base: srv.URL, http: srv.Client()}
	ctx := observability.WithRequestID(context.Background(), "req-42")
	var got string
	if err := f.do(ctx, http.MethodGet, "/", nil, &got); err != nil {
		t.Fatal(err)
	}
	if got != "req-42" {
		t.Fatalf("want req-42, got %q", got)
	}
}

func TestEndDialogPropagatesRequestID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","result":{},"id":1}`))
		if strings.TrimSpace(r.Header.Get("x-request-id")) == "" {
			t.Error("missing request id header")
		}
	}))
	defer srv.Close()

	k := &kamRPC{base: srv.URL, http: srv.Client()}
	ctx := observability.WithRequestID(context.Background(), "req-43")
	if err := k.endDialog(ctx, "7", "9"); err != nil {
		t.Fatal(err)
	}
}
