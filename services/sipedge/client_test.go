package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"omnifoni/observability"
)

func writeRawNotFound(t *testing.T, w http.ResponseWriter, reason, contentType, body string) {
	t.Helper()
	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(buf, "HTTP/1.1 404 %s\r\nContent-Type: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		reason, contentType, len(body), body)
	_ = buf.Flush()
}

func TestEndDialogRejectsGenericNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	k := &kamRPC{base: srv.URL, http: srv.Client()}
	if err := k.endDialog(context.Background(), "7", "9"); err == nil {
		t.Fatal("generic 404 must be an error, not a gone dialog")
	}
}

func TestEndDialogAcceptsDialogNotFoundStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeRawNotFound(t, w, "Dialog not found", "application/json", `{}`)
	}))
	defer srv.Close()

	k := &kamRPC{base: srv.URL, http: srv.Client()}
	if err := k.endDialog(context.Background(), "7", "9"); err != nil {
		t.Fatalf("dialog-gone 404 should mean gone, got %v", err)
	}
}

func TestEndDialogAcceptsDialogNotFoundJSONError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeRawNotFound(t, w, "Not Found", "application/json", `{"error":{"message":"Dialog not found (7/9)"}}`)
	}))
	defer srv.Close()

	k := &kamRPC{base: srv.URL, http: srv.Client()}
	if err := k.endDialog(context.Background(), "7", "9"); err != nil {
		t.Fatalf("dialog-gone JSON 404 should mean gone, got %v", err)
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
