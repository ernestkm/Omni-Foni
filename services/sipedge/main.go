package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"omnifoni/observability"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	observability.SetupLogging("sipedge")
	shutdown, err := observability.Init(context.Background(), "sipedge")
	if err != nil {
		slog.Warn("tracing disabled", "err", err)
	}
	defer func() { _ = shutdown(context.Background()) }()

	accounts := parseAccounts(os.Getenv("SIPEDGE_TRUNK_ACCOUNTS"))
	facade := newFacadeClient(env("SIPEDGE_SWITCHING_HTTP", "http://switching:50070"))
	rpc := &kamRPC{base: env("SIPEDGE_KAMAILIO_RPC", "http://kamailio:5060/RPC"), http: &http.Client{Timeout: 5 * time.Second}}
	st := newStore()

	srv := &sipedgeServer{accounts: accounts, facade: facade, store: st}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /invite", srv.invite)
	mux.HandleFunc("POST /bye", srv.bye)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	if d := os.Getenv("SIPEDGE_REAP_INTERVAL"); d != "" && d != "0" {
		dur, err := time.ParseDuration(d)
		if err != nil {
			slog.Error("reap interval", "err", err)
			os.Exit(1)
		}
		go newReaper(facade, rpc, st).run(dur)
	}

	addr := env("SIPEDGE_SHIM_ADDR", ":50071")
	slog.Info("sipedge up", "addr", addr, "trunks", len(accounts))
	if err := http.ListenAndServe(addr, mux); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}
