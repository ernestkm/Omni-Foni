package main

import (
	"encoding/json"
	"net/http"
	"time"

	"google.golang.org/grpc"

	pb "omnifoni/switching/gen/switchingpb"
)

type startReq struct {
	AccountID   string `json:"account_id"`
	Destination string `json:"destination"`
	CLI         string `json:"cli"`
	SourceIP    string `json:"source_ip"`
}

type startResp struct {
	Allowed    bool   `json:"allowed"`
	Reason     string `json:"reason,omitempty"`
	CallID     string `json:"call_id,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
	MaxSeconds int64  `json:"max_seconds,omitempty"`
	TargetURI  string `json:"target_uri,omitempty"`
}

type liveCall struct {
	CallID         string `json:"call_id"`
	SessionID      string `json:"session_id"`
	AccountID      string `json:"account_id"`
	Destination    string `json:"destination"`
	CLI            string `json:"cli"`
	TrunkID        string `json:"trunk_id"`
	StartedAtMs    int64  `json:"started_at_ms"`
	ElapsedSeconds int64  `json:"elapsed_seconds"`
	MaxSeconds     int64  `json:"max_seconds"`
	ChargedSeconds int64  `json:"charged_seconds"`
}

// newFacade exposes the call registry to the SIP edge over HTTP.
func newFacade(reg *Registry, cc *grpc.ClientConn) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /start", func(w http.ResponseWriter, r *http.Request) {
		var req startReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if req.AccountID == "" || req.Destination == "" {
			http.Error(w, "account_id and destination required", http.StatusBadRequest)
			return
		}
		res, err := reg.StartCall(r.Context(), cc, &pb.StartCallRequest{
			AccountId: req.AccountID, Destination: req.Destination,
			Cli: req.CLI, SourceIp: req.SourceIP,
		})
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		writeJSON(w, startResp{
			Allowed: res.GetAllowed(), Reason: res.GetReason(),
			CallID: res.GetCallId(), SessionID: res.GetSessionId(),
			MaxSeconds: res.GetMaxSeconds(), TargetURI: res.GetTargetUri(),
		})
	})
	mux.HandleFunc("POST /stop", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			CallID string `json:"call_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.CallID == "" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]bool{"stopped": reg.StopCall(req.CallID)})
	})
	mux.HandleFunc("GET /live", func(w http.ResponseWriter, _ *http.Request) {
		snaps := reg.LiveCalls(nil)
		now := time.Now()
		out := make([]liveCall, 0, len(snaps))
		for _, c := range snaps {
			elapsed := int64(now.Sub(c.StartedAt).Seconds())
			if elapsed < 0 {
				elapsed = 0
			}
			out = append(out, liveCall{
				CallID: c.CallID, SessionID: c.SessionID, AccountID: c.AccountID,
				Destination: c.Destination, CLI: c.CLI, TrunkID: c.TrunkID,
				StartedAtMs: c.StartedAt.UnixMilli(), ElapsedSeconds: elapsed,
				MaxSeconds: c.MaxSeconds, ChargedSeconds: c.ChargedSeconds,
			})
		}
		writeJSON(w, map[string]any{"calls": out})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
