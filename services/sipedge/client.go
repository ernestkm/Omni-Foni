package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
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

type facade interface {
	start(context.Context, startReq) (startResp, error)
	stop(context.Context, string) (bool, error)
}

type facadeClient struct {
	base string
	http *http.Client
}

func newFacadeClient(base string) *facadeClient {
	return &facadeClient{base: base, http: &http.Client{Timeout: 5 * time.Second}}
}

func (f *facadeClient) start(ctx context.Context, req startReq) (startResp, error) {
	var out startResp
	err := f.do(ctx, http.MethodPost, "/start", req, &out)
	return out, err
}

func (f *facadeClient) stop(ctx context.Context, id string) (bool, error) {
	var out struct {
		Stopped bool `json:"stopped"`
	}
	err := f.do(ctx, http.MethodPost, "/stop", map[string]string{"call_id": id}, &out)
	return out.Stopped, err
}

func (f *facadeClient) live(ctx context.Context) (map[string]bool, error) {
	var out struct {
		Calls []struct {
			CallID string `json:"call_id"`
		} `json:"calls"`
	}
	if err := f.do(ctx, http.MethodGet, "/live", nil, &out); err != nil {
		return nil, err
	}
	m := make(map[string]bool, len(out.Calls))
	for _, c := range out.Calls {
		m[c.CallID] = true
	}
	return m, nil
}

func (f *facadeClient) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, f.base+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := f.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type kamRPC struct {
	base string
	http *http.Client
}

func (k *kamRPC) endDialog(ctx context.Context, hEntry, hID string) error {
	return nil
}

type reaper struct {
	facade *facadeClient
	kam    *kamRPC
	store  *store
}

func newReaper(f *facadeClient, kam *kamRPC, st *store) *reaper {
	return &reaper{facade: f, kam: kam, store: st}
}

func (r *reaper) run(interval time.Duration) {}
