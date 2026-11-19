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

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"omnifoni/observability"
)

func instrumentedClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: otelhttp.NewTransport(http.DefaultTransport)}
}

func propagateRequestID(ctx context.Context, req *http.Request) {
	if id := observability.RequestIDFrom(ctx); id != "" {
		req.Header.Set("x-request-id", id)
	}
}

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
	return &facadeClient{base: base, http: instrumentedClient(5 * time.Second)}
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
	propagateRequestID(ctx, req)
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

// kamRPC talks to Kamailio's jsonrpcs HTTP transport.
type kamRPC struct {
	base string
	http *http.Client
}

func (k *kamRPC) endDialog(ctx context.Context, hEntry, hID string) error {
	body := `{"jsonrpc":"2.0","method":"dlg.end_dlg","params":[` + hEntry + `,` + hID + `],"id":1}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, k.base, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	propagateRequestID(ctx, req)
	resp, err := k.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if strings.Contains(resp.Status, "Dialog not found") {
			return nil
		}
		var gone struct {
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(b, &gone) == nil && gone.Error != nil &&
			strings.Contains(gone.Error.Message, "Dialog not found") {
			return nil
		}
		return fmt.Errorf("kamailio rpc status 404: %s", strings.TrimSpace(string(b)))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kamailio rpc status %d", resp.StatusCode)
	}
	var out struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	if out.Error != nil {
		return fmt.Errorf("kamailio rpc: %s", out.Error.Message)
	}
	return nil
}
