package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"testing"
	"time"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"omnifoni/observability"
)

func TestIngesterMasksRecipientEmailInTerminalLog(t *testing.T) {
	resetOutbox(t)
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(observability.NewHandler(slog.NewJSONHandler(&buf, nil), "notifications")))

	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	ev := validEvent(now)
	exp := now.Add(-time.Hour).UnixMilli()
	ev.ExpiresAtMs = exp
	ev.ArtifactUrl = "http://localhost:50062/scheduled-reports/s1/artifacts/a1/download?exp=" + strconv.FormatInt(exp, 10) + "&sig=abc"
	b, err := proto.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewIngester(testStore).Handle(context.Background(), &kgo.Record{Value: b}); err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode log %q: %v", buf.String(), err)
	}
	if got["email"] != observability.Mask(ev.RecipientEmail) {
		t.Fatalf("email = %v, want %q", got["email"], observability.Mask(ev.RecipientEmail))
	}
	if got["recipient"] != nil {
		t.Fatalf("unredacted recipient attr: %v", got["recipient"])
	}
	if bytes.Contains(buf.Bytes(), []byte(ev.RecipientEmail)) {
		t.Fatalf("log leaked recipient email: %q", buf.String())
	}
}
