package main

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestArtifactSignerRoundTrip(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	s := NewArtifactSigner("test-secret", "http://reports.test/", time.Hour)
	raw, exp := s.Sign("s1", "a1", now)
	if exp != now.Add(time.Hour).UnixMilli() {
		t.Fatalf("exp = %d", exp)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != "/scheduled-reports/s1/artifacts/a1/download" {
		t.Fatalf("path = %q", u.Path)
	}
	if u.Query().Get("exp") == "" || u.Query().Get("sig") == "" {
		t.Fatalf("missing query: %s", raw)
	}
	if !s.Valid("s1", "a1", u.Query().Get("exp"), u.Query().Get("sig"), now) {
		t.Fatal("valid signature rejected")
	}
	sid, aid, ok := s.ValidDownloadURL(raw, now)
	if !ok || sid != "s1" || aid != "a1" {
		t.Fatalf("ValidDownloadURL = %q %q %v", sid, aid, ok)
	}
}

func TestArtifactSignerRejectsExpiryAndTampering(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	s := NewArtifactSigner("test-secret", "http://reports.test", time.Hour)
	raw, _ := s.Sign("s1", "a1", now)
	u, _ := url.Parse(raw)
	exp := u.Query().Get("exp")
	sig := u.Query().Get("sig")

	if s.Valid("s1", "a1", exp, sig, now.Add(2*time.Hour)) {
		t.Fatal("expired signature accepted")
	}
	if s.Valid("s1", "a2", exp, sig, now) {
		t.Fatal("wrong artifact accepted")
	}
	badSig := "A" + sig[1:]
	if badSig == sig {
		badSig = "B" + sig[1:]
	}
	if s.Valid("s1", "a1", exp, badSig, now) {
		t.Fatal("tampered signature accepted")
	}
	if s.Valid("s1", "a1", "not-a-number", sig, now) {
		t.Fatal("bad exp accepted")
	}
	if _, _, ok := s.ValidDownloadURL(strings.Replace(raw, "/artifacts/a1/", "/artifacts/a2/", 1), now); ok {
		t.Fatal("wrong artifact path accepted")
	}
	if _, _, ok := s.ValidDownloadURL("http://reports.test/other", now); ok {
		t.Fatal("wrong path accepted")
	}
}
