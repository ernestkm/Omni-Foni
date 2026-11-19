package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type ArtifactSigner struct {
	secret  []byte
	baseURL string
	ttl     time.Duration
}

func NewArtifactSigner(secret, baseURL string, ttl time.Duration) *ArtifactSigner {
	return &ArtifactSigner{secret: []byte(secret), baseURL: strings.TrimRight(baseURL, "/"), ttl: ttl}
}

func (s *ArtifactSigner) mac(scheduleID, artifactID string, exp int64) []byte {
	h := hmac.New(sha256.New, s.secret)
	fmt.Fprintf(h, "%s|%s|%d", scheduleID, artifactID, exp)
	return h.Sum(nil)
}

func (s *ArtifactSigner) Sign(scheduleID, artifactID string, now time.Time) (string, int64) {
	exp := now.Add(s.ttl).UnixMilli()
	sig := base64.RawURLEncoding.EncodeToString(s.mac(scheduleID, artifactID, exp))
	return fmt.Sprintf("%s/scheduled-reports/%s/artifacts/%s/download?exp=%d&sig=%s",
		s.baseURL, url.PathEscape(scheduleID), url.PathEscape(artifactID), exp, sig), exp
}

func (s *ArtifactSigner) Valid(scheduleID, artifactID, expRaw, sig string, now time.Time) bool {
	exp, err := strconv.ParseInt(expRaw, 10, 64)
	if err != nil || exp <= now.UnixMilli() {
		return false
	}
	got, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return false
	}
	return hmac.Equal(got, s.mac(scheduleID, artifactID, exp))
}

func (s *ArtifactSigner) ValidDownloadURL(raw string, now time.Time) (string, string, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 5 || parts[0] != "scheduled-reports" || parts[2] != "artifacts" || parts[4] != "download" {
		return "", "", false
	}
	scheduleID, artifactID := parts[1], parts[3]
	if !s.Valid(scheduleID, artifactID, u.Query().Get("exp"), u.Query().Get("sig"), now) {
		return "", "", false
	}
	return scheduleID, artifactID, true
}
