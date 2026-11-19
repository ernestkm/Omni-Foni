package main

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	schedulingpb "omnifoni/notifications/gen/schedulingpb"
)

func dedupeKey(artifactID, recipient string) string {
	return "report-ready:" + artifactID + ":" + recipient
}

func retryDelay(attempt int, base time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 5 {
		shift = 5
	}
	return base * time.Duration(1<<shift)
}

func validateReportReady(ev *schedulingpb.ReportReadyEvent, now time.Time) string {
	if strings.TrimSpace(ev.GetRecipientEmail()) == "" {
		return "missing recipient_email"
	}
	raw := strings.TrimSpace(ev.GetArtifactUrl())
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "malformed artifact_url"
	}
	if u.Query().Get("sig") == "" {
		return "missing artifact_url signature"
	}
	if u.Query().Get("exp") != strconv.FormatInt(ev.GetExpiresAtMs(), 10) {
		return "artifact_url expiry mismatch"
	}
	if ev.GetExpiresAtMs() <= now.UnixMilli() {
		return "expired artifact_url"
	}
	return ""
}

func notificationFromEvent(ev *schedulingpb.ReportReadyEvent, now time.Time) Notification {
	subject := "Omni-Foní scheduled report ready"
	body := fmt.Sprintf("Your %s report is ready.\n\nPeriod: %s to %s\nRows: %d\n\nDownload: %s\n\nThis link expires at %s.\n",
		ev.GetReportKind(), ev.GetPeriodFrom(), ev.GetPeriodTo(), ev.GetRowCount(),
		ev.GetArtifactUrl(), time.UnixMilli(ev.GetExpiresAtMs()).UTC().Format(time.RFC3339))
	n := Notification{
		DedupeKey: dedupeKey(ev.GetArtifactId(), ev.GetRecipientEmail()),
		Channel:   "email", Recipient: ev.GetRecipientEmail(), Subject: subject, Body: body,
		Status: "pending", NextAttemptAt: now,
	}
	if reason := validateReportReady(ev, now); reason != "" {
		n.Status = "failed"
		n.Attempts = 1
		n.LastError = "terminal: " + reason
	}
	return n
}
