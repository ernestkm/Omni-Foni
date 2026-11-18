package main

import (
	"reflect"
	"testing"
	"time"
)

func TestScore(t *testing.T) {
	cfg := Config{AlertThreshold: 60, SoftAccountCalls: 5, SoftPrefixCalls: 2}
	day := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	odd := time.Date(2026, 11, 18, 5, 59, 0, 0, time.UTC)

	cases := []struct {
		name       string
		signals    Signals
		wantScore  int
		wantReason []string
	}{
		{
			name:      "clean",
			signals:   Signals{Destination: "2782001", CLI: "2710001", Start: day},
			wantScore: 0,
		},
		{
			name: "risk prefix",
			signals: Signals{
				Destination: "9001234", CLI: "2710001", Start: day,
				RiskPrefixWeight: 30,
			},
			wantScore:  30,
			wantReason: []string{"risk_prefix"},
		},
		{
			name: "first seen prefix and cli",
			signals: Signals{
				Destination: "9001234", CLI: "2710001", Start: day,
				RiskPrefixWeight: 30, FirstSeenPrefix: true, FirstSeenCLI: true,
			},
			wantScore:  65,
			wantReason: []string{"risk_prefix", "first_seen_prefix", "first_seen_cli"},
		},
		{
			name: "elevated account velocity",
			signals: Signals{
				Destination: "2782001", CLI: "2710001", Start: day,
				AccountCalls: 5,
			},
			wantScore:  20,
			wantReason: []string{"velocity"},
		},
		{
			name: "elevated prefix velocity only once",
			signals: Signals{
				Destination: "2782001", CLI: "2710001", Start: day,
				AccountCalls: 5, PrefixCalls: 2,
			},
			wantScore:  20,
			wantReason: []string{"velocity"},
		},
		{
			name: "odd hour caps at one hundred",
			signals: Signals{
				Destination: "9001234", CLI: "1888555", Start: odd,
				RiskPrefixWeight: 100, FirstSeenPrefix: true, FirstSeenCLI: true,
				PrefixCalls: 2,
			},
			wantScore: 100,
			wantReason: []string{
				"risk_prefix", "first_seen_prefix", "velocity", "first_seen_cli", "odd_hour",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			score, reasons := Score(cfg, tc.signals)
			if score != tc.wantScore {
				t.Fatalf("score = %d, want %d", score, tc.wantScore)
			}
			if !reflect.DeepEqual(reasons, tc.wantReason) {
				t.Fatalf("reasons = %v, want %v", reasons, tc.wantReason)
			}
			if score >= cfg.AlertThreshold && Level(score) == "low" {
				t.Fatalf("alert-level score must not be low: %d", score)
			}
		})
	}
}

func TestLevel(t *testing.T) {
	cases := map[int]string{0: "low", 39: "low", 40: "medium", 79: "medium", 80: "high", 100: "high"}
	for score, want := range cases {
		if got := Level(score); got != want {
			t.Fatalf("Level(%d) = %q, want %q", score, got, want)
		}
	}
}

func TestDestinationPrefix(t *testing.T) {
	if got := DestinationPrefix("2782123456"); got != "2782" {
		t.Fatalf("prefix = %q", got)
	}
	if got := DestinationPrefix("900"); got != "900" {
		t.Fatalf("short prefix = %q", got)
	}
}
