package main

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	fraudscore "omnifoni/fraud/scoring"
)

func TestReplayBaseline(t *testing.T) {
	cfg := fraudscore.Config{AlertThreshold: 60, SoftAccountCalls: 5, SoftPrefixCalls: 2}
	res, count, err := run("../../testdata/fraud_cases.jsonl", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if count != 35 {
		t.Fatalf("case count = %d, want 35", count)
	}
	if res.TP != 15 || res.FP != 0 || res.FN != 5 || res.TN != 15 {
		t.Fatalf("unexpected confusion matrix: %+v", res)
	}
	precision := ratio(res.TP, res.TP+res.FP)
	recall := ratio(res.TP, res.TP+res.FN)
	if precision < 0.80 || recall < 0.70 {
		t.Fatalf("below gate: precision=%.3f recall=%.3f", precision, recall)
	}
	body, err := os.ReadFile("../../../../docs/fraud-baseline.md")
	if err != nil {
		t.Fatal(err)
	}
	want := report(res, count, cfg)
	if string(body) != want {
		t.Fatalf("baseline mismatch\ngot:\n%s\nwant:\n%s", body, want)
	}
	if !strings.Contains(want, "precision: 1.000") {
		t.Fatalf("missing precision in report: %s", want)
	}
	if !strings.Contains(want, "recall: 0.750") {
		t.Fatalf("missing expected recall in report: %s", want)
	}
}

func TestReplayDetectedCasesUseDayScopedVelocity(t *testing.T) {
	cfg := fraudscore.Config{AlertThreshold: 60, SoftAccountCalls: 5, SoftPrefixCalls: 2}
	f, err := os.Open("../../testdata/fraud_cases.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var c testCase
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			t.Fatal(err)
		}
		start, err := time.Parse(time.RFC3339, c.Start)
		if err != nil {
			t.Fatal(err)
		}
		score, _ := fraudscore.Score(cfg, signalsFor(c, cfg, start))
		if score < cfg.AlertThreshold {
			continue
		}
		prefix := fraudscore.DestinationPrefix(c.Destination)
		for _, h := range c.History {
			if fraudscore.DestinationPrefix(h.Destination) != prefix || h.StartedAt == "" {
				continue
			}
			at, err := time.Parse(time.RFC3339, h.StartedAt)
			if err != nil {
				t.Fatal(err)
			}
			day := at.UTC().Format("2006-01-02")
			startDay := start.UTC().Format("2006-01-02")
			if day != startDay {
				t.Fatalf("%s prefix history at %s is not on case day %s", c.AccountID, day, startDay)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}
