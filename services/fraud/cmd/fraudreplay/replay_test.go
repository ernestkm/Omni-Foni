package main

import (
	"os"
	"strings"
	"testing"

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
