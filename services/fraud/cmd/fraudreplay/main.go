package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	fraudscore "omnifoni/fraud/scoring"
)

type history struct {
	Destination string `json:"destination"`
	CLI         string `json:"cli"`
	StartedAt   string `json:"started_at"`
}

type testCase struct {
	AccountID   string    `json:"account_id"`
	Destination string    `json:"destination"`
	CLI         string    `json:"cli"`
	Start       string    `json:"start"`
	History     []history `json:"history"`
	Fraud       bool      `json:"fraud"`
}

type result struct {
	TP int
	FP int
	FN int
	TN int
}

func riskWeight(destination string) int {
	if strings.HasPrefix(destination, "900") {
		return 30
	}
	return 0
}

func signalsFor(c testCase, cfg fraudscore.Config, start time.Time) fraudscore.Signals {
	prefix := fraudscore.DestinationPrefix(c.Destination)
	seenPrefix := false
	firstCLI := true
	accountCalls := 0
	prefixCalls := 0
	for _, h := range c.History {
		if fraudscore.DestinationPrefix(h.Destination) == prefix {
			prefixCalls++
			seenPrefix = true
		}
		if h.CLI == c.CLI {
			firstCLI = false
		}
		if h.StartedAt != "" {
			accountCalls++
		}
	}
	return fraudscore.Signals{
		Destination:      c.Destination,
		CLI:              c.CLI,
		Start:            start,
		RiskPrefixWeight: riskWeight(c.Destination),
		FirstSeenPrefix:  !seenPrefix,
		FirstSeenCLI:     firstCLI,
		AccountCalls:     accountCalls,
		PrefixCalls:      prefixCalls,
	}
}

func run(path string, cfg fraudscore.Config) (result, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return result{}, 0, err
	}
	defer f.Close()

	res := result{}
	count := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		c := testCase{}
		if err := json.Unmarshal([]byte(line), &c); err != nil {
			return result{}, 0, err
		}
		start, err := time.Parse(time.RFC3339, c.Start)
		if err != nil {
			return result{}, 0, err
		}
		score, _ := fraudscore.Score(cfg, signalsFor(c, cfg, start))
		predicted := score >= cfg.AlertThreshold
		count++
		switch {
		case predicted && c.Fraud:
			res.TP++
		case predicted && !c.Fraud:
			res.FP++
		case !predicted && c.Fraud:
			res.FN++
		default:
			res.TN++
		}
	}
	if err := scanner.Err(); err != nil {
		return result{}, 0, err
	}
	return res, count, nil
}

func ratio(num, den int) float64 {
	if den == 0 {
		return 1.0
	}
	return float64(num) / float64(den)
}

func report(res result, count int, cfg fraudscore.Config) string {
	precision := ratio(res.TP, res.TP+res.FP)
	recall := ratio(res.TP, res.TP+res.FN)
	return fmt.Sprintf(`# Toll-fraud baseline

scorer_version: %s
cases: %d
threshold: %d
soft_account_calls: %d
soft_prefix_calls: %d
tp: %d
fp: %d
fn: %d
tn: %d
precision: %.3f
recall: %.3f
`, fraudscore.RuleVersion, count, cfg.AlertThreshold, cfg.SoftAccountCalls,
		cfg.SoftPrefixCalls, res.TP, res.FP, res.FN, res.TN, precision, recall)
}

func main() {
	casesPath := flag.String("cases", "testdata/fraud_cases.jsonl", "labelled JSONL cases")
	baselinePath := flag.String("baseline", "../../docs/fraud-baseline.md", "baseline report path")
	threshold := flag.Int("threshold", 60, "alert threshold")
	softAccount := flag.Int("soft-account-calls", 5, "soft account velocity")
	softPrefix := flag.Int("soft-prefix-calls", 2, "soft prefix velocity")
	flag.Parse()

	cfg := fraudscore.Config{
		AlertThreshold:   *threshold,
		SoftAccountCalls: *softAccount,
		SoftPrefixCalls:  *softPrefix,
	}
	res, count, err := run(*casesPath, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "replay:", err)
		os.Exit(1)
	}
	body := report(res, count, cfg)
	if err := os.MkdirAll(filepath.Dir(*baselinePath), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "baseline dir:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*baselinePath, []byte(body), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "baseline write:", err)
		os.Exit(1)
	}
	precision := ratio(res.TP, res.TP+res.FP)
	recall := ratio(res.TP, res.TP+res.FN)
	fmt.Printf("scorer_version=%s cases=%d tp=%d fp=%d fn=%d tn=%d precision=%.3f recall=%.3f run=%s\n",
		fraudscore.RuleVersion, count, res.TP, res.FP, res.FN, res.TN, precision, recall, uuid.NewString())
	if precision < 0.80 || recall < 0.70 {
		os.Exit(1)
	}
}
