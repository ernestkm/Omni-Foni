package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

type runResult struct {
	ExitCode int
	Output   string
}

func execute(policyPath, reportDir, format string, now time.Time) (runResult, error) {
	if format != "text" && format != "json" {
		return runResult{}, fmt.Errorf("unknown format %q", format)
	}
	findings, err := runAudit(policyPath, now)
	if err != nil {
		return runResult{}, err
	}
	if reportDir != "" {
		if err := writeReports(reportDir, findings); err != nil {
			return runResult{}, err
		}
	}
	output, err := formatFindings(format, findings)
	if err != nil {
		return runResult{}, err
	}
	exitCode := 0
	if len(findings) > 0 {
		exitCode = 1
	}
	return runResult{ExitCode: exitCode, Output: output}, nil
}

func main() {
	policyPath := flag.String("policy", "docs/compliance/cleanroom-policy.json", "path to clean-room policy")
	reportDir := flag.String("report", "", "optional report output directory")
	format := flag.String("format", "text", "output format: text or json")
	flag.Parse()

	result, err := execute(*policyPath, *reportDir, *format, time.Now().UTC())
	if err != nil {
		fmt.Fprintln(os.Stderr, "cleanroom:", err)
		os.Exit(2)
	}
	fmt.Print(result.Output)
	os.Exit(result.ExitCode)
}
