package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type reportPayload struct {
	FindingCount int       `json:"finding_count"`
	Findings     []Finding `json:"findings"`
}

func formatFindings(format string, findings []Finding) (string, error) {
	switch format {
	case "text":
		return formatText(findings), nil
	case "json":
		payload := reportPayload{FindingCount: len(findings), Findings: findings}
		data, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return "", err
		}
		return string(data) + "\n", nil
	default:
		return "", fmt.Errorf("unknown format %q", format)
	}
}

func formatText(findings []Finding) string {
	if len(findings) == 0 {
		return "ok: clean-room audit 0 findings\n"
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "clean-room audit: %d findings\n", len(findings))
	for _, finding := range findings {
		fmt.Fprintf(&builder, "%s:%d %s %s: %s\n", finding.Path, finding.Line, finding.Severity, finding.RuleID, finding.Snippet)
	}
	return builder.String()
}

func writeReports(dir string, findings []Finding) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create report directory %s: %w", dir, err)
	}
	text, err := formatFindings("text", findings)
	if err != nil {
		return err
	}
	jsonText, err := formatFindings("json", findings)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "cleanroom-report.txt"), []byte(text), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "cleanroom-report.json"), []byte(jsonText), 0o644); err != nil {
		return err
	}
	return nil
}
