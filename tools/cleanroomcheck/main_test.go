package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExecuteCleanAudit(t *testing.T) {
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.json")
	writeJSON(t, policyPath, validPolicy())
	writeJSON(t, filepath.Join(dir, "licence-manifest.json"), validManifest())

	result, err := execute(policyPath, "", "text", time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit 0, got %d: %s", result.ExitCode, result.Output)
	}
	if !strings.Contains(result.Output, "ok: clean-room audit 0 findings") {
		t.Fatalf("unexpected output %q", result.Output)
	}
}

func TestExecuteFindingsExitOne(t *testing.T) {
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.json")
	p := validPolicy()
	p.ForbiddenTerms[0].AllowPaths = nil
	writeJSON(t, policyPath, p)
	writeJSON(t, filepath.Join(dir, "licence-manifest.json"), validManifest())
	if err := os.WriteFile(filepath.Join(dir, "ASTPP source.txt"), []byte("ASTPP source\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := execute(policyPath, "", "text", time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("expected exit 1, got %d: %s", result.ExitCode, result.Output)
	}
}

func TestWriteReports(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "reports")
	findings := []Finding{{Severity: "high", RuleID: "term.forbidden", Path: "a.txt", Line: 1, Snippet: "x", Remediation: "remove"}}

	if err := writeReports(dir, findings); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cleanroom-report.json", "cleanroom-report.txt"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected %s: %v", name, err)
		}
	}
}
