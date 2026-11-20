package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCheckTermsHonorsAllowPathsAndExceptions(t *testing.T) {
	p := validPolicy()
	p.ForbiddenTerms[0].AllowPaths = nil
	p.Exceptions = []Exception{{
		ID:        "allowed-legacy",
		RuleID:    "term.forbidden",
		PathGlob:  "legacy/",
		Reason:    "reviewed",
		Owner:     "maintainers",
		ExpiresOn: "2099-01-01",
	}}
	now := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	files := []scannedFile{
		{Path: "legacy/note.txt", Lines: []string{"ASTPP source"}},
		{Path: "bad/note.txt", Lines: []string{"ASTPP source"}},
	}

	findings := checkTerms(files, p, now)
	if len(findings) != 1 {
		t.Fatalf("expected one finding, got %#v", findings)
	}
	if findings[0].Path != "bad/note.txt" || findings[0].Line != 1 {
		t.Fatalf("unexpected finding %#v", findings[0])
	}
}

func TestCheckClaimsRequiresSourceDeclaration(t *testing.T) {
	p := validPolicy()
	p.ClaimRules = []ClaimRule{{PathGlob: "docs/*.md"}}
	files := []scannedFile{
		{Path: "docs/good.md", Lines: []string{"Source basis: S1", "body"}},
		{Path: "docs/bad.md", Lines: []string{"body only"}},
	}

	findings := checkClaims(files, p)
	if len(findings) != 1 || findings[0].Path != "docs/bad.md" {
		t.Fatalf("expected one finding for docs/bad.md, got %#v", findings)
	}
}

func TestCheckLicencesRejectsDeniedAndUnlisted(t *testing.T) {
	p := validPolicy()
	m := validManifest()
	m.Components = append(m.Components,
		Component{Name: "bad", Version: "1", Licence: "BUSL", Distribution: "tool"},
		Component{Name: "unlisted", Version: "1", Licence: "ISC", Distribution: "library"},
	)

	findings := checkLicences(m, p, "docs/compliance/licence-manifest.json")
	if len(findings) != 2 {
		t.Fatalf("expected two licence findings, got %#v", findings)
	}
	if findings[0].RuleID != "licence.denied" || findings[1].RuleID != "licence.not_allowed" {
		t.Fatalf("unexpected licence findings %#v", findings)
	}
	if findings[0].Path != "docs/compliance/licence-manifest.json" {
		t.Fatalf("expected licence findings to name the manifest, got %#v", findings[0])
	}
}

func TestCheckLicencesEnforcesDistributionContext(t *testing.T) {
	p := validPolicy()
	p.LicencePolicy.Allow = []LicenceRule{
		{Licence: "GPL-3.0", Distribution: "separate-binary"},
		{Licence: "GPL-3.0", Distribution: "tool"},
	}
	m := validManifest()
	m.Components = []Component{
		{Name: "rtpengine", Version: "master-plan", Licence: "GPL-3.0", Distribution: "separate-binary"},
		{Name: "ansible", Version: "master-plan", Licence: "GPL-3.0", Distribution: "tool"},
		{Name: "linked", Version: "1", Licence: "GPL-3.0", Distribution: "linked"},
	}

	findings := checkLicences(m, p, "docs/compliance/licence-manifest.json")
	if len(findings) != 1 {
		t.Fatalf("expected one distribution finding, got %#v", findings)
	}
	if findings[0].RuleID != "licence.not_allowed" || !strings.Contains(findings[0].Snippet, "linked") {
		t.Fatalf("unexpected finding %#v", findings[0])
	}
}

func TestCheckTermsAllowPathsExpire(t *testing.T) {
	p := validPolicy()
	p.ForbiddenTerms[0].AllowPaths = []string{"docs/"}
	p.ForbiddenTerms[0].ExpiresOn = "2026-01-01"
	files := []scannedFile{{Path: "docs/note.md", Lines: []string{"ASTPP source"}}}

	if findings := checkTerms(files, p, time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)); len(findings) != 0 {
		t.Fatalf("expected allow path to apply before expiry, got %#v", findings)
	}
	findings := checkTerms(files, p, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	if len(findings) != 1 {
		t.Fatalf("expected allow path to stop applying after expiry, got %#v", findings)
	}
}

func TestExcludeAuditConfigFilesKeepsLicenceManifest(t *testing.T) {
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.json")
	files := []scannedFile{
		{Path: "policy.json", Lines: []string{"{}"}},
		{Path: "licence-manifest.json", Lines: []string{"{\"components\": []}"}},
		{Path: "docs/a.md", Lines: []string{"body"}},
	}

	kept := excludeAuditConfigFiles(dir, policyPath, files)
	if len(kept) != 2 {
		t.Fatalf("expected policy excluded and manifest kept, got %#v", kept)
	}
	for _, file := range kept {
		if file.Path == "policy.json" {
			t.Fatalf("policy file should be excluded, got %#v", kept)
		}
	}
}

func TestCheckArtifactsMatchesVendorSubtree(t *testing.T) {
	p := validPolicy()
	files := []scannedFile{{Path: "vendor/astpp-1.0/schema.sql", Lines: []string{"CREATE TABLE x ()"}}}

	findings := checkArtifacts(files, p, time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	if len(findings) != 1 || findings[0].RuleID != "artifact.forbidden" {
		t.Fatalf("expected one artifact finding, got %#v", findings)
	}
}

func TestSortFindingsIsDeterministic(t *testing.T) {
	findings := []Finding{
		{Path: "b.txt", Line: 2, RuleID: "term.forbidden", Severity: "high"},
		{Path: "a.txt", Line: 2, RuleID: "licence.denied", Severity: "critical"},
		{Path: "a.txt", Line: 1, RuleID: "term.forbidden", Severity: "high"},
	}

	sortFindings(findings)
	if findings[0].Path != "a.txt" || findings[0].Line != 1 || findings[0].RuleID != "term.forbidden" {
		t.Fatalf("unexpected first finding %#v", findings[0])
	}
	if findings[1].Path != "a.txt" || findings[1].Line != 2 || findings[1].RuleID != "licence.denied" {
		t.Fatalf("unexpected second finding %#v", findings[1])
	}
	if findings[2].Path != "b.txt" || findings[2].Line != 2 || findings[2].RuleID != "term.forbidden" {
		t.Fatalf("unexpected third finding %#v", findings[2])
	}
}

func TestRunAuditSortsAndReturnsFindings(t *testing.T) {
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.json")
	policy := validPolicy()
	policy.Root = "."
	policy.ClaimRules = nil
	policy.ClaimRules = []ClaimRule{{PathGlob: "docs/master-plan.md"}}
	writeJSON(t, policyPath, policy)
	writeJSON(t, filepath.Join(dir, "licence-manifest.json"), validManifest())
	if err := os.MkdirAll(filepath.Join(dir, "docs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "docs", "bad.md"), []byte("ASTPP source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy.ForbiddenTerms[0].AllowPaths = nil
	writeJSON(t, policyPath, policy)

	findings, err := runAudit(policyPath, time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].RuleID != "term.forbidden" {
		t.Fatalf("expected one term finding, got %#v", findings)
	}
}

func TestRunAuditReportsDeniedLicenceAndWritesReport(t *testing.T) {
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.json")
	policy := validPolicy()
	writeJSON(t, policyPath, policy)
	manifest := validManifest()
	manifest.Components[0].Licence = "BUSL"
	writeJSON(t, filepath.Join(dir, "licence-manifest.json"), manifest)

	result, err := execute(policyPath, filepath.Join(dir, "reports"), "text", time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("denied licence must be a finding, not an error: %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("expected exit 1, got %d: %s", result.ExitCode, result.Output)
	}
	if !strings.Contains(result.Output, "licence.denied") {
		t.Fatalf("expected licence.denied in output, got %q", result.Output)
	}
	for _, name := range []string{"cleanroom-report.txt", "cleanroom-report.json"} {
		if _, err := os.Stat(filepath.Join(dir, "reports", name)); err != nil {
			t.Fatalf("expected report %s to be written: %v", name, err)
		}
	}
}

func TestExecuteRejectsUnknownFormat(t *testing.T) {
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.json")
	writeJSON(t, policyPath, validPolicy())
	writeJSON(t, filepath.Join(dir, "licence-manifest.json"), validManifest())

	if _, err := execute(policyPath, "", "xml", time.Now()); err == nil {
		t.Fatal("expected unknown format error")
	}
}
