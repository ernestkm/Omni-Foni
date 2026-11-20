package main

import (
	"os"
	"path/filepath"
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

	findings := checkLicences(m, p)
	if len(findings) != 2 {
		t.Fatalf("expected two licence findings, got %#v", findings)
	}
	if findings[0].RuleID != "licence.denied" || findings[1].RuleID != "licence.not_allowed" {
		t.Fatalf("unexpected licence findings %#v", findings)
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
