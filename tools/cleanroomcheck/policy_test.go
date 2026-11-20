package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func validPolicy() *Policy {
	return &Policy{
		Version: 1,
		Root:    ".",
		SourceBasis: SourceBasis{
			Allowed: []Source{
				{ID: "S1", Status: "partially retrieved", Locations: []string{"docs/master-plan.md"}},
				{ID: "S2", Status: "unavailable", Locations: []string{"docs/master-plan.md"}},
				{ID: "S3", Status: "fully retrieved", Locations: []string{"docs/master-plan.md"}},
				{ID: "S4", Status: "fully retrieved", Locations: []string{"docs/master-plan.md"}},
			},
		},
		ExcludePaths: []string{".git/", ".worktrees/", "artifacts/", ".superpowers/"},
		ForbiddenArtifacts: []ForbiddenArtifact{
			{Pattern: "vendor/astpp*", Severity: "high"},
		},
		ForbiddenTerms: []ForbiddenTerm{
			{Term: "ASTPP", Severity: "high", AllowPaths: []string{"AGENTS.md", "CONTRIBUTING.md", "docs/", "tools/cleanroomcheck/"}},
		},
		ClaimRules: []ClaimRule{
			{PathGlob: "docs/master-plan.md"},
		},
		LicencePolicy: LicencePolicy{
			Manifest: "licence-manifest.json",
			Allow: []LicenceRule{
				{Licence: "Apache-2.0"},
			},
			Deny: []string{"BUSL", "RSALv2", "SSPL", "proprietary", "unknown"},
		},
		Exceptions: []Exception{},
	}
}

func validManifest() *LicenceManifest {
	return &LicenceManifest{
		Components: []Component{
			{Name: "demo", Version: "1", Licence: "Apache-2.0", Distribution: "source"},
		},
	}
}

func TestLoadPolicyRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	policyPath := filepath.Join(dir, "policy.json")
	manifestPath := filepath.Join(dir, "licence-manifest.json")
	if err := os.WriteFile(policyPath, []byte(`{"version":1,"unexpected":true}`), 0o644); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, manifestPath, validManifest())

	_, _, _, _, err := loadPolicy(policyPath, time.Now())
	if err == nil {
		t.Fatal("expected unknown field error")
	}
	if !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("expected unexpected field in error, got %v", err)
	}
}

func TestValidatePolicyAllowsDeniedLicenceThroughToFindings(t *testing.T) {
	p := validPolicy()
	m := validManifest()
	m.Components[0].Licence = "BUSL"

	if err := validatePolicy(p, m, time.Now()); err != nil {
		t.Fatalf("licence violations must be findings, not policy errors: %v", err)
	}
	if findings := checkLicences(m, p, "docs/compliance/licence-manifest.json"); len(findings) != 1 {
		t.Fatalf("expected one licence.denied finding, got %#v", findings)
	}
}

func TestValidatePolicyAcceptsCommittedManifestLicences(t *testing.T) {
	p := validPolicy()
	p.LicencePolicy.Allow = []LicenceRule{
		{Licence: "GPL-3.0", Distribution: "separate-binary"},
		{Licence: "GPL-3.0", Distribution: "tool"},
	}
	m := validManifest()
	m.Components = []Component{
		{Name: "rtpengine", Version: "master-plan", Licence: "GPL-3.0", Distribution: "separate-binary"},
		{Name: "ansible", Version: "master-plan", Licence: "GPL-3.0", Distribution: "tool"},
	}

	if err := validatePolicy(p, m, time.Now()); err != nil {
		t.Fatalf("expected two GPL-3.0 contexts to validate, got %v", err)
	}
}

func TestValidatePolicyRejectsMalformedGlobs(t *testing.T) {
	cases := map[string]func(*Policy){
		"claim_rules.path_glob": func(p *Policy) {
			p.ClaimRules = []ClaimRule{{PathGlob: "docs/[a-"}}
		},
		"forbidden_artifact_patterns": func(p *Policy) {
			p.ForbiddenArtifacts = []ForbiddenArtifact{{Pattern: "vendor/astpp[", Severity: "high"}}
		},
		"forbidden_terms.allow_paths": func(p *Policy) {
			p.ForbiddenTerms[0].AllowPaths = []string{"docs/[a-"}
		},
		"exclude_paths": func(p *Policy) {
			p.ExcludePaths = append(p.ExcludePaths, "build/[a-")
		},
		"exceptions.path_glob": func(p *Policy) {
			p.Exceptions = []Exception{{
				ID:        "broken",
				RuleID:    "term.forbidden",
				PathGlob:  "legacy/[a-",
				Reason:    "reviewed",
				Owner:     "maintainers",
				ExpiresOn: "2099-01-01",
			}}
		},
	}
	for name, mutate := range cases {
		p := validPolicy()
		mutate(p)
		err := validatePolicy(p, validManifest(), time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
		if err == nil {
			t.Fatalf("%s: expected malformed glob error", name)
		}
		if !strings.Contains(err.Error(), "glob") {
			t.Fatalf("%s: expected glob error, got %v", name, err)
		}
	}
}

func TestValidatePolicyRejectsDoubleStarGlobs(t *testing.T) {
	p := validPolicy()
	p.ClaimRules = []ClaimRule{{PathGlob: "docs/**"}}

	err := validatePolicy(p, validManifest(), time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected ** glob error")
	}
	if !strings.Contains(err.Error(), "**") {
		t.Fatalf("expected ** in error, got %v", err)
	}
}

func TestValidatePolicyRejectsInvalidTermExpiry(t *testing.T) {
	p := validPolicy()
	p.ForbiddenTerms[0].ExpiresOn = "2099-13-01"

	err := validatePolicy(p, validManifest(), time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected invalid expires_on error")
	}
}

func TestValidatePolicyRejectsExpiredException(t *testing.T) {
	p := validPolicy()
	p.Exceptions = []Exception{{
		ID:        "legacy",
		RuleID:    "term.forbidden",
		PathGlob:  "legacy/*",
		Reason:    "old import",
		Owner:     "maintainers",
		ExpiresOn: "2020-01-01",
	}}

	err := validatePolicy(p, validManifest(), time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected expired exception error")
	}
}

func TestMatchGlob(t *testing.T) {
	if !matchGlob(".git/", ".git/config") {
		t.Fatal("directory prefix glob should match")
	}
	if !matchGlob("docs/compliance/*.md", "docs/compliance/cleanroom-policy.json.md") {
		t.Fatal("file glob should match")
	}
	if matchGlob("docs/compliance/*.md", "docs/compliance/nested/file.md") {
		t.Fatal("file glob should not cross directories")
	}
}

func TestExceptionApplies(t *testing.T) {
	p := validPolicy()
	p.Exceptions = []Exception{{
		ID:        "future",
		RuleID:    "term.forbidden",
		PathGlob:  "legacy/",
		Reason:    "reviewed",
		Owner:     "maintainers",
		ExpiresOn: "2099-01-01",
	}}
	now := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)

	if !exceptionApplies(p, "term.forbidden", "legacy/file.txt", now) {
		t.Fatal("expected exception to apply")
	}
	if exceptionApplies(p, "artifact.forbidden", "legacy/file.txt", now) {
		t.Fatal("exception must be rule scoped")
	}
}

func TestValidatePolicyRequiresSuperpowersExclude(t *testing.T) {
	p := validPolicy()
	p.ExcludePaths = []string{".git/", ".worktrees/", "artifacts/"}

	err := validatePolicy(p, validManifest(), time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("expected missing .superpowers/ exclude error")
	}
}

func TestCommittedPolicyExcludesSDDArtifactsAndAllowsDocumentationQuotes(t *testing.T) {
	var p Policy
	policyPath := filepath.Join("..", "..", "docs", "compliance", "cleanroom-policy.json")
	if err := decodeJSONFile(policyPath, &p); err != nil {
		t.Fatal(err)
	}
	if !containsString(p.ExcludePaths, ".superpowers/") {
		t.Fatalf("expected .superpowers/ in exclude_paths, got %#v", p.ExcludePaths)
	}

	var term *ForbiddenTerm
	for index := range p.ForbiddenTerms {
		if p.ForbiddenTerms[index].Term == "astppbilling.org" {
			term = &p.ForbiddenTerms[index]
		}
	}
	if term == nil {
		t.Fatal("expected astppbilling.org forbidden term")
	}
	want := map[string]bool{
		"docs/master-plan.md":                                          true,
		"tools/cleanroomcheck/":                                        true,
		"docs/compliance/cleanroom-audit.md":                           true,
		"docs/superpowers/specs/2026-11-20-clean-room-audit-design.md": true,
		"docs/superpowers/plans/2026-11-20-clean-room-audit.md":        true,
	}
	if len(term.AllowPaths) != len(want) {
		t.Fatalf("expected %d exact allow paths, got %#v", len(want), term.AllowPaths)
	}
	for _, allowPath := range term.AllowPaths {
		if !want[allowPath] {
			t.Fatalf("unexpected allow path %q in %#v", allowPath, term.AllowPaths)
		}
	}
}
