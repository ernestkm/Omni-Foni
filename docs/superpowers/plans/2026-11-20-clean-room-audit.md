# Clean-room Audit And Governance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a blocking, deterministic clean-room audit gate and a full community governance pack.

**Architecture:** A stdlib-only Go checker reads a committed JSON policy and licence manifest, scans repository text files for forbidden artifacts/terms, verifies declared source basis in designated docs, validates declared licences, and emits deterministic text/JSON findings. Make and CI run it as a blocking gate; the governance pack documents contribution, conduct, governance, security reporting, releases, maintainer onboarding, and clean-room review.

**Tech Stack:** Go 1.22 stdlib, JSON, Make, GitHub Actions, Markdown.

Source basis: S1, S2, S3, S4

## Global Constraints

- Work in `/root/Omni-Foní/.worktrees/p7-cleanroom-governance` on branch `p7-cleanroom-governance`.
- Never read ASTPP source code, schemas, or licensed assets. The checker scans repository text only.
- Feature claims come only from S1–S4 in `docs/master-plan.md` sections 0 and 8.
- No network access, no external scanner downloads, no ASTPP checkout, no SBOM generation, and no vendored licence-file collection.
- No changes to application source, proto, database schemas, Docker Compose, Kubernetes manifests, or generated code.
- Script modes stay `100644`; do not chmod scripts.
- Use `grep` or `python3`; `rg` and `jq` are unavailable.
- Use `COMPOSE_PROJECT_NAME=omni-fon` for stack/test commands.
- Never stage `.superpowers/sdd/progress.md`, briefs, reports, or diffs.
- Reports live under `artifacts/`, are ignored, and are never committed.
- Git auto-config identity warnings are expected; do not amend commits unless asked.
- `make test` must remain green.

## File Structure

Create:

- `tools/cleanroomcheck/go.mod` — module definition.
- `tools/cleanroomcheck/policy.go` — policy structs, JSON loading, validation, glob/exception helpers.
- `tools/cleanroomcheck/policy_test.go` — policy loader and validator tests.
- `tools/cleanroomcheck/scan.go` — repository walker and UTF-8 text admission.
- `tools/cleanroomcheck/scan_test.go` — walker tests.
- `tools/cleanroomcheck/rules.go` — rule checks, finding type, audit orchestration.
- `tools/cleanroomcheck/rules_test.go` — rule tests.
- `tools/cleanroomcheck/report.go` — deterministic text/JSON reports.
- `tools/cleanroomcheck/main.go` — flags, exit codes, CLI.
- `tools/cleanroomcheck/main_test.go` — CLI behavior tests.
- `docs/compliance/cleanroom-policy.json` — versioned policy manifest.
- `docs/compliance/licence-manifest.json` — declared components from master-plan section 4.
- `docs/compliance/cleanroom-audit.md` — operator guide.
- `CONTRIBUTING.md` — contribution workflow.
- `CODE_OF_CONDUCT.md` — community standards.
- `GOVERNANCE.md` — roles and decisions.
- `SECURITY.md` — vulnerability reporting.
- `docs/governance/rfc-template.md` — RFC template.
- `docs/governance/release-policy.md` — release policy.
- `docs/governance/maintainer-onboarding.md` — maintainer onboarding.
- `docs/governance/cleanroom-checklist.md` — clean-room review checklist.
- `.github/ISSUE_TEMPLATE/bug_report.yml` — bug issue template.
- `.github/ISSUE_TEMPLATE/feature_request.yml` — feature issue template.
- `.github/pull_request_template.md` — PR checklist.

Modify:

- `Makefile` — add `cleanroom` and make `test` depend on it.
- `.github/workflows/ci.yml` — add blocking clean-room step and report upload.
- `.gitignore` — ignore `artifacts/`.
- `AGENTS.md` — add a short status line for this slice.

---

### Task 1: Policy manifests and loader

**Files:**
- Create: `docs/compliance/cleanroom-policy.json`
- Create: `docs/compliance/licence-manifest.json`
- Create: `tools/cleanroomcheck/go.mod`
- Create: `tools/cleanroomcheck/policy.go`
- Test: `tools/cleanroomcheck/policy_test.go`

**Interfaces:**
- Consumes: none.
- Produces:
  - `type Policy struct`
  - `type LicenceManifest struct`
  - `func loadPolicy(policyPath string, now time.Time) (*Policy, *LicenceManifest, string, error)`
  - `func validatePolicy(p *Policy, m *LicenceManifest, now time.Time) error`
  - `func matchGlob(glob, path string) bool`
  - `func isInPaths(rel string, paths []string) bool`
  - `func exceptionApplies(p *Policy, ruleID, rel string, now time.Time) bool`
  - test helpers `validPolicy()`, `validManifest()`, `writeJSON()`

- [ ] **Step 1: Write the failing policy loader tests**

Create `tools/cleanroomcheck/policy_test.go`:

```go
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
		ExcludePaths: []string{".git/", ".worktrees/", "artifacts/"},
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

	_, _, _, err := loadPolicy(policyPath, time.Now())
	if err == nil {
		t.Fatal("expected unknown field error")
	}
	if !strings.Contains(err.Error(), "unexpected") {
		t.Fatalf("expected unexpected field in error, got %v", err)
	}
}

func TestValidatePolicyRejectsDeniedLicence(t *testing.T) {
	p := validPolicy()
	m := validManifest()
	m.Components[0].Licence = "BUSL"

	err := validatePolicy(p, m, time.Now())
	if err == nil {
		t.Fatal("expected denied licence error")
	}
}

func TestValidatePolicyRejectsExpiredException(t *testing.T) {
	p := validPolicy()
	p.Exceptions = []Exception{{
		ID:         "legacy",
		RuleID:     "term.forbidden",
		PathGlob:   "legacy/*",
		Reason:     "old import",
		Owner:      "maintainers",
		ExpiresOn:  "2020-01-01",
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd tools/cleanroomcheck && go test ./...
```

Expected: FAIL because package files do not exist yet.

- [ ] **Step 3: Create the Go module**

Create `tools/cleanroomcheck/go.mod`:

```go
module omnifoni/cleanroomcheck

go 1.22
```

- [ ] **Step 4: Write the policy implementation**

Create `tools/cleanroomcheck/policy.go`:

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

type Policy struct {
	Version            int                 `json:"version"`
	Root               string              `json:"root"`
	SourceBasis        SourceBasis         `json:"source_basis"`
	ExcludePaths       []string            `json:"exclude_paths"`
	ForbiddenArtifacts []ForbiddenArtifact `json:"forbidden_artifact_patterns"`
	ForbiddenTerms     []ForbiddenTerm     `json:"forbidden_terms"`
	ClaimRules         []ClaimRule         `json:"claim_rules"`
	LicencePolicy      LicencePolicy       `json:"licence_policy"`
	Exceptions         []Exception         `json:"exceptions"`
}

type SourceBasis struct {
	Allowed []Source `json:"allowed"`
}

type Source struct {
	ID        string   `json:"id"`
	Status    string   `json:"status"`
	Locations []string `json:"locations"`
}

type ForbiddenArtifact struct {
	Pattern  string `json:"pattern"`
	Severity string `json:"severity"`
}

type ForbiddenTerm struct {
	Term       string   `json:"term"`
	Severity   string   `json:"severity"`
	AllowPaths []string `json:"allow_paths"`
}

type ClaimRule struct {
	PathGlob string `json:"path_glob"`
}

type LicencePolicy struct {
	Manifest string        `json:"manifest"`
	Allow    []LicenceRule `json:"allow"`
	Deny     []string      `json:"deny"`
}

type LicenceRule struct {
	Licence      string `json:"licence"`
	Distribution string `json:"distribution"`
	Notes        string `json:"notes"`
}

type LicenceManifest struct {
	Components []Component `json:"components"`
}

type Component struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	Licence      string `json:"licence"`
	Distribution string `json:"distribution"`
	Notes        string `json:"notes"`
}

type Exception struct {
	ID        string `json:"id"`
	RuleID    string `json:"rule_id"`
	PathGlob  string `json:"path_glob"`
	Reason    string `json:"reason"`
	Owner     string `json:"owner"`
	ExpiresOn string `json:"expires_on"`
}

func decodeJSONFile(filePath string, value any) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open %s: %w", filePath, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode %s: %w", filePath, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("%s: trailing JSON data", filePath)
	}
	return nil
}

func loadPolicy(policyPath string, now time.Time) (*Policy, *LicenceManifest, string, error) {
	var policy Policy
	if err := decodeJSONFile(policyPath, &policy); err != nil {
		return nil, nil, "", err
	}
	if policy.LicencePolicy.Manifest == "" {
		return nil, nil, "", fmt.Errorf("licence_policy.manifest is required")
	}

	manifestPath := filepath.Join(filepath.Dir(policyPath), policy.LicencePolicy.Manifest)
	var manifest LicenceManifest
	if err := decodeJSONFile(manifestPath, &manifest); err != nil {
		return nil, nil, "", err
	}

	root := filepath.Clean(filepath.Join(filepath.Dir(policyPath), policy.Root))
	if err := validatePolicy(&policy, &manifest, now); err != nil {
		return nil, nil, "", err
	}
	return &policy, &manifest, root, nil
}

func validatePolicy(p *Policy, m *LicenceManifest, now time.Time) error {
	if p.Version != 1 {
		return fmt.Errorf("version must be 1")
	}
	if p.Root == "" {
		return fmt.Errorf("root is required")
	}

	requiredSources := []string{"S1", "S2", "S3", "S4"}
	seenSources := map[string]bool{}
	for _, source := range p.SourceBasis.Allowed {
		if !containsString(requiredSources, source.ID) {
			return fmt.Errorf("unknown source id %q", source.ID)
		}
		if seenSources[source.ID] {
			return fmt.Errorf("duplicate source id %q", source.ID)
		}
		seenSources[source.ID] = true
		if source.Status == "" {
			return fmt.Errorf("source %s status is required", source.ID)
		}
		if len(source.Locations) != 1 || source.Locations[0] != "docs/master-plan.md" {
			return fmt.Errorf("source %s must have one location docs/master-plan.md", source.ID)
		}
	}
	for _, id := range requiredSources {
		if !seenSources[id] {
			return fmt.Errorf("source_basis.allowed missing %s", id)
		}
	}

	for _, required := range []string{".git/", ".worktrees/", "artifacts/"} {
		if !containsString(p.ExcludePaths, required) {
			return fmt.Errorf("exclude_paths must contain %s", required)
		}
	}
	for _, exclude := range p.ExcludePaths {
		if exclude == "" {
			return fmt.Errorf("exclude_paths cannot contain an empty value")
		}
	}

	validSeverity := map[string]bool{"low": true, "medium": true, "high": true, "critical": true}
	if len(p.ForbiddenArtifacts) == 0 {
		return fmt.Errorf("forbidden_artifact_patterns must not be empty")
	}
	for _, artifact := range p.ForbiddenArtifacts {
		if artifact.Pattern == "" {
			return fmt.Errorf("forbidden artifact pattern is required")
		}
		if !validSeverity[artifact.Severity] {
			return fmt.Errorf("invalid severity %q for artifact %q", artifact.Severity, artifact.Pattern)
		}
	}
	if len(p.ForbiddenTerms) == 0 {
		return fmt.Errorf("forbidden_terms must not be empty")
	}
	for _, term := range p.ForbiddenTerms {
		if term.Term == "" {
			return fmt.Errorf("forbidden term is required")
		}
		if !validSeverity[term.Severity] {
			return fmt.Errorf("invalid severity %q for term %q", term.Severity, term.Term)
		}
	}
	if len(p.ClaimRules) == 0 {
		return fmt.Errorf("claim_rules must not be empty")
	}
	for _, rule := range p.ClaimRules {
		if rule.PathGlob == "" {
			return fmt.Errorf("claim rule path_glob is required")
		}
	}

	if p.LicencePolicy.Manifest == "" {
		return fmt.Errorf("licence_policy.manifest is required")
	}
	if len(p.LicencePolicy.Allow) == 0 {
		return fmt.Errorf("licence_policy.allow must not be empty")
	}
	if len(p.LicencePolicy.Deny) == 0 {
		return fmt.Errorf("licence_policy.deny must not be empty")
	}

	denied := map[string]bool{}
	for _, licence := range p.LicencePolicy.Deny {
		if licence == "" {
			return fmt.Errorf("licence_policy.deny cannot contain an empty value")
		}
		denied[licence] = true
	}
	for _, required := range []string{"BUSL", "RSALv2", "SSPL", "proprietary", "unknown"} {
		if !denied[required] {
			return fmt.Errorf("licence_policy.deny must contain %s", required)
		}
	}

	allowed := map[string]LicenceRule{}
	for _, rule := range p.LicencePolicy.Allow {
		if rule.Licence == "" {
			return fmt.Errorf("licence_policy.allow licence is required")
		}
		allowed[rule.Licence] = rule
	}
	if len(m.Components) == 0 {
		return fmt.Errorf("licence manifest must declare components")
	}
	for _, component := range m.Components {
		if component.Name == "" || component.Version == "" || component.Licence == "" || component.Distribution == "" {
			return fmt.Errorf("licence manifest component %q has missing identity fields", component.Name)
		}
		if denied[component.Licence] {
			return fmt.Errorf("component %q declares denied licence %q", component.Name, component.Licence)
		}
		rule, ok := allowed[component.Licence]
		if !ok {
			return fmt.Errorf("component %q declares unlisted licence %q", component.Name, component.Licence)
		}
		if rule.Distribution != "" && rule.Distribution != component.Distribution {
			return fmt.Errorf("component %q distribution %q is invalid for licence %q", component.Name, component.Distribution, component.Licence)
		}
	}

	seenExceptions := map[string]bool{}
	for _, exception := range p.Exceptions {
		if exception.ID == "" || exception.RuleID == "" || exception.PathGlob == "" || exception.Reason == "" || exception.Owner == "" || exception.ExpiresOn == "" {
			return fmt.Errorf("exception %q has missing fields", exception.ID)
		}
		if seenExceptions[exception.ID] {
			return fmt.Errorf("duplicate exception id %q", exception.ID)
		}
		seenExceptions[exception.ID] = true
		expiry, err := time.Parse("2006-01-02", exception.ExpiresOn)
		if err != nil {
			return fmt.Errorf("exception %q has invalid expires_on %q", exception.ID, exception.ExpiresOn)
		}
		today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		if expiry.Before(today) {
			return fmt.Errorf("exception %q expired on %s", exception.ID, exception.ExpiresOn)
		}
	}
	return nil
}

func matchGlob(glob, filePath string) bool {
	glob = strings.ReplaceAll(glob, `\`, "/")
	filePath = strings.ReplaceAll(filePath, `\`, "/")
	trimmed := strings.TrimSuffix(glob, "/")
	if trimmed != "" && (filePath == trimmed || strings.HasPrefix(filePath, trimmed+"/")) {
		return true
	}
	matched, err := path.Match(glob, filePath)
	return err == nil && matched
}

func isInPaths(rel string, paths []string) bool {
	for _, candidate := range paths {
		if matchGlob(candidate, rel) {
			return true
		}
	}
	return false
}

func exceptionApplies(p *Policy, ruleID, rel string, now time.Time) bool {
	today := now.UTC().Format("2006-01-02")
	for _, exception := range p.Exceptions {
		if exception.RuleID != ruleID || exception.ExpiresOn < today {
			continue
		}
		if matchGlob(exception.PathGlob, rel) {
			return true
		}
	}
	return false
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
cd tools/cleanroomcheck && gofmt -w . && go vet ./... && go test ./...
```

Expected: PASS.

- [ ] **Step 6: Write the committed policy manifests**

Create `docs/compliance/cleanroom-policy.json`:

```json
{
  "version": 1,
  "root": "../..",
  "source_basis": {
    "allowed": [
      {
        "id": "S1",
        "status": "partially retrieved",
        "locations": ["docs/master-plan.md"]
      },
      {
        "id": "S2",
        "status": "unavailable",
        "locations": ["docs/master-plan.md"]
      },
      {
        "id": "S3",
        "status": "fully retrieved",
        "locations": ["docs/master-plan.md"]
      },
      {
        "id": "S4",
        "status": "fully retrieved",
        "locations": ["docs/master-plan.md"]
      }
    ]
  },
  "exclude_paths": [
    ".git/",
    ".worktrees/",
    "artifacts/"
  ],
  "forbidden_artifact_patterns": [
    { "pattern": "vendor/astpp*", "severity": "high" },
    { "pattern": "third_party/astpp*", "severity": "high" },
    { "pattern": "*astpp*.sql", "severity": "high" },
    { "pattern": "*astpp*.tar.gz", "severity": "high" },
    { "pattern": "*astpp*.zip", "severity": "high" },
    { "pattern": "*astpp*.patch", "severity": "high" },
    { "pattern": "astpp/COPYING*", "severity": "high" },
    { "pattern": "astpp/LICENSE*", "severity": "high" }
  ],
  "forbidden_terms": [
    {
      "term": "ASTPP",
      "severity": "high",
      "allow_paths": ["AGENTS.md", "CONTRIBUTING.md", "docs/", "tools/cleanroomcheck/"]
    },
    {
      "term": "astppbilling.org",
      "severity": "high",
      "allow_paths": ["docs/master-plan.md", "tools/cleanroomcheck/"]
    },
    {
      "term": "vendor_astpp",
      "severity": "high",
      "allow_paths": ["docs/", "tools/cleanroomcheck/"]
    },
    {
      "term": "astpp.sql",
      "severity": "high",
      "allow_paths": ["docs/", "tools/cleanroomcheck/"]
    }
  ],
  "claim_rules": [
    { "path_glob": "docs/master-plan.md" },
    { "path_glob": "docs/compliance/cleanroom-audit.md" },
    { "path_glob": "docs/superpowers/specs/2026-11-20-clean-room-audit-design.md" },
    { "path_glob": "docs/superpowers/plans/2026-11-20-clean-room-audit.md" }
  ],
  "licence_policy": {
    "manifest": "licence-manifest.json",
    "allow": [
      { "licence": "MIT, BSD-3", "notes": "master-plan section 4 Python/FastAPI/Pydantic row" },
      { "licence": "Apache-2.0", "notes": "master-plan section 4" },
      { "licence": "BSD-3", "notes": "master-plan section 4" },
      { "licence": "GPLv2 or later, with link exceptions", "notes": "master-plan section 4 Kamailio" },
      { "licence": "GPLv2 with OpenSSL link exception", "notes": "master-plan section 4 OpenSIPS" },
      { "licence": "MPL-1.1", "notes": "master-plan section 4 FreeSWITCH" },
      { "licence": "GPL-3.0", "notes": "master-plan section 4 rtpengine (separate binary) and Ansible (tool)" },
      { "licence": "PostgreSQL Licence", "notes": "master-plan section 4" },
      { "licence": "AGPL-3.0", "distribution": "server-side", "notes": "master-plan section 4 Grafana" },
      { "licence": "MIT", "notes": "master-plan section 4 frontend libraries" },
      { "licence": "MPL-2.0", "notes": "master-plan section 4 OpenTofu" }
    ],
    "deny": ["BUSL", "RSALv2", "SSPL", "proprietary", "unknown"]
  },
  "exceptions": []
}
```

Create `docs/compliance/licence-manifest.json`:

```json
{
  "components": [
    { "name": "Python 3.12, FastAPI, Pydantic", "version": "3.12 / master-plan", "licence": "MIT, BSD-3", "distribution": "mixed" },
    { "name": "Go", "version": "1.24", "licence": "BSD-3", "distribution": "toolchain" },
    { "name": "Kamailio", "version": "5.8", "licence": "GPLv2 or later, with link exceptions", "distribution": "separate-binary" },
    { "name": "OpenSIPS", "version": "3.6", "licence": "GPLv2 with OpenSSL link exception", "distribution": "separate-binary" },
    { "name": "FreeSWITCH", "version": "master-plan", "licence": "MPL-1.1", "distribution": "separate-binary" },
    { "name": "rtpengine", "version": "master-plan", "licence": "GPL-3.0", "distribution": "separate-binary" },
    { "name": "PostgreSQL", "version": "17", "licence": "PostgreSQL Licence", "distribution": "container" },
    { "name": "Valkey", "version": "8", "licence": "BSD-3", "distribution": "container" },
    { "name": "Apache Kafka", "version": "master-plan", "licence": "Apache-2.0", "distribution": "container" },
    { "name": "ClickHouse", "version": "master-plan", "licence": "Apache-2.0", "distribution": "container" },
    { "name": "Keycloak", "version": "master-plan", "licence": "Apache-2.0", "distribution": "container" },
    { "name": "APISIX", "version": "master-plan", "licence": "Apache-2.0", "distribution": "container" },
    { "name": "OpenTelemetry", "version": "master-plan", "licence": "Apache-2.0", "distribution": "library" },
    { "name": "Prometheus", "version": "master-plan", "licence": "Apache-2.0", "distribution": "container" },
    { "name": "Loki", "version": "master-plan", "licence": "Apache-2.0", "distribution": "container" },
    { "name": "Grafana", "version": "master-plan", "licence": "AGPL-3.0", "distribution": "server-side" },
    { "name": "Kubernetes", "version": "master-plan", "licence": "Apache-2.0", "distribution": "platform" },
    { "name": "gVisor", "version": "master-plan", "licence": "Apache-2.0", "distribution": "runtime" },
    { "name": "OpenTofu", "version": "master-plan", "licence": "MPL-2.0", "distribution": "tool" },
    { "name": "Ansible", "version": "master-plan", "licence": "GPL-3.0", "distribution": "tool" },
    { "name": "React", "version": "master-plan", "licence": "MIT", "distribution": "frontend" },
    { "name": "SvelteKit", "version": "master-plan", "licence": "MIT", "distribution": "frontend" }
  ]
}
```

- [ ] **Step 7: Commit Task 1**

```bash
git add docs/compliance/cleanroom-policy.json docs/compliance/licence-manifest.json tools/cleanroomcheck/go.mod tools/cleanroomcheck/policy.go tools/cleanroomcheck/policy_test.go
git commit -m "feat: add clean-room policy loader"
```

---

### Task 2: Repository scanner

**Files:**
- Create: `tools/cleanroomcheck/scan.go`
- Test: `tools/cleanroomcheck/scan_test.go`

**Interfaces:**
- Consumes: `Policy`, `matchGlob`, `isInPaths` from Task 1.
- Produces:
  - `type scannedFile struct`
  - `func scanRepository(root string, p *Policy) ([]scannedFile, error)`

- [ ] **Step 1: Write the failing scanner tests**

Create `tools/cleanroomcheck/scan_test.go`:

```go
package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanRepositorySkipsExcludedPaths(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{"a.txt", ".git/config", "artifacts/cleanroom/report.txt"} {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("clean\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	p := validPolicy()
	files, err := scanRepository(root, p)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "a.txt" {
		t.Fatalf("expected only a.txt, got %#v", files)
	}
	if len(files[0].Lines) != 2 {
		t.Fatalf("expected trailing empty line split, got %#v", files[0].Lines)
	}
}

func TestScanRepositoryFailsOnNonUTF8(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "bad.bin"), []byte{0xff, 0xfe}, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := scanRepository(root, validPolicy())
	if err == nil {
		t.Fatal("expected non-UTF-8 error")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd tools/cleanroomcheck && go test ./...
```

Expected: FAIL because `scanRepository` is undefined.

- [ ] **Step 3: Write the scanner implementation**

Create `tools/cleanroomcheck/scan.go`:

```go
package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

type scannedFile struct {
	Path  string
	Lines []string
}

func scanRepository(root string, p *Policy) ([]scannedFile, error) {
	var files []scannedFile
	err := filepath.WalkDir(root, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if isInPaths(rel, p.ExcludePaths) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(current)
		if err != nil {
			return fmt.Errorf("read %s: %w", rel, err)
		}
		if !utf8.Valid(data) {
			return fmt.Errorf("%s: unsupported non-UTF-8 file", rel)
		}
		files = append(files, scannedFile{
			Path:  rel,
			Lines: strings.Split(string(data), "\n"),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	return files, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd tools/cleanroomcheck && gofmt -w . && go vet ./... && go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

```bash
git add tools/cleanroomcheck/scan.go tools/cleanroomcheck/scan_test.go
git commit -m "feat: add clean-room repository scanner"
```

---

### Task 3: Findings rules and audit orchestration

**Files:**
- Create: `tools/cleanroomcheck/rules.go`
- Test: `tools/cleanroomcheck/rules_test.go`

**Interfaces:**
- Consumes: `Policy`, `LicenceManifest`, `scannedFile`, `loadPolicy`, `scanRepository`, `isInPaths`, `matchGlob`, `exceptionApplies`.
- Produces:
  - `type Finding struct`
  - `func checkArtifacts(files []scannedFile, p *Policy, now time.Time) []Finding`
  - `func checkTerms(files []scannedFile, p *Policy, now time.Time) []Finding`
  - `func checkClaims(files []scannedFile, p *Policy) []Finding`
  - `func checkLicences(m *LicenceManifest, p *Policy) []Finding`
  - `func sortFindings(findings []Finding)`
  - `func runAudit(policyPath string, now time.Time) ([]Finding, error)`

- [ ] **Step 1: Write the failing rule tests**

Create `tools/cleanroomcheck/rules_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd tools/cleanroomcheck && go test ./...
```

Expected: FAIL because rule functions are undefined.

- [ ] **Step 3: Write the rules implementation**

Create `tools/cleanroomcheck/rules.go`:

```go
package main

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
)

type Finding struct {
	Severity    string `json:"severity"`
	RuleID      string `json:"rule_id"`
	Path        string `json:"path"`
	Line        int    `json:"line"`
	Snippet     string `json:"snippet"`
	Remediation string `json:"remediation"`
}

func checkArtifacts(files []scannedFile, p *Policy, now time.Time) []Finding {
	var findings []Finding
	for _, file := range files {
		for _, artifact := range p.ForbiddenArtifacts {
			if !matchArtifactPattern(artifact.Pattern, file.Path) {
				continue
			}
			if exceptionApplies(p, "artifact.forbidden", file.Path, now) {
				continue
			}
			findings = append(findings, Finding{
				Severity:    artifact.Severity,
				RuleID:      "artifact.forbidden",
				Path:        file.Path,
				Line:        0,
				Snippet:     file.Path,
				Remediation: "Remove the vendor artifact or add a reviewed exception to docs/compliance/cleanroom-policy.json",
			})
		}
	}
	return findings
}

func checkTerms(files []scannedFile, p *Policy, now time.Time) []Finding {
	var findings []Finding
	for _, file := range files {
		for _, term := range p.ForbiddenTerms {
			if isInPaths(file.Path, term.AllowPaths) || exceptionApplies(p, "term.forbidden", file.Path, now) {
				continue
			}
			for index, line := range file.Lines {
				if !strings.Contains(line, term.Term) {
					continue
				}
				findings = append(findings, Finding{
					Severity:    term.Severity,
					RuleID:      "term.forbidden",
					Path:        file.Path,
					Line:        index + 1,
					Snippet:     makeSnippet(line),
					Remediation: "Remove the proprietary reference or add an allow path in docs/compliance/cleanroom-policy.json",
				})
			}
		}
	}
	return findings
}

func checkClaims(files []scannedFile, p *Policy) []Finding {
	var findings []Finding
	sourceIDs := make([]string, 0, len(p.SourceBasis.Allowed))
	for _, source := range p.SourceBasis.Allowed {
		sourceIDs = append(sourceIDs, source.ID)
	}

	for _, rule := range p.ClaimRules {
		for _, file := range files {
			if !matchGlob(rule.PathGlob, file.Path) {
				continue
			}
			body := strings.Join(file.Lines, "\n")
			if hasSourceDeclaration(file, body) && hasSourceID(body, sourceIDs) {
				continue
			}
			findings = append(findings, Finding{
				Severity:    "high",
				RuleID:      "claim.source_basis",
				Path:        file.Path,
				Line:        1,
				Snippet:     "missing source-basis declaration or S1-S4 marker",
				Remediation: "Add a source-basis declaration with S1-S4 or update the claim rule in docs/compliance/cleanroom-policy.json",
			})
		}
	}
	return findings
}

func checkLicences(m *LicenceManifest, p *Policy) []Finding {
	var findings []Finding
	denied := map[string]bool{}
	for _, licence := range p.LicencePolicy.Deny {
		denied[licence] = true
	}
	allowed := map[string]LicenceRule{}
	for _, rule := range p.LicencePolicy.Allow {
		allowed[rule.Licence] = rule
	}

	for _, component := range m.Components {
		if denied[component.Licence] {
			findings = append(findings, Finding{
				Severity:    "critical",
				RuleID:      "licence.denied",
				Path:        "docs/compliance/licence-manifest.json",
				Line:        0,
				Snippet:     fmt.Sprintf("%s %s: %s", component.Name, component.Version, component.Licence),
				Remediation: "Remove the component or replace it with an approved FOSS licence",
			})
			continue
		}
		rule, ok := allowed[component.Licence]
		if !ok {
			findings = append(findings, Finding{
				Severity:    "high",
				RuleID:      "licence.not_allowed",
				Path:        "docs/compliance/licence-manifest.json",
				Line:        0,
				Snippet:     fmt.Sprintf("%s %s: %s", component.Name, component.Version, component.Licence),
				Remediation: "Add the licence to docs/master-plan.md section 4 and the allowlist",
			})
			continue
		}
		if rule.Distribution != "" && rule.Distribution != component.Distribution {
			findings = append(findings, Finding{
				Severity:    "high",
				RuleID:      "licence.not_allowed",
				Path:        "docs/compliance/licence-manifest.json",
				Line:        0,
				Snippet:     fmt.Sprintf("%s %s: %s on %s", component.Name, component.Version, component.Licence, component.Distribution),
				Remediation: "Use the approved distribution context or update the policy allowlist",
			})
		}
	}
	return findings
}

func runAudit(policyPath string, now time.Time) ([]Finding, error) {
	p, manifest, root, err := loadPolicy(policyPath, now)
	if err != nil {
		return nil, err
	}
	files, err := scanRepository(root, p)
	if err != nil {
		return nil, err
	}

	var findings []Finding
	findings = append(findings, checkArtifacts(files, p, now)...)
	findings = append(findings, checkTerms(files, p, now)...)
	findings = append(findings, checkClaims(files, p)...)
	findings = append(findings, checkLicences(manifest, p)...)
	sortFindings(findings)
	return findings, nil
}

func sortFindings(findings []Finding) {
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].Path != findings[j].Path {
			return findings[i].Path < findings[j].Path
		}
		if findings[i].Line != findings[j].Line {
			return findings[i].Line < findings[j].Line
		}
		if findings[i].RuleID != findings[j].RuleID {
			return findings[i].RuleID < findings[j].RuleID
		}
		return findings[i].Severity < findings[j].Severity
	})
}

func matchArtifactPattern(pattern, filePath string) bool {
	if matchGlob(pattern, filePath) {
		return true
	}
	if strings.HasSuffix(pattern, "*") && strings.HasPrefix(filePath, strings.TrimSuffix(pattern, "*")) {
		return true
	}
	matched, err := path.Match(pattern, path.Base(filePath))
	return err == nil && matched
}

func makeSnippet(line string) string {
	line = strings.TrimSpace(line)
	runes := []rune(line)
	if len(runes) > 160 {
		return string(runes[:157]) + "..."
	}
	return line
}

func hasSourceDeclaration(file scannedFile, body string) bool {
	if strings.Contains(body, "Source basis:") {
		return true
	}
	if strings.Contains(body, "Feature claims come only from the four ASTPP marketing pages cited in") {
		return true
	}
	return file.Path == "docs/master-plan.md" && strings.Contains(body, "## 0. Source status and verification basis")
}

func hasSourceID(body string, sourceIDs []string) bool {
	for _, id := range sourceIDs {
		if strings.Contains(body, id) {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd tools/cleanroomcheck && gofmt -w . && go vet ./... && go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit Task 3**

```bash
git add tools/cleanroomcheck/rules.go tools/cleanroomcheck/rules_test.go
git commit -m "feat: add clean-room audit rules"
```

---

### Task 4: Reports, CLI, Make, and CI

**Files:**
- Create: `tools/cleanroomcheck/report.go`
- Create: `tools/cleanroomcheck/main.go`
- Test: `tools/cleanroomcheck/main_test.go`
- Modify: `Makefile`
- Modify: `.github/workflows/ci.yml`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: `runAudit`, `Finding` from Task 3.
- Produces:
  - `type runResult struct`
  - `func execute(policyPath, reportDir, format string, now time.Time) (runResult, error)`
  - `func formatFindings(format string, findings []Finding) (string, error)`
  - `func writeReports(dir string, findings []Finding) error`

- [ ] **Step 1: Write the failing CLI tests**

Create `tools/cleanroomcheck/main_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd tools/cleanroomcheck && go test ./...
```

Expected: FAIL because CLI functions are undefined.

- [ ] **Step 3: Write the report implementation**

Create `tools/cleanroomcheck/report.go`:

```go
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
```

- [ ] **Step 4: Write the CLI implementation**

Create `tools/cleanroomcheck/main.go`:

```go
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
```

- [ ] **Step 5: Run CLI tests**

Run:

```bash
cd tools/cleanroomcheck && gofmt -w . && go vet ./... && go test ./...
```

Expected: PASS.

- [ ] **Step 6: Wire Make**

Modify `Makefile`:

```make
.PHONY: up down ps smoke logs test protoc compliance cleanroom up-gvisor down-gvisor check-gvisor smoke-gvisor
```

Add this target after `compliance`:

```make
cleanroom:
	cd tools/cleanroomcheck && go vet ./... && go test ./... && go run . -policy ../../docs/compliance/cleanroom-policy.json -report ../../artifacts/cleanroom
```

Change:

```make
test: compliance
```

to:

```make
test: compliance cleanroom
```

- [ ] **Step 7: Wire CI**

Modify `.github/workflows/ci.yml` by adding these steps after `actions/setup-go@v5`:

```yaml
      - name: Clean-room audit
        run: make cleanroom
      - name: Upload clean-room report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: cleanroom-report
          path: artifacts/cleanroom/
```

The resulting file must be:

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:

jobs:
  smoke:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.27"
      - name: Clean-room audit
        run: make cleanroom
      - name: Upload clean-room report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: cleanroom-report
          path: artifacts/cleanroom/
      - name: Start data layer
        run: docker compose up -d --wait
      - name: Go tests
        run: make test
      - name: Smoke test
        run: bash ops/smoke.sh
```

- [ ] **Step 8: Ignore reports**

Append to `.gitignore`:

```gitignore
artifacts/
```

- [ ] **Step 9: Verify the gate**

Run:

```bash
make cleanroom
```

Expected: PASS with `ok: clean-room audit 0 findings`.

- [ ] **Step 10: Commit Task 4**

```bash
git add tools/cleanroomcheck/report.go tools/cleanroomcheck/main.go tools/cleanroomcheck/main_test.go Makefile .github/workflows/ci.yml .gitignore
git commit -m "feat: make clean-room audit blocking"
```

---

### Task 5: Governance process pack

**Files:**
- Create: `CONTRIBUTING.md`
- Create: `CODE_OF_CONDUCT.md`
- Create: `GOVERNANCE.md`
- Create: `SECURITY.md`
- Create: `docs/governance/rfc-template.md`
- Create: `docs/governance/release-policy.md`
- Create: `docs/governance/maintainer-onboarding.md`
- Create: `docs/governance/cleanroom-checklist.md`
- Create: `.github/ISSUE_TEMPLATE/bug_report.yml`
- Create: `.github/ISSUE_TEMPLATE/feature_request.yml`
- Create: `.github/pull_request_template.md`

**Interfaces:**
- Consumes: clean-room gate commands from Task 4.
- Produces: contributor-facing process documentation and PR issue templates.

- [ ] **Step 1: Write contribution guide**

Create `CONTRIBUTING.md`:

```md
# Contributing

Thanks for helping build Omni-Foní.

## Ground rules

- This is a clean-room project. Never read, copy, or summarize ASTPP source code, schemas, or licensed assets.
- Feature claims come only from the four marketing pages cited in `docs/master-plan.md` sections 0 and 8.
- Keep all dependencies FOSS and compatible with `docs/master-plan.md` section 4.
- Do not introduce Terraform, Redis, Helm charts, ingress, TLS, or cloud IaC without an RFC and roadmap update.

## Local setup

- Go is installed at `/usr/local/go` and symlinked to `/usr/local/bin/go`.
- Docker Compose is the dev data layer.
- Start the stack with `make up`.
- Run the full test suite with `COMPOSE_PROJECT_NAME=omni-fon make test`.
- Run the clean-room gate with `make cleanroom`.
- Regenerate gRPC stubs with `make protoc` when a `.proto` changes; `protoc` and plugins live in `/root/go/bin`.

## Workflow

1. Open an issue before large work.
2. Use an RFC from `docs/governance/rfc-template.md` for cross-cutting design changes.
3. Work on a focused branch or worktree.
4. Add or update tests for each behavior change.
5. Run `make cleanroom` and `COMPOSE_PROJECT_NAME=omni-fon make test`.
6. Open a PR with the PR template filled in.

## Review expectations

- Every PR needs one reviewer who did not write the change.
- Security, licence, clean-room, schema, proto, and deployment changes need a maintainer review.
- A green CI run is required before merge.
- Reviewers may request a clean-room checklist entry in `docs/governance/cleanroom-checklist.md` for any new external material.

## Commits

Use short imperative commit messages such as `feat: add rating cache` or `docs: clarify reporting policy`. Do not include secrets, tokens, customer data, or proprietary material.
```

- [ ] **Step 2: Write code of conduct**

Create `CODE_OF_CONDUCT.md`:

```md
# Code of Conduct

## Our standard

We are committed to a welcoming, harassment-free project for contributors and maintainers. Harassment, personal attacks, discriminatory language, publishing someone else's private information, and sustained disruption are not acceptable.

## Expected behavior

- Be respectful and assume good intent.
- Give actionable feedback about the work, not the person.
- Accept constructive criticism gracefully.
- Respect different experience levels, time zones, and communication styles.
- Keep discussions focused on the project.

## Unacceptable behavior

- Sexualized language or imagery and unwelcome sexual attention.
- Trolling, insulting or derogatory comments, and personal or political attacks.
- Public or private harassment.
- Publishing others' private information without explicit permission.
- Other conduct that would be inappropriate in a professional setting.

## Reporting

Use GitHub private vulnerability reporting on this repository for conduct reports. If that channel is unavailable, ask any maintainer to open a private maintainer thread. Do not open a public issue for a conduct report.

Maintainers will review reports privately, keep reporter identity confidential where possible, and may issue a private warning, temporary ban, or permanent ban. Maintainers who do not follow these rules may be removed by the other maintainers.

## Scope

This code applies in repository issues, pull requests, discussions, reviews, and any project space where a person represents the project.
```

- [ ] **Step 3: Write governance model**

Create `GOVERNANCE.md`:

```md
# Governance

## Roles

- Contributors open issues, propose changes, review work, and may become maintainers by contribution.
- Maintainers merge approved changes, manage releases, enforce the code of conduct, and maintain the roadmap.
- Reviewers are contributors or maintainers who review a specific change; at least one reviewer must be independent of the author.

## Decision making

Most decisions use lazy consensus: propose in an issue or RFC, allow at least seven full days for objections, then proceed if there is no unresolved objection. Cross-cutting design, licence, clean-room, schema, or deployment decisions require an RFC and explicit maintainer approval.

If consensus fails, maintainers decide by simple majority. A maintainer who authored the change does not vote on their own objection. Decisions are recorded in the issue, RFC, or PR that introduced them.

## RFC process

1. Copy `docs/governance/rfc-template.md` into `docs/superpowers/specs/` with a dated name.
2. Link the RFC from an issue.
3. Solve the problem statement, constraints, options, and verification plan.
4. Leave the RFC open for at least seven days.
5. Merge when approved by at least two maintainers.

## Releases

Releases follow `docs/governance/release-policy.md`. Only maintainers create tags and publish release notes.

## Conduct enforcement

Code of conduct reports are handled privately as described in `CODE_OF_CONDUCT.md`. Maintainers may act immediately when safety or confidentiality is at risk.

## Changing governance

Changes to this file require an RFC and approval by at least two maintainers.
```

- [ ] **Step 4: Write security policy**

Create `SECURITY.md`:

```md
# Security Policy

## Reporting a vulnerability

Use GitHub private vulnerability reporting on this repository. Do not open a public issue for a suspected vulnerability.

Include:

- affected component or commit;
- reproduction steps or proof of concept;
- impact;
- suggested mitigation, if known.

## Response expectations

Maintainers will acknowledge private reports within seven days, confirm or reject the report with reasoning, and coordinate a fix or disclosure timeline. Critical reports affecting money, authentication, or tenant isolation are prioritized.

## Supported versions

The latest tagged release and the `main` branch receive security fixes. Older releases are best effort.

## Safe harbors

Researchers acting in good faith, avoiding production data, and following this reporting path will not be pursued by the project for responsible disclosure.
```

- [ ] **Step 5: Write RFC template**

Create `docs/governance/rfc-template.md`:

```md
# RFC: Short title

Date: YYYY-MM-DD
Status: proposed
Owner: GitHub handle
Source basis: S1, S2, S3, S4

## Problem

What problem is being solved and who has it?

## Constraints

List technical, licence, clean-room, operational, and timeline constraints.

## Options

Describe at least two viable options.

## Recommendation

Choose one option and explain why.

## Verification

State the commands, tests, and gates that prove the change works.

## Open questions

List unresolved questions and who must answer them.
```

- [ ] **Step 6: Write release policy**

Create `docs/governance/release-policy.md`:

```md
# Release Policy

## Versioning

Releases use semantic versioning. Breaking API, proto, schema, or deployment changes require a minor version bump before 1.0 and a major bump after 1.0.

## Preconditions

A release may be tagged only when:

- `make cleanroom` passes;
- `COMPOSE_PROJECT_NAME=omni-fon make test` passes;
- `bash ops/smoke.sh` passes on a clean stack;
- release notes list changes, migrations, and new environment variables;
- any manual gate that cannot run in CI is recorded as unrun, not passed.

## Tagging and publication

Only maintainers create tags. Release notes must name the commit range and include upgrade instructions for schema or deployment changes.

## Rollback

If a release breaks a release gate, maintainers may delete the tag before downstream adoption. After adoption, publish a patch release rather than moving the tag.
```

- [ ] **Step 7: Write maintainer onboarding**

Create `docs/governance/maintainer-onboarding.md`:

```md
# Maintainer Onboarding

## Prerequisites

A maintainer candidate should have a history of constructive reviews, at least one merged cross-cutting change, and familiarity with the master plan, clean-room rules, and release policy.

## Appointment

A current maintainer opens an issue with the candidate, rationale, and scope. After seven days and approval by at least two maintainers, add the candidate to repository maintainer settings and link the issue here.

## First-month checklist

- Review at least one PR per service area.
- Run `make cleanroom` and `COMPOSE_PROJECT_NAME=omni-fon make test`.
- Read `docs/master-plan.md` sections 0, 4, and 8.
- Review `docs/compliance/` and `docs/security/`.
- Triage open issues and label stale ones.

## Ongoing duties

Maintainers review, merge, release, handle private reports, enforce the code of conduct, and keep the roadmap aligned with implementation.
```

- [ ] **Step 8: Write clean-room checklist**

Create `docs/governance/cleanroom-checklist.md`:

```md
# Clean-room review checklist

Use this checklist for any PR that adds documentation, dependencies, external material, or feature descriptions.

- [ ] No ASTPP source, schema, asset, archive, or licence file was read or copied.
- [ ] Feature claims cite S1, S2, S3, or S4 from `docs/master-plan.md`.
- [ ] No protocol, schema, API shape, or implementation detail was inferred from proprietary material.
- [ ] New dependencies are FOSS and recorded in `docs/compliance/licence-manifest.json`.
- [ ] Licence changes are reflected in `docs/master-plan.md` section 4.
- [ ] Any new forbidden term or path exception has an owner, reason, and expiry.
- [ ] `make cleanroom` passes locally.
- [ ] CI clean-room report was reviewed when findings appear.
```

- [ ] **Step 9: Write issue templates**

Create `.github/ISSUE_TEMPLATE/bug_report.yml`:

```yaml
name: Bug report
description: Report reproducible incorrect behavior
title: "fix: "
labels: [bug]
body:
  - type: textarea
    id: summary
    attributes:
      label: Summary
      description: What happened?
    validations:
      required: true
  - type: textarea
    id: reproduce
    attributes:
      label: Reproduction
      description: Exact commands and observed output.
    validations:
      required: true
  - type: textarea
    id: expected
    attributes:
      label: Expected behavior
    validations:
      required: true
  - type: input
    id: commit
    attributes:
      label: Commit or version
    validations:
      required: true
```

Create `.github/ISSUE_TEMPLATE/feature_request.yml`:

```yaml
name: Feature request
description: Propose new behavior
title: "feat: "
labels: [enhancement]
body:
  - type: textarea
    id: problem
    attributes:
      label: Problem
      description: Who has this problem and why it matters.
    validations:
      required: true
  - type: textarea
    id: proposal
    attributes:
      label: Proposal
    validations:
      required: true
  - type: textarea
    id: source_basis
    attributes:
      label: Source basis
      description: Cite S1, S2, S3, or S4 for any claimed upstream feature.
    validations:
      required: false
  - type: textarea
    id: alternatives
    attributes:
      label: Alternatives
    validations:
      required: false
```

- [ ] **Step 10: Write PR template**

Create `.github/pull_request_template.md`:

```md
## Summary

What changed and why?

## Verification

- [ ] `make cleanroom`
- [ ] `COMPOSE_PROJECT_NAME=omni-fon make test`
- [ ] Focused tests for changed behavior
- [ ] Manual gates listed as unrun if not executed

## Clean-room

- [ ] No proprietary source, schema, asset, or licensed text was read or copied.
- [ ] Feature claims cite S1-S4.
- [ ] Licence manifest and master-plan section 4 updated if dependencies changed.

## Impact

List schema, proto, deployment, environment, or API changes. Write `none` if absent.
```

- [ ] **Step 11: Run the gate and commit Task 5**

Run:

```bash
make cleanroom
```

Expected: PASS.

Commit:

```bash
git add CONTRIBUTING.md CODE_OF_CONDUCT.md GOVERNANCE.md SECURITY.md docs/governance .github/ISSUE_TEMPLATE .github/pull_request_template.md
git commit -m "docs: add community governance pack"
```

---

### Task 6: Audit guide, status, and full verification

**Files:**
- Create: `docs/compliance/cleanroom-audit.md`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: all previous tasks.
- Produces: operator documentation and final green branch.

- [ ] **Step 1: Write the audit guide**

Create `docs/compliance/cleanroom-audit.md`:

```md
# Clean-room audit

Source basis: S1, S2, S3, S4

Feature claims come only from the four ASTPP marketing pages cited in `docs/master-plan.md` sections 0 and 8. No proprietary source, schema, or licensed asset was consulted.

## Run

From the repository root:

```bash
make cleanroom
```

The command runs `go vet`, unit tests, and the checker. It exits `0` only with zero findings.

For direct use:

```bash
cd tools/cleanroomcheck
go run . -policy ../../docs/compliance/cleanroom-policy.json -report ../../artifacts/cleanroom
go run . -policy ../../docs/compliance/cleanroom-policy.json -format json
```

## Policy

- `docs/compliance/cleanroom-policy.json` defines exclusions, forbidden artifacts, forbidden terms, claim-bearing docs, licence rules, and exceptions.
- `docs/compliance/licence-manifest.json` declares components and licences from `docs/master-plan.md` section 4.
- Reports are written to `artifacts/cleanroom/` when `-report` is used. Reports are ignored and are not committed.

## Findings

Each finding names severity, rule ID, path, line, snippet, and remediation. Any finding fails CI. Do not downgrade a finding to a warning.

Rule IDs:

- `artifact.forbidden` — a vendor artifact path matched the policy.
- `term.forbidden` — a proprietary marker appeared outside an allow path.
- `claim.source_basis` — a claim-bearing doc lacks a source-basis declaration or S1-S4 marker.
- `licence.denied` — a declared licence is explicitly denied.
- `licence.not_allowed` — a licence is unlisted or used in the wrong distribution context.

## Exceptions

Exceptions require an ID, rule ID, path glob, reason, owner, and expiry date (`YYYY-MM-DD`). Expired or incomplete exceptions fail closed. Add exceptions only after review, keep them narrow, and remove them with the material they cover.

## Scope

The checker enforces repository policy over text files. It does not verify upstream licence files, detect semantic paraphrase, generate SBOMs, read proprietary material, access the network, or prove runtime behavior.
```

- [ ] **Step 2: Update status**

Modify `AGENTS.md` by inserting this line after the P6 slice three bullet:

```md
- P7 slice two: `tools/cleanroomcheck` is a stdlib-only Go clean-room audit (`make cleanroom`, blocking in CI and a prerequisite of `make test`) driven by `docs/compliance/cleanroom-policy.json` and `docs/compliance/licence-manifest.json`; it checks forbidden artifacts/terms, source-basis declarations, and declared licences, emits text/JSON reports under `artifacts/cleanroom/`, and fails closed on any finding. Governance pack: `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, `GOVERNANCE.md`, `SECURITY.md`, and templates/policies under `docs/governance/` and `.github/`. GitHub private vulnerability reporting is the reporting channel. The P7 install timing gate remains manual.
```

- [ ] **Step 3: Verify formatting and focused checks**

Run:

```bash
cd tools/cleanroomcheck && gofmt -l . && go vet ./... && go test ./...
```

Expected: `gofmt -l` prints no paths; vet and tests pass.

- [ ] **Step 4: Verify the blocking gate**

Run:

```bash
make cleanroom
```

Expected: PASS with `ok: clean-room audit 0 findings`.

- [ ] **Step 5: Verify the full suite**

Run:

```bash
COMPOSE_PROJECT_NAME=omni-fon make test
```

Expected: PASS.

- [ ] **Step 6: Verify patch hygiene**

Run:

```bash
git diff --check
git status --short
```

Expected: no whitespace errors; only intended files changed.

- [ ] **Step 7: Commit Task 6**

```bash
git add docs/compliance/cleanroom-audit.md AGENTS.md
git commit -m "docs: document clean-room audit"
```

---

## Final verification

After all tasks:

```bash
cd tools/cleanroomcheck && gofmt -l . && go vet ./... && go test ./...
make cleanroom
COMPOSE_PROJECT_NAME=omni-fon make test
git diff --check
git status --short
```

Expected: all commands pass, no report files are tracked, and `git status --short` is empty.
