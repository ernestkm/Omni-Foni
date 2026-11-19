# POPIA Control Pack Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a POPIA documentation pack, a machine-readable data-classification manifest, and a CI validation gate without changing runtime service behaviour.

**Architecture:** A standalone stdlib-only Go CLI in `tools/compliancecheck/` validates `docs/compliance/data-classification.json` against a fixed schema, retention rules, storage allowlist, evidence paths, and control/class coverage. Docs under `docs/compliance/` provide the control matrix and operational runbooks, and the root `Makefile` runs the validator before `make test`.

**Tech Stack:** Go 1.22+, standard library only (`encoding/json`, `os`, `path/filepath`, `sort`, `strings`), GNU Make, Markdown.

## Global Constraints

- Clean-room: never read ASTPP source code, schemas, or licensed assets. Feature claims come only from the four ASTPP marketing pages cited in `docs/master-plan.md` sections 0 and 8.
- Design controls come from `docs/master-plan.md` sections 5 and 6.
- No runtime service, database, Kafka topic, HTTP endpoint, or gRPC API is added.
- No runtime deletion jobs, KYC workflow, lawful-intercept hooks, residency enforcement, or operator UI is added.
- No code comments may be added; existing required `//go:embed` directives and comments already inside edited blocks are allowed.
- Generated protobufs must not be hand-edited; `make protoc` is the only regeneration path.
- Run all commands from `/root/Omni-Foní/.worktrees/p6-popia-pack` unless a step specifies a different directory.
- `make test` needs the dev stack up (`COMPOSE_PROJECT_NAME=omni-fon make up`); host tests need `127.0.0.1 kafka` in `/etc/hosts`.
- `docker compose ps --status healthy` returns no rows on this CLI; use `docker inspect --format '{{.State.Health.Status}}'` for health checks.
- Verification claims require actual command output; never claim a command passed without running it.
- `rg` is unavailable; use `grep`. `jq` is unavailable; use `python3`.

---

### Task 1: Compliance validator module and tests

**Files:**
- Create: `tools/compliancecheck/go.mod`
- Create: `tools/compliancecheck/main.go`
- Create: `tools/compliancecheck/main_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `manifest`, `control`, `dataClass`, `personalField`, `storageTarget`, `retention`, `loadManifest(path string) (manifest, error)`, `(manifest).validate(root string) error`, `storageKey(storageTarget) string`, `allowedStorageTargets map[string]bool`.

- [ ] **Step 1: Create the module**

Create `tools/compliancecheck/go.mod` with exactly:

```go
module omnifoni/compliancecheck

go 1.22
```

- [ ] **Step 2: Write the failing validator tests**

Create `tools/compliancecheck/main_test.go` with exactly:

```go
package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func baseManifest() manifest {
	days := 90
	return manifest{
		Version: 1,
		Controls: []control{{
			ID:        "popia-s10",
			Instrument: "POPIA s10",
			ClassIDs:  []string{"test"},
		}},
		Classes: []dataClass{{
			ID:          "test",
			Purpose:     "Test purpose.",
			LawfulBasis: "POPIA s11",
			PersonalData: []personalField{{
				Field:          "email",
				Classification: "contact",
				Purpose:        "Test contact purpose.",
			}},
			Storage: []storageTarget{{
				System:  "postgres",
				Database: "identity",
				Table:   "users",
				Columns: []string{"email"},
			}},
			Retention: retention{
				Kind:      "fixed_days",
				Days:      &days,
				Evidence:  []string{"evidence.txt"},
			},
			EnforcementStatus: "documented_target",
		}},
	}
}

func newTestManifest(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "evidence.txt"), "evidence")
	m := baseManifest()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "manifest.json")
	writeTestFile(t, path, string(data))
	return path, root
}

func TestLoadAndValidateManifest(t *testing.T) {
	path, root := newTestManifest(t)
	m, err := loadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.validate(root); err != nil {
		t.Fatal(err)
	}
}

func TestValidateManifestRejectsInvalidFixtures(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*manifest)
		want   string
	}{
		{
			name: "duplicate class id",
			mutate: func(m *manifest) {
				m.Classes = append(m.Classes, m.Classes[0])
			},
			want: "duplicate classes[].id",
		},
		{
			name: "missing purpose",
			mutate: func(m *manifest) {
				m.Classes[0].Purpose = ""
			},
			want: "classes[\"test\"].purpose is required",
		},
		{
			name: "invalid classification",
			mutate: func(m *manifest) {
				m.Classes[0].PersonalData[0].Classification = "biometric"
			},
			want: "invalid classification",
		},
		{
			name: "invalid retention kind",
			mutate: func(m *manifest) {
				m.Classes[0].Retention.Kind = "forever"
			},
			want: "invalid retention kind",
		},
		{
			name: "non-positive fixed days",
			mutate: func(m *manifest) {
				zero := 0
				m.Classes[0].Retention.Days = &zero
			},
			want: "retention.days must be greater than zero",
		},
		{
			name: "missing evidence path",
			mutate: func(m *manifest) {
				m.Classes[0].Retention.Evidence = []string{"missing.txt"}
			},
			want: "evidence path not found",
		},
		{
			name: "unknown storage target",
			mutate: func(m *manifest) {
				m.Classes[0].Storage[0].Table = "does_not_exist"
			},
			want: "unknown storage target",
		},
		{
			name: "dangling control reference",
			mutate: func(m *manifest) {
				m.Controls[0].ClassIDs = []string{"missing"}
			},
			want: "references unknown class",
		},
		{
			name: "missing counsel assumption",
			mutate: func(m *manifest) {
				m.Classes[0].Retention.CounselRequired = true
				m.Classes[0].Retention.Assumption = ""
			},
			want: "counsel assumption is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, root := newTestManifest(t)
			m, err := loadManifest(path)
			if err != nil {
				t.Fatal(err)
			}
			tt.mutate(&m)
			err = m.validate(root)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err, tt.want)
			}
		})
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run:

```bash
cd tools/compliancecheck && go test ./...
```

Expected: FAIL with `undefined: manifest`, `undefined: loadManifest`, and `undefined: storageKey`.

- [ ] **Step 4: Write the validator implementation**

Create `tools/compliancecheck/main.go` with exactly:

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type manifest struct {
	Version  int         `json:"version"`
	Controls []control   `json:"controls"`
	Classes  []dataClass `json:"classes"`
}

type control struct {
	ID         string   `json:"id"`
	Instrument string   `json:"instrument"`
	ClassIDs   []string `json:"class_ids"`
}

type dataClass struct {
	ID                string         `json:"id"`
	Purpose           string         `json:"purpose"`
	LawfulBasis       string         `json:"lawful_basis"`
	PersonalData      []personalField `json:"personal_data"`
	Storage           []storageTarget `json:"storage"`
	Retention         retention       `json:"retention"`
	EnforcementStatus string         `json:"enforcement_status"`
}

type personalField struct {
	Field          string `json:"field"`
	Classification string `json:"classification"`
	Purpose        string `json:"purpose"`
}

type storageTarget struct {
	System   string   `json:"system"`
	Database string   `json:"database"`
	Table    string   `json:"table"`
	Columns  []string `json:"columns"`
}

type retention struct {
	Kind            string   `json:"kind"`
	Days            *int     `json:"days,omitempty"`
	Evidence        []string `json:"evidence"`
	CounselRequired bool     `json:"counsel_required"`
	Assumption      string   `json:"assumption"`
}

var allowedClassifications = map[string]bool{
	"identifier": true,
	"contact":    true,
	"network":    true,
	"financial":  true,
	"content":    true,
	"security":   true,
	"none":       true,
}

var allowedRetentionKinds = map[string]bool{
	"fixed_days":            true,
	"until_deleted":         true,
	"until_account_closure": true,
}

var allowedEnforcementStatuses = map[string]bool{
	"existing":          true,
	"documented_target": true,
}

var allowedStorageTargets = map[string]bool{
	"postgres\x00charging\x00accounts":                   true,
	"postgres\x00charging\x00sessions":                   true,
	"postgres\x00charging\x00ledger":                     true,
	"postgres\x00catalogue\x00rates":                     true,
	"postgres\x00routing\x00rules":                       true,
	"postgres\x00routing\x00lcr_routes":                  true,
	"postgres\x00routing\x00trunks":                      true,
	"postgres\x00sbc\x00barring_rules":                   true,
	"postgres\x00sbc\x00active_calls":                    true,
	"postgres\x00sbc\x00trunk_limits":                    true,
	"postgres\x00sbc\x00auth_failures":                   true,
	"postgres\x00identity\x00tenants":                    true,
	"postgres\x00identity\x00users":                      true,
	"postgres\x00identity\x00sessions":                   true,
	"postgres\x00identity\x00accounts":                   true,
	"postgres\x00billing\x00invoice_lines":               true,
	"postgres\x00billing\x00cdr_period_totals":           true,
	"postgres\x00payments\x00payments":                   true,
	"postgres\x00fraud\x00fraud_controls":                true,
	"postgres\x00fraud\x00prefix_blocks":                 true,
	"postgres\x00fraud\x00cli_blocks":                    true,
	"postgres\x00fraud\x00risk_prefixes":                 true,
	"postgres\x00fraud\x00daily_usage":                   true,
	"postgres\x00fraud\x00reservations":                  true,
	"postgres\x00fraud\x00processed_cdrs":                true,
	"postgres\x00fraud\x00alerts":                        true,
	"postgres\x00reports_sched\x00schedules":             true,
	"postgres\x00reports_sched\x00report_artifacts":      true,
	"postgres\x00notifications\x00outbox":                true,
	"postgres\x00mediation\x00cdrs":                      true,
	"clickhouse\x00reports\x00cdrs":                      true,
	"kafka\x00\x00cdrs.raw":                              true,
	"kafka\x00\x00cdrs.normalized":                       true,
}

func storageKey(target storageTarget) string {
	return target.System + "\x00" + target.Database + "\x00" + target.Table
}

func loadManifest(path string) (manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return manifest{}, err
	}
	defer file.Close()
	var m manifest
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&m); err != nil {
		return manifest{}, err
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return manifest{}, errors.New("manifest contains trailing JSON")
		}
		return manifest{}, err
	}
	return m, nil
}

func (m manifest) validate(root string) error {
	var errs []string
	if m.Version != 1 {
		errs = append(errs, fmt.Sprintf("version must be 1, got %d", m.Version))
	}
	controlIDs := map[string]bool{}
	classIDs := map[string]bool{}
	referenced := map[string]bool{}

	for i, c := range m.Controls {
		label := fmt.Sprintf("controls[%d]", i)
		if c.ID == "" {
			errs = append(errs, label+".id is required")
		} else if controlIDs[c.ID] {
			errs = append(errs, fmt.Sprintf("duplicate controls[].id %q", c.ID))
		} else {
			controlIDs[c.ID] = true
			label = fmt.Sprintf("controls[%q]", c.ID)
		}
		if c.Instrument == "" {
			errs = append(errs, label+".instrument is required")
		}
		if len(c.ClassIDs) == 0 {
			errs = append(errs, label+".class_ids must not be empty")
		}
		for _, id := range c.ClassIDs {
			referenced[id] = true
		}
	}

	for i, c := range m.Classes {
		label := fmt.Sprintf("classes[%d]", i)
		if c.ID == "" {
			errs = append(errs, label+".id is required")
		} else if classIDs[c.ID] {
			errs = append(errs, fmt.Sprintf("duplicate classes[].id %q", c.ID))
		} else {
			classIDs[c.ID] = true
			label = fmt.Sprintf("classes[%q]", c.ID)
		}
		if c.Purpose == "" {
			errs = append(errs, label+".purpose is required")
		}
		if c.LawfulBasis == "" {
			errs = append(errs, label+".lawful_basis is required")
		}
		if len(c.Storage) == 0 {
			errs = append(errs, label+".storage must not be empty")
		}
		for j, target := range c.Storage {
			if !allowedStorageTargets[storageKey(target)] {
				errs = append(errs, fmt.Sprintf("%s.storage[%d]: unknown storage target %s/%s/%s", label, j, target.System, target.Database, target.Table))
			}
			if len(target.Columns) == 0 {
				errs = append(errs, fmt.Sprintf("%s.storage[%d].columns must not be empty", label, j))
			}
		}
		for j, field := range c.PersonalData {
			if field.Field == "" {
				errs = append(errs, fmt.Sprintf("%s.personal_data[%d].field is required", label, j))
			}
			if !allowedClassifications[field.Classification] {
				errs = append(errs, fmt.Sprintf("%s.personal_data[%d]: invalid classification %q", label, j, field.Classification))
			}
			if field.Purpose == "" {
				errs = append(errs, fmt.Sprintf("%s.personal_data[%d].purpose is required", label, j))
			}
		}
		switch c.Retention.Kind {
		case "fixed_days":
			if c.Retention.Days == nil || *c.Retention.Days <= 0 {
				errs = append(errs, label+".retention.days must be greater than zero for fixed_days")
			}
		case "until_deleted", "until_account_closure":
			if c.Retention.Days != nil {
				errs = append(errs, label+".retention.days must be omitted for "+c.Retention.Kind)
			}
		default:
			errs = append(errs, fmt.Sprintf("%s.retention: invalid retention kind %q", label, c.Retention.Kind))
		}
		if len(c.Retention.Evidence) == 0 {
			errs = append(errs, label+".retention.evidence must not be empty")
		}
		for j, evidence := range c.Retention.Evidence {
			if evidence == "" || filepath.IsAbs(evidence) || strings.HasPrefix(evidence, "..") {
				errs = append(errs, fmt.Sprintf("%s.retention.evidence[%d]: invalid path %q", label, j, evidence))
				continue
			}
			if _, err := os.Stat(filepath.Join(root, evidence)); err != nil {
				errs = append(errs, fmt.Sprintf("%s.retention.evidence[%d]: evidence path not found %q", label, j, evidence))
			}
		}
		if c.Retention.CounselRequired && c.Retention.Assumption == "" {
			errs = append(errs, label+".retention: counsel assumption is required when counsel_required is true")
		}
		if !allowedEnforcementStatuses[c.EnforcementStatus] {
			errs = append(errs, fmt.Sprintf("%s.enforcement_status: invalid status %q", label, c.EnforcementStatus))
		}
	}

	var missingIDs []string
	for id := range referenced {
		if !classIDs[id] {
			missingIDs = append(missingIDs, id)
		}
	}
	sort.Strings(missingIDs)
	for _, id := range missingIDs {
		errs = append(errs, fmt.Sprintf("controls reference unknown class %q", id))
	}
	var unreferenced []string
	for id := range classIDs {
		if !referenced[id] {
			unreferenced = append(unreferenced, id)
		}
	}
	sort.Strings(unreferenced)
	for _, id := range unreferenced {
		errs = append(errs, fmt.Sprintf("class %q is not referenced by any control", id))
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}

func repoRoot() string {
	return filepath.Clean(filepath.Join("..", ".."))
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: compliancecheck <manifest.json>")
		os.Exit(2)
	}
	m, err := loadManifest(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "compliance manifest unreadable: %v\n", err)
		os.Exit(1)
	}
	if err := m.validate(repoRoot()); err != nil {
		fmt.Fprintf(os.Stderr, "compliance manifest invalid:\n%s\n", err)
		os.Exit(1)
	}
	fmt.Printf("ok: compliance manifest %d (%d classes)\n", m.Version, len(m.Classes))
}
```

- [ ] **Step 5: Run the tests and commit**

Run:

```bash
cd tools/compliancecheck && gofmt -l . && go vet ./... && go test ./...
```

Expected: `gofmt -l .` prints nothing; `go vet` and `go test` pass with `ok omnifoni/compliancecheck`.

Commit:

```bash
git add tools/compliancecheck
git commit -m "feat: add compliance manifest validator"
```

---

### Task 2: Checked-in data-classification manifest

**Files:**
- Create: `docs/compliance/data-classification.json`
- Modify: `tools/compliancecheck/main_test.go`

**Interfaces:**
- Consumes: `loadManifest`, `manifest.validate`, `allowedStorageTargets`, `storageKey` from Task 1.
- Produces: `TestCheckedInManifest`, `TestAllowedStorageTargets`; the manifest consumed by `make compliance` and all compliance docs.

- [ ] **Step 1: Write the failing checked-in manifest tests**

Append to `tools/compliancecheck/main_test.go`:

```go
func TestCheckedInManifest(t *testing.T) {
	m, err := loadManifest("../../docs/compliance/data-classification.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.validate("../.."); err != nil {
		t.Fatal(err)
	}
	if len(m.Classes) != 14 {
		t.Fatalf("got %d classes, want 14", len(m.Classes))
	}
}

func TestAllowedStorageTargets(t *testing.T) {
	m, err := loadManifest("../../docs/compliance/data-classification.json")
	if err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	for _, class := range m.Classes {
		for _, target := range class.Storage {
			used[storageKey(target)] = true
		}
	}
	for key := range allowedStorageTargets {
		if !used[key] {
			t.Errorf("allowlist entry %q is unused by the manifest", key)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
cd tools/compliancecheck && go test ./...
```

Expected: FAIL with `no such file or directory` for `../../docs/compliance/data-classification.json`.

- [ ] **Step 3: Create the manifest**

Create `docs/compliance/data-classification.json` with exactly:

```json
{
  "version": 1,
  "controls": [
    {
      "id": "popia-s10",
      "instrument": "POPIA s10",
      "class_ids": [
        "cdr",
        "charging_financial",
        "charging_session",
        "billing_financial",
        "payment",
        "identity",
        "identity_session",
        "fraud_security",
        "fraud_configuration",
        "sbc_security",
        "report_artifact",
        "report_configuration",
        "notification_outbox",
        "configuration_catalogue_routing"
      ]
    },
    {
      "id": "popia-s11",
      "instrument": "POPIA s11",
      "class_ids": [
        "cdr",
        "charging_financial",
        "charging_session",
        "billing_financial",
        "payment",
        "identity",
        "identity_session",
        "fraud_security",
        "fraud_configuration",
        "sbc_security",
        "report_artifact",
        "report_configuration",
        "notification_outbox",
        "configuration_catalogue_routing"
      ]
    },
    {
      "id": "popia-s19",
      "instrument": "POPIA s19",
      "class_ids": [
        "cdr",
        "charging_financial",
        "identity",
        "identity_session",
        "fraud_security",
        "sbc_security",
        "report_artifact",
        "report_configuration",
        "notification_outbox"
      ]
    },
    {
      "id": "popia-s22",
      "instrument": "POPIA s22",
      "class_ids": ["cdr", "identity", "notification_outbox"]
    },
    {
      "id": "popia-ss55-56",
      "instrument": "POPIA ss55-56",
      "class_ids": ["identity", "identity_session"]
    },
    {
      "id": "popia-s72",
      "instrument": "POPIA s72",
      "class_ids": [
        "cdr",
        "charging_financial",
        "charging_session",
        "billing_financial",
        "payment",
        "identity",
        "identity_session",
        "fraud_security",
        "fraud_configuration",
        "sbc_security",
        "report_artifact",
        "report_configuration",
        "notification_outbox",
        "configuration_catalogue_routing"
      ]
    },
    {
      "id": "rica-kyc",
      "instrument": "RICA subscriber verification assumption",
      "class_ids": ["identity", "cdr"]
    },
    {
      "id": "rica-intercept",
      "instrument": "RICA lawful intercept assumption",
      "class_ids": ["cdr", "sbc_security"]
    },
    {
      "id": "icasa-licence",
      "instrument": "ICASA licence-classification assumption",
      "class_ids": ["configuration_catalogue_routing"]
    },
    {
      "id": "traffic-retention",
      "instrument": "Traffic-data retention assumption",
      "class_ids": ["cdr", "billing_financial", "payment"]
    },
    {
      "id": "au-malabo",
      "instrument": "AU Malabo Convention alignment",
      "class_ids": [
        "cdr",
        "charging_financial",
        "charging_session",
        "billing_financial",
        "payment",
        "identity",
        "identity_session",
        "fraud_security",
        "fraud_configuration",
        "sbc_security",
        "report_artifact",
        "report_configuration",
        "notification_outbox",
        "configuration_catalogue_routing"
      ]
    },
    {
      "id": "ai-governance",
      "instrument": "AI governance",
      "class_ids": ["fraud_security", "fraud_configuration"]
    }
  ],
  "classes": [
    {
      "id": "cdr",
      "purpose": "Bill and reconcile completed calls and support operator traffic reporting.",
      "lawful_basis": "POPIA s11(1)(b) contract performance and s11(1)(f) legitimate interest",
      "personal_data": [
        {
          "field": "account_id",
          "classification": "identifier",
          "purpose": "Link a call to the billed account."
        },
        {
          "field": "caller_id",
          "classification": "network",
          "purpose": "Identify the originating number for billing and fraud controls."
        },
        {
          "field": "destination",
          "classification": "network",
          "purpose": "Rate the call and detect toll-fraud prefixes."
        },
        {
          "field": "started_at",
          "classification": "content",
          "purpose": "Establish the billing period and call duration."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "mediation",
          "table": "cdrs",
          "columns": ["call_id", "account_id", "caller_id", "destination", "started_at"]
        },
        {
          "system": "clickhouse",
          "database": "reports",
          "table": "cdrs",
          "columns": ["call_id", "account_id", "caller_id", "destination", "started_at"]
        },
        {
          "system": "kafka",
          "database": "",
          "table": "cdrs.raw",
          "columns": ["payload"]
        },
        {
          "system": "kafka",
          "database": "",
          "table": "cdrs.normalized",
          "columns": ["payload"]
        }
      ],
      "retention": {
        "kind": "fixed_days",
        "days": 90,
        "evidence": ["services/mediation/main.go", "services/reports/main.go", "services/switching/main.go"],
        "counsel_required": true,
        "assumption": "Ninety days is an engineering default; statutory traffic-data and billing-retention periods need counsel confirmation."
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "charging_financial",
      "purpose": "Maintain prepaid balances and postpaid charging ledgers.",
      "lawful_basis": "POPIA s11(1)(b) contract performance",
      "personal_data": [
        {
          "field": "account_id",
          "classification": "identifier",
          "purpose": "Identify the charged account."
        },
        {
          "field": "amount_minor",
          "classification": "financial",
          "purpose": "Record monetary charging events in minor units."
        },
        {
          "field": "balance_after",
          "classification": "financial",
          "purpose": "Record the resulting account balance."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "charging",
          "table": "accounts",
          "columns": ["account_id", "balance_minor", "credit_limit_minor"]
        },
        {
          "system": "postgres",
          "database": "charging",
          "table": "ledger",
          "columns": ["account_id", "session_id", "delta_minor", "balance_after"]
        }
      ],
      "retention": {
        "kind": "fixed_days",
        "days": 2555,
        "evidence": ["services/charging/migrate.sql", "services/charging/main.go"],
        "counsel_required": true,
        "assumption": "Seven years is a conservative financial-record default; counsel must confirm the applicable period."
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "charging_session",
      "purpose": "Track active and completed charging sessions for the OCS.",
      "lawful_basis": "POPIA s11(1)(b) contract performance",
      "personal_data": [
        {
          "field": "account_id",
          "classification": "identifier",
          "purpose": "Link a charging session to the customer account."
        },
        {
          "field": "session_id",
          "classification": "identifier",
          "purpose": "Correlate ledger entries with a call."
        },
        {
          "field": "max_seconds",
          "classification": "financial",
          "purpose": "Record the authorised call duration."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "charging",
          "table": "sessions",
          "columns": ["session_id", "account_id", "started_at", "max_seconds"]
        }
      ],
      "retention": {
        "kind": "fixed_days",
        "days": 90,
        "evidence": ["services/charging/migrate.sql"],
        "counsel_required": false,
        "assumption": ""
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "billing_financial",
      "purpose": "Invoice completed calls and maintain per-period billing totals.",
      "lawful_basis": "POPIA s11(1)(b) contract performance",
      "personal_data": [
        {
          "field": "account_id",
          "classification": "identifier",
          "purpose": "Place an invoice line on the correct account."
        },
        {
          "field": "call_id",
          "classification": "identifier",
          "purpose": "Deduplicate invoice lines across Kafka replays."
        },
        {
          "field": "amount_minor",
          "classification": "financial",
          "purpose": "Record the invoice amount in minor units."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "billing",
          "table": "invoice_lines",
          "columns": ["call_id", "account_id", "period", "amount_minor"]
        },
        {
          "system": "postgres",
          "database": "billing",
          "table": "cdr_period_totals",
          "columns": ["account_id", "period", "amount_minor"]
        }
      ],
      "retention": {
        "kind": "fixed_days",
        "days": 2555,
        "evidence": ["services/billing/migrate.sql", "services/billing/biller.go"],
        "counsel_required": true,
        "assumption": "Seven years is a conservative financial-record default; counsel must confirm the applicable period."
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "payment",
      "purpose": "Record gateway payment webhooks and support reconciliation.",
      "lawful_basis": "POPIA s11(1)(b) contract performance",
      "personal_data": [
        {
          "field": "account_id",
          "classification": "identifier",
          "purpose": "Apply a payment to the customer account."
        },
        {
          "field": "event_id",
          "classification": "identifier",
          "purpose": "Deduplicate webhook deliveries."
        },
        {
          "field": "amount_minor",
          "classification": "financial",
          "purpose": "Record the received amount in minor units."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "payments",
          "table": "payments",
          "columns": ["event_id", "gateway", "account_id", "amount_minor", "created_at"]
        }
      ],
      "retention": {
        "kind": "fixed_days",
        "days": 2555,
        "evidence": ["services/payments/migrate.sql", "services/payments/main.go"],
        "counsel_required": true,
        "assumption": "Seven years is a conservative financial-record default; counsel must confirm the applicable period."
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "identity",
      "purpose": "Authenticate operators and authorise tenant-scoped access.",
      "lawful_basis": "POPIA s11(1)(b) contract performance and s11(1)(f) legitimate interest",
      "personal_data": [
        {
          "field": "email",
          "classification": "contact",
          "purpose": "Identify users and deliver scheduled-report notifications."
        },
        {
          "field": "password_hash",
          "classification": "security",
          "purpose": "Verify user passwords without storing plaintext."
        },
        {
          "field": "role",
          "classification": "security",
          "purpose": "Enforce admin, reseller, and user authorisation."
        },
        {
          "field": "tenant_id",
          "classification": "identifier",
          "purpose": "Enforce tenant scoping."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "identity",
          "table": "tenants",
          "columns": ["tenant_id", "name"]
        },
        {
          "system": "postgres",
          "database": "identity",
          "table": "users",
          "columns": ["user_id", "tenant_id", "email", "password_hash", "role"]
        },
        {
          "system": "postgres",
          "database": "identity",
          "table": "accounts",
          "columns": ["account_id", "tenant_id"]
        }
      ],
      "retention": {
        "kind": "until_account_closure",
        "evidence": ["services/identity/migrate.sql", "services/identity/identity.go"],
        "counsel_required": true,
        "assumption": "Closure criteria and post-closure retention need counsel validation."
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "identity_session",
      "purpose": "Maintain short-lived authenticated browser and API sessions.",
      "lawful_basis": "POPIA s11(1)(f) legitimate interest",
      "personal_data": [
        {
          "field": "token",
          "classification": "security",
          "purpose": "Authenticate a request without re-sending credentials."
        },
        {
          "field": "user_id",
          "classification": "identifier",
          "purpose": "Bind a session to an authenticated user."
        },
        {
          "field": "expires_at",
          "classification": "security",
          "purpose": "Expire sessions after 24 hours."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "identity",
          "table": "sessions",
          "columns": ["token", "user_id", "expires_at"]
        }
      ],
      "retention": {
        "kind": "fixed_days",
        "days": 1,
        "evidence": ["services/identity/identity.go", "services/identity/migrate.sql"],
        "counsel_required": false,
        "assumption": ""
      },
      "enforcement_status": "existing"
    },
    {
      "id": "fraud_security",
      "purpose": "Detect toll fraud and retain security alerts and usage for investigation.",
      "lawful_basis": "POPIA s11(1)(f) legitimate interest",
      "personal_data": [
        {
          "field": "account_id",
          "classification": "identifier",
          "purpose": "Apply velocity controls to the calling account."
        },
        {
          "field": "caller_id",
          "classification": "network",
          "purpose": "Detect caller-ID fraud patterns."
        },
        {
          "field": "destination",
          "classification": "network",
          "purpose": "Detect high-risk called prefixes."
        },
        {
          "field": "score",
          "classification": "security",
          "purpose": "Record advisory fraud scores for alerting."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "fraud",
          "table": "daily_usage",
          "columns": ["account_id", "dest_prefix", "call_date", "call_count"]
        },
        {
          "system": "postgres",
          "database": "fraud",
          "table": "reservations",
          "columns": ["call_id", "account_id", "caller_id", "destination"]
        },
        {
          "system": "postgres",
          "database": "fraud",
          "table": "processed_cdrs",
          "columns": ["call_id", "account_id"]
        },
        {
          "system": "postgres",
          "database": "fraud",
          "table": "alerts",
          "columns": ["call_id", "account_id", "score", "reasons"]
        }
      ],
      "retention": {
        "kind": "fixed_days",
        "days": 365,
        "evidence": ["services/fraud/migrate.sql", "services/fraud/fraud.go"],
        "counsel_required": true,
        "assumption": "One year is an engineering default; counsel must confirm fraud-investigation retention needs."
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "fraud_configuration",
      "purpose": "Maintain deterministic fraud policies and operator-managed blocks.",
      "lawful_basis": "POPIA s11(1)(f) legitimate interest",
      "personal_data": [
        {
          "field": "prefix",
          "classification": "network",
          "purpose": "Block or score high-risk destination ranges."
        },
        {
          "field": "caller_id",
          "classification": "network",
          "purpose": "Block known fraudulent caller IDs."
        },
        {
          "field": "enabled",
          "classification": "security",
          "purpose": "Control policy activation."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "fraud",
          "table": "fraud_controls",
          "columns": ["key", "value"]
        },
        {
          "system": "postgres",
          "database": "fraud",
          "table": "prefix_blocks",
          "columns": ["prefix", "reason", "enabled"]
        },
        {
          "system": "postgres",
          "database": "fraud",
          "table": "cli_blocks",
          "columns": ["caller_id", "reason", "enabled"]
        },
        {
          "system": "postgres",
          "database": "fraud",
          "table": "risk_prefixes",
          "columns": ["prefix", "risk", "enabled"]
        }
      ],
      "retention": {
        "kind": "until_deleted",
        "evidence": ["services/fraud/migrate.sql"],
        "counsel_required": false,
        "assumption": ""
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "sbc_security",
      "purpose": "Enforce session-border barring, trunk limits, and intrusion blocks.",
      "lawful_basis": "POPIA s11(1)(f) legitimate interest",
      "personal_data": [
        {
          "field": "source_ip",
          "classification": "network",
          "purpose": "Detect and block repeated authentication failures."
        },
        {
          "field": "call_id",
          "classification": "identifier",
          "purpose": "Track active call slots."
        },
        {
          "field": "trunk_id",
          "classification": "network",
          "purpose": "Enforce per-trunk concurrency limits."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "sbc",
          "table": "auth_failures",
          "columns": ["source_ip", "occurred_at"]
        },
        {
          "system": "postgres",
          "database": "sbc",
          "table": "active_calls",
          "columns": ["call_id", "trunk_id", "started_at"]
        }
      ],
      "retention": {
        "kind": "fixed_days",
        "days": 30,
        "evidence": ["services/sbc/migrate.sql", "services/sbc/sbc.go"],
        "counsel_required": false,
        "assumption": ""
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "report_artifact",
      "purpose": "Store generated scheduled-report CSV artifacts for download and notification.",
      "lawful_basis": "POPIA s11(1)(f) legitimate interest",
      "personal_data": [
        {
          "field": "artifact_id",
          "classification": "identifier",
          "purpose": "Identify a generated artifact."
        },
        {
          "field": "recipient_email",
          "classification": "contact",
          "purpose": "Notify the schedule creator that an artifact is ready."
        },
        {
          "field": "content",
          "classification": "content",
          "purpose": "Deliver the operator-requested report."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "reports_sched",
          "table": "report_artifacts",
          "columns": ["artifact_id", "schedule_id", "slot_key", "recipient_email", "content"]
        }
      ],
      "retention": {
        "kind": "fixed_days",
        "days": 90,
        "evidence": ["services/reports/main.go", "services/reports/sched_migrate.sql"],
        "counsel_required": true,
        "assumption": "Ninety days matches the current prune default; counsel must confirm artifact retention needs."
      },
      "enforcement_status": "existing"
    },
    {
      "id": "report_configuration",
      "purpose": "Store operator-defined recurring report schedules.",
      "lawful_basis": "POPIA s11(1)(f) legitimate interest",
      "personal_data": [
        {
          "field": "created_by",
          "classification": "identifier",
          "purpose": "Attribute a schedule to its creator."
        },
        {
          "field": "recipient_email",
          "classification": "contact",
          "purpose": "Deliver generated artifact notifications."
        },
        {
          "field": "kind",
          "classification": "content",
          "purpose": "Define which report the schedule renders."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "reports_sched",
          "table": "schedules",
          "columns": ["schedule_id", "tenant_id", "created_by", "recipient_email", "kind"]
        }
      ],
      "retention": {
        "kind": "until_deleted",
        "evidence": ["services/reports/sched_migrate.sql", "services/reports/main.go"],
        "counsel_required": false,
        "assumption": ""
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "notification_outbox",
      "purpose": "Provide a durable outbox for scheduled-report email delivery.",
      "lawful_basis": "POPIA s11(1)(f) legitimate interest",
      "personal_data": [
        {
          "field": "recipient_email",
          "classification": "contact",
          "purpose": "Deliver a report-ready notification."
        },
        {
          "field": "artifact_id",
          "classification": "identifier",
          "purpose": "Correlate the notification with its artifact."
        },
        {
          "field": "status",
          "classification": "security",
          "purpose": "Track send and retry state."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "notifications",
          "table": "outbox",
          "columns": ["artifact_id", "recipient_email", "status", "attempts", "created_at"]
        }
      ],
      "retention": {
        "kind": "fixed_days",
        "days": 90,
        "evidence": ["services/notifications/migrate.sql", "services/notifications/main.go"],
        "counsel_required": true,
        "assumption": "Ninety days is an engineering default; counsel must confirm outbox retention needs."
      },
      "enforcement_status": "documented_target"
    },
    {
      "id": "configuration_catalogue_routing",
      "purpose": "Maintain rates, dial plans, LCR routes, trunks, and SBC policy configuration.",
      "lawful_basis": "POPIA s11(1)(f) legitimate interest",
      "personal_data": [
        {
          "field": "prefix",
          "classification": "network",
          "purpose": "Match dialled numbers to rates, routes, or barring rules."
        },
        {
          "field": "trunk_id",
          "classification": "network",
          "purpose": "Select an outbound trunk and enforce trunk limits."
        },
        {
          "field": "rate_per_sec",
          "classification": "financial",
          "purpose": "Price calls in minor units per second."
        }
      ],
      "storage": [
        {
          "system": "postgres",
          "database": "catalogue",
          "table": "rates",
          "columns": ["prefix", "rate_per_sec"]
        },
        {
          "system": "postgres",
          "database": "routing",
          "table": "rules",
          "columns": ["prefix", "trunk_id"]
        },
        {
          "system": "postgres",
          "database": "routing",
          "table": "lcr_routes",
          "columns": ["prefix", "trunk_id", "cost_minor"]
        },
        {
          "system": "postgres",
          "database": "routing",
          "table": "trunks",
          "columns": ["trunk_id", "target_uri", "enabled"]
        },
        {
          "system": "postgres",
          "database": "sbc",
          "table": "barring_rules",
          "columns": ["direction", "prefix", "enabled"]
        },
        {
          "system": "postgres",
          "database": "sbc",
          "table": "trunk_limits",
          "columns": ["trunk_id", "max_calls"]
        }
      ],
      "retention": {
        "kind": "until_deleted",
        "evidence": ["services/catalogue/migrate.sql", "services/routing/migrate.sql", "services/sbc/migrate.sql"],
        "counsel_required": true,
        "assumption": "ICASA licence-classification requirements for routing and numbering configuration need counsel validation."
      },
      "enforcement_status": "documented_target"
    }
  ]
}
```

- [ ] **Step 4: Run the tests and commit**

Run:

```bash
cd tools/compliancecheck && gofmt -l . && go vet ./... && go test ./... && go run . ../../docs/compliance/data-classification.json
```

Expected:

```text
ok  	omnifoni/compliancecheck	0.XXXs
?   	omnifoni/compliancecheck	[no test files]
ok: compliance manifest 1 (14 classes)
```

Commit:

```bash
git add docs/compliance/data-classification.json tools/compliancecheck/main_test.go
git commit -m "feat: add POPIA data classification manifest"
```

---

### Task 3: Compliance documentation pack

**Files:**
- Create: `docs/compliance/README.md`
- Create: `docs/compliance/popia-control-matrix.md`
- Create: `docs/compliance/runbooks/information-officer-paia.md`
- Create: `docs/compliance/runbooks/breach-notification.md`
- Create: `docs/compliance/runbooks/retention-legal-hold.md`
- Modify: `tools/compliancecheck/main_test.go`

**Interfaces:**
- Consumes: the 14 class IDs and 12 control IDs from Task 2.
- Produces: `TestMatrixCoversManifestClasses`; docs referenced by `docs/compliance/README.md` and the root `Makefile`.

- [ ] **Step 1: Write the failing matrix-coverage test**

Append to `tools/compliancecheck/main_test.go`:

```go
func TestMatrixCoversManifestClasses(t *testing.T) {
	m, err := loadManifest("../../docs/compliance/data-classification.json")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("../../docs/compliance/popia-control-matrix.md")
	if err != nil {
		t.Fatal(err)
	}
	matrix := string(data)
	for _, class := range m.Classes {
		if !strings.Contains(matrix, "`"+class.ID+"`") {
			t.Errorf("matrix does not reference class %q", class.ID)
		}
	}
	for _, control := range m.Controls {
		if !strings.Contains(matrix, "`"+control.ID+"`") {
			t.Errorf("matrix does not reference control %q", control.ID)
		}
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cd tools/compliancecheck && go test ./...
```

Expected: FAIL with `no such file or directory` for `../../docs/compliance/popia-control-matrix.md`.

- [ ] **Step 3: Create the documentation index**

Create `docs/compliance/README.md` with exactly:

```markdown
# Compliance pack

This pack is an engineering-ready draft, not legal advice. Feature claims come
only from the four ASTPP marketing pages cited in `docs/master-plan.md`
sections 0 and 8. Design controls are taken from sections 5 and 6.

- `data-classification.json` — authoritative purpose, classification, storage,
  and retention declarations.
- `popia-control-matrix.md` — control matrix with evidence and blank sign-off
  cells.
- `runbooks/information-officer-paia.md` — Information Officer and PAIA request
  handling.
- `runbooks/breach-notification.md` — breach escalation and notification
  template.
- `runbooks/retention-legal-hold.md` — retention defaults and manual legal
  holds.

Run `make compliance` from the repository root to validate the manifest and
docs. The validator is stdlib-only Go and has no network or database access.
```

- [ ] **Step 4: Create the control matrix**

Create `docs/compliance/popia-control-matrix.md` with exactly:

```markdown
# POPIA control matrix

Status: engineering-ready draft. This document is not legal advice.
Source controls: `docs/master-plan.md` sections 5 and 6.
Manifest: `data-classification.json`.

| Obligation | Instrument | Design control | Manifest class IDs | Evidence | Status | Assumption / counsel gate | Engineering sign-off | Information Officer sign-off | Legal sign-off |
|---|---|---|---|---|---|---|---|---|---|
| Data minimisation | POPIA s10 | Only required call, account, and operator fields are stored per flow. | `cdr`, `charging_session`, `identity`, `sbc_security` | `services/mediation/main.go`, `services/identity/identity.go`, `services/sbc/migrate.sql` | documented_target | Counsel must confirm field-level minimisation. |  |  |  |
| Purpose limitation | POPIA s11 / `popia-s11` | Every class declares a purpose and lawful basis. | `cdr`, `charging_financial`, `charging_session`, `billing_financial`, `payment`, `identity`, `identity_session`, `fraud_security`, `fraud_configuration`, `sbc_security`, `report_artifact`, `report_configuration`, `notification_outbox`, `configuration_catalogue_routing` | `data-classification.json` | implemented | Counsel must confirm lawful-basis wording. |  |  |  |
| Security safeguards | POPIA s19 / `popia-s19` | Tenant scoping, bcrypt, redacted JSON logs, signed artifact links, fraud/SBC controls, and 24h sessions. | `cdr`, `charging_financial`, `identity`, `identity_session`, `fraud_security`, `sbc_security`, `report_artifact`, `report_configuration`, `notification_outbox` | `services/identity/identity.go`, `services/observability/log.go`, `services/reports/main.go`, `services/fraud/fraud.go`, `services/sbc/sbc.go` | implemented | Counsel and security review must confirm safeguard adequacy. |  |  |  |
| Breach notification | POPIA s22 / `popia-s22` | Detect, contain, preserve evidence, escalate internally within 72 hours, then decide regulator notification. | `cdr`, `identity`, `notification_outbox` | `runbooks/breach-notification.md` | counsel_required | Regulator and deadline details require counsel confirmation. |  |  |  |
| Information Officer and PAIA | POPIA ss55-56 / `popia-ss55-56` | Runbook defines intake, verification, tenant scoping, and response assembly from the manifest. | `identity`, `identity_session` | `runbooks/information-officer-paia.md` | counsel_required | The 30-day target response window needs counsel confirmation. |  |  |  |
| Cross-border transfer | POPIA s72 / `popia-s72` | Every class is covered by a jurisdiction-transfer assumption for the chosen deployment. | `cdr`, `charging_financial`, `charging_session`, `billing_financial`, `payment`, `identity`, `identity_session`, `fraud_security`, `fraud_configuration`, `sbc_security`, `report_artifact`, `report_configuration`, `notification_outbox`, `configuration_catalogue_routing` | `data-classification.json` | counsel_required | Deployment region and transfer mechanism need counsel validation. |  |  |  |
| RICA subscriber verification | RICA / `rica-kyc` | KYC is out of scope; identity and CDR classes record the assumption that an operator performs subscriber verification upstream. | `identity`, `cdr` | `docs/master-plan.md`, `data-classification.json` | counsel_required | Counsel must confirm whether upstream verification satisfies RICA duties. |  |  |  |
| RICA lawful intercept | RICA / `rica-intercept` | Lawful-intercept hooks are out of scope; CDR and SBC classes record the assumption. | `cdr`, `sbc_security` | `docs/master-plan.md`, `data-classification.json` | counsel_required | Counsel must confirm statutory intercept obligations. |  |  |  |
| ICASA licence classification | ICASA / `icasa-licence` | Routing and policy configuration is declared as operator licensing input. | `configuration_catalogue_routing` | `data-classification.json` | counsel_required | Licence category and related reporting need counsel validation. |  |  |  |
| Traffic-data retention | Traffic retention / `traffic-retention` | CDR, billing, and payment retention defaults are documented; deletion workers are not implemented. | `cdr`, `billing_financial`, `payment` | `runbooks/retention-legal-hold.md`, `data-classification.json` | documented_target | Counsel must confirm statutory traffic and billing periods. |  |  |  |
| AU Malabo Convention | AU Malabo / `au-malabo` | All classes carry purpose, lawful basis, security, and transfer assumptions. | `cdr`, `charging_financial`, `charging_session`, `billing_financial`, `payment`, `identity`, `identity_session`, `fraud_security`, `fraud_configuration`, `sbc_security`, `report_artifact`, `report_configuration`, `notification_outbox`, `configuration_catalogue_routing` | `data-classification.json` | counsel_required | Ratification and deployment applicability need counsel validation. |  |  |  |
| AI governance | AI governance / `ai-governance` | Fraud scoring is versioned and advisory; deterministic controls make denial decisions. | `fraud_security`, `fraud_configuration` | `services/fraud/fraud.go`, `docs/fraud-baseline.md` | implemented | Counsel and product owners must confirm explainability and review duties. |  |  |  |

## Sign-off

Engineering review:  ____________________  Date: __________
Information Officer: ____________________  Date: __________
Legal / counsel:      ____________________  Date: __________
```

- [ ] **Step 5: Create the runbooks**

Create `docs/compliance/runbooks/information-officer-paia.md` with exactly:

```markdown
# Information Officer and PAIA runbook

Status: engineering-ready draft. This runbook is not legal advice.

## Role

The deployment's Information Officer is the named operator contact for
personal-information requests. The repository does not assign a person; the
deployment record must do so.

## Intake

Record:

- request date and reference;
- requester identity and contact details;
- requested information or purpose;
- tenant or account scope claimed by the requester;
- requested format.

## Verification

1. Verify requester identity using operator-approved evidence.
2. Confirm the tenant and account scope with the identity registry.
3. Do not disclose another tenant's data. Resellers are scoped to their own
   tenant; users cannot list accounts.

## Response assembly

1. Find relevant classes in `../data-classification.json`.
2. Use the manifest's storage targets as the collection checklist.
3. Export only the requester's in-scope data.
4. Redact third-party personal information not belonging to the requester.
5. Record what was searched, what was disclosed, and why any item was withheld.

Target response window: 30 days. Counsel must confirm the statutory deadline
and permitted grounds for refusal.
```

Create `docs/compliance/runbooks/breach-notification.md` with exactly:

````markdown
# Breach notification runbook

Status: engineering-ready draft. This runbook is not legal advice.

## Detection intake

Record:

- detection time and detector;
- affected systems from `../data-classification.json`;
- affected class IDs, tenant IDs, and approximate record counts;
- whether credentials, payment data, CDRs, or report artifacts are involved;
- current containment owner.

## Containment

1. Preserve logs, Kafka offsets, and database snapshots before destructive
   cleanup.
2. Revoke affected identity sessions if credentials or tokens may be exposed.
3. Disable affected signed artifact URLs if artifacts may be exposed.
4. Do not rotate signing secrets until the evidence-preservation step is done.

## Escalation

Escalate to the Information Officer and counsel within 72 hours of detection.
The repository does not define the regulator, filing channel, or statutory
deadline; counsel must supply them per deployment.

## Notification template

```text
Subject: Suspected personal-information breach — <reference>

Detected at: <UTC timestamp>
Affected systems: <systems>
Affected classes: <class ids>
Nature of breach: <summary>
Containment actions: <list>
Evidence preserved: <list>
Information Officer: <name>
Counsel contact: <name>
Next update due: <UTC timestamp>
```
````

Create `docs/compliance/runbooks/retention-legal-hold.md` with exactly:

```markdown
# Retention and legal-hold runbook

Status: engineering-ready draft. This runbook is not legal advice.

## Retention kinds

- `fixed_days`: retain for the declared number of days. Where no runtime
  deletion job exists, this is a documented target, not an enforced control.
- `until_deleted`: retain until an operator or customer deletes the record.
- `until_account_closure`: retain until the account-closure process completes.

Identity sessions expire after 24 hours in `services/identity/identity.go`.
Scheduled report artifacts are pruned by `REPORTS_SCHED_RETENTION_DAYS` in
`services/reports/main.go`. All other retention entries are documented targets.

## Legal hold

1. Record the hold reference, scope, start date, and authorising counsel.
2. Identify affected class IDs and storage targets from
   `../data-classification.json`.
3. Pause any planned retention work for those targets.
4. Notify the Information Officer and the system owners.
5. Record hold release with date, authoriser, and scope before normal
   retention resumes.

## Follow-up work

Runtime deletion workers for CDRs, financial records, fraud alerts, SBC
security data, and notification outbox rows are not implemented by this pack.
A future slice must define ordering, idempotency, observability, and legal-hold
interlocks before deleting production data.
```

- [ ] **Step 6: Run the tests and commit**

Run:

```bash
cd tools/compliancecheck && gofmt -l . && go vet ./... && go test ./...
```

Expected: all tests pass, including `TestMatrixCoversManifestClasses`.

Commit:

```bash
git add docs/compliance tools/compliancecheck/main_test.go
git commit -m "docs: add POPIA control matrix and runbooks"
```

---

### Task 4: Wire the gate into Make and record project status

**Files:**
- Modify: `Makefile:1`, `Makefile:18-19`
- Modify: `AGENTS.md`

**Interfaces:**
- Consumes: `tools/compliancecheck` and `docs/compliance/data-classification.json` from Tasks 1-3.
- Produces: `make compliance`; `make test` depends on the compliance gate; project status entry in `AGENTS.md`.

- [ ] **Step 1: Add the failing Make target check**

Run:

```bash
make -n compliance
```

Expected: FAIL with `No rule to make target 'compliance'`.

- [ ] **Step 2: Modify the Makefile**

Edit `Makefile:1` from:

```make
.PHONY: up down ps smoke logs test protoc
```

to:

```make
.PHONY: up down ps smoke logs test protoc compliance
```

Insert this target above `test:`:

```make
compliance:
	cd tools/compliancecheck && go vet ./... && go test ./... && go run . ../../docs/compliance/data-classification.json
```

Edit `Makefile:18-19` from:

```make
test:
	cd services/billing && go vet ./... && go test ./...
```

to:

```make
test: compliance
	cd services/billing && go vet ./... && go test ./...
```

- [ ] **Step 3: Update `AGENTS.md` status**

Insert this bullet immediately after the P6 slice one bullet:

```markdown
- P6 slice two: the POPIA control pack lives in `docs/compliance/` (`README.md`, `data-classification.json`, `popia-control-matrix.md`, `runbooks/`). `tools/compliancecheck` is a standalone stdlib-only Go validator for the manifest's schema, retention rules, storage allowlist, evidence paths, and class/control coverage; run it with `make compliance`, which is also a prerequisite of `make test`. Retention entries without a runtime prune job are `documented_target`, not enforced; legal sign-off cells remain blank. No runtime deletion, KYC, lawful-intercept, residency, or operator UI was added.
```

- [ ] **Step 4: Run focused verification**

Run:

```bash
make -n compliance && make compliance && gofmt -l tools/compliancecheck
```

Expected: `make -n compliance` prints the validator command; `make compliance` ends with `ok: compliance manifest 1 (14 classes)`; `gofmt -l tools/compliancecheck` prints nothing.

- [ ] **Step 5: Run full verification**

With the dev stack up, run:

```bash
COMPOSE_PROJECT_NAME=omni-fon make protoc
git diff --exit-code
make test
COMPOSE_PROJECT_NAME=omni-fon make smoke
COMPOSE_PROJECT_NAME=omni-fon bash ops/sip-e2e.sh
docker compose config -q
```

Expected: all commands exit 0; `make test` starts with the compliance gate and passes; smoke prints its normal success markers; `git diff --exit-code` prints nothing after `make protoc`.

- [ ] **Step 6: Commit**

```bash
git add Makefile AGENTS.md
git commit -m "chore: gate tests on compliance manifest"
```

## Self-review

- Spec coverage: Tasks 1-4 cover every spec section — architecture and file
  layout, manifest schema/rules, all 14 stores, control matrix, three runbooks,
  CLI/CI gate, tests, verification commands, sign-off block, risks, and
  acceptance criteria.
- Placeholder scan: no `TODO`, `TBD`, `FIXME`, “fill in”, “appropriate”, or
  “similar to” shortcuts remain; every code-changing step includes complete
  code and exact commands.
- Type consistency: `manifest`, `control`, `dataClass`, `personalField`,
  `storageTarget`, `retention`, `loadManifest`, `validate`, `storageKey`, and
  `allowedStorageTargets` are defined in Task 1 and used consistently in Tasks
  2-3. `make compliance` and `test: compliance` are defined in Task 4 with the
  same CLI invocation used in Task 2.
