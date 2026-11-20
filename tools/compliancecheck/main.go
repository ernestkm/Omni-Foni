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
	ID                string          `json:"id"`
	Purpose           string          `json:"purpose"`
	LawfulBasis       string          `json:"lawful_basis"`
	PersonalData      []personalField `json:"personal_data"`
	Storage           []storageTarget `json:"storage"`
	Retention         retention       `json:"retention"`
	EnforcementStatus string          `json:"enforcement_status"`
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
	"postgres\x00charging\x00accounts":              true,
	"postgres\x00charging\x00sessions":              true,
	"postgres\x00charging\x00ledger":                true,
	"postgres\x00catalogue\x00rates":                true,
	"postgres\x00routing\x00rules":                  true,
	"postgres\x00routing\x00lcr_routes":             true,
	"postgres\x00routing\x00trunks":                 true,
	"postgres\x00sbc\x00barring_rules":              true,
	"postgres\x00sbc\x00active_calls":               true,
	"postgres\x00sbc\x00trunk_limits":               true,
	"postgres\x00sbc\x00auth_failures":              true,
	"postgres\x00identity\x00tenants":               true,
	"postgres\x00identity\x00users":                 true,
	"postgres\x00identity\x00sessions":              true,
	"postgres\x00identity\x00accounts":              true,
	"postgres\x00billing\x00invoice_lines":          true,
	"postgres\x00billing\x00cdr_period_totals":      true,
	"postgres\x00payments\x00payments":              true,
	"postgres\x00fraud\x00fraud_controls":           true,
	"postgres\x00fraud\x00prefix_blocks":            true,
	"postgres\x00fraud\x00cli_blocks":               true,
	"postgres\x00fraud\x00risk_prefixes":            true,
	"postgres\x00fraud\x00daily_usage":              true,
	"postgres\x00fraud\x00reservations":             true,
	"postgres\x00fraud\x00processed_cdrs":           true,
	"postgres\x00fraud\x00alerts":                   true,
	"postgres\x00reports_sched\x00schedules":        true,
	"postgres\x00reports_sched\x00report_artifacts": true,
	"postgres\x00notifications\x00outbox":           true,
	"postgres\x00mediation\x00cdrs":                 true,
	"clickhouse\x00reports\x00cdrs":                 true,
	"kafka\x00\x00cdrs.raw":                         true,
	"kafka\x00\x00cdrs.normalized":                  true,
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
		errs = append(errs, fmt.Sprintf("control references unknown class %q", id))
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
