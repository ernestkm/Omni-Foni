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

	for _, required := range []string{".git/", ".worktrees/", "artifacts/", ".superpowers/"} {
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
