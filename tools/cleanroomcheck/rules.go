package main

import (
	"fmt"
	"path"
	"path/filepath"
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
	files = excludeAuditConfigFiles(root, policyPath, p, files)

	var findings []Finding
	findings = append(findings, checkArtifacts(files, p, now)...)
	findings = append(findings, checkTerms(files, p, now)...)
	findings = append(findings, checkClaims(files, p)...)
	findings = append(findings, checkLicences(manifest, p)...)
	sortFindings(findings)
	return findings, nil
}

func excludeAuditConfigFiles(root, policyPath string, p *Policy, files []scannedFile) []scannedFile {
	configPaths := map[string]bool{}
	for _, candidate := range []string{policyPath, filepath.Join(filepath.Dir(policyPath), p.LicencePolicy.Manifest)} {
		if rel, err := filepath.Rel(root, candidate); err == nil {
			configPaths[filepath.ToSlash(rel)] = true
		}
	}
	kept := files[:0]
	for _, file := range files {
		if configPaths[file.Path] {
			continue
		}
		kept = append(kept, file)
	}
	return kept
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
