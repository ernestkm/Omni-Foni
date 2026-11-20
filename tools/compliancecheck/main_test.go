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
			ID:         "popia-s10",
			Instrument: "POPIA s10",
			ClassIDs:   []string{"test"},
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
				System:   "postgres",
				Database: "identity",
				Table:    "users",
				Columns:  []string{"email"},
			}},
			Retention: retention{
				Kind:     "fixed_days",
				Days:     &days,
				Evidence: []string{"evidence.txt"},
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
