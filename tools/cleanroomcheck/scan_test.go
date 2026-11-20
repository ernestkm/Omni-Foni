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
