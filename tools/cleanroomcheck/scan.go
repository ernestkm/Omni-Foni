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
