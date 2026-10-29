package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func insideRoot(root, path string) (string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve artifact root: %w", err)
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve artifact path: %w", err)
	}
	relative, err := filepath.Rel(root, absolute)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || len(relative) > 2 && relative[:3] == ".."+string(filepath.Separator) {
		return "", errors.New("trace and metadata outputs must remain inside artifact root")
	}
	current := root
	components := strings.Split(relative, string(filepath.Separator))
	for index, component := range components {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("inspect artifact path: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("trace, metadata, and data paths must not traverse symlinks")
		}
		if index < len(components)-1 && !info.IsDir() {
			return "", errors.New("trace, metadata, and data paths must use directory components")
		}
	}
	return absolute, nil
}
