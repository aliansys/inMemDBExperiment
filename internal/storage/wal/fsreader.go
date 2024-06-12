package wal

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type reader struct {
	dir string
}

func NewReader(dir string) *reader {
	return &reader{
		dir: dir,
	}
}

func (r *reader) Read() ([]Log, error) {
	var logs []Log

	err := filepath.WalkDir(r.dir, func(path string, info os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk dir: %w", err)
		}

		if info.IsDir() {
			return nil
		}

		content, err := os.ReadFile(path)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return fmt.Errorf("read file %s: %w", path, err)
			}
		}

		r := bytes.NewReader(content)
		gb := gob.NewDecoder(r)

		var batch []Log
		err = gb.Decode(&batch)
		if err != nil {
			return fmt.Errorf("decode log: %w", err)
		}

		logs = append(logs, batch...)
		return nil
	})

	return logs, err
}
