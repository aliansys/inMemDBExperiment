package wal

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
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

		batch, err := r.Decode(content)
		if err != nil {
			return fmt.Errorf("decode log: %w", err)
		}

		logs = append(logs, batch...)
		return nil
	})

	return logs, err
}

func (r *reader) Decode(content []byte) ([]Log, error) {
	rd := bytes.NewReader(content)
	gb := gob.NewDecoder(rd)

	var batch []Log
	err := gb.Decode(&batch)
	if err != nil {
		return nil, fmt.Errorf("decode log: %w", err)
	}

	return batch, nil
}

func (r *reader) LastSegmentName() (string, error) {
	files, err := os.ReadDir(r.dir)
	if err != nil {
		return "", fmt.Errorf("read dir %s: %w", r.dir, err)
	}

	filename := ""
	for i := len(files) - 1; i >= 0; i-- {
		file := files[i]
		if file.IsDir() {
			continue
		}

		filename = file.Name()
		break
	}

	return filename, nil
}

func (r *reader) NextSegmentName(lastSegmentName string) (string, error) {
	files, err := os.ReadDir(r.dir)
	if err != nil {
		return "", fmt.Errorf("read dir %s: %r", r.dir, err)
	}

	filenames := make([]string, 0, len(files))
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filenames = append(filenames, file.Name())
	}

	idx, ok := slices.BinarySearch(filenames, lastSegmentName)
	if ok && len(filenames) > idx+1 {
		return filenames[idx+1], nil
	}

	return "", nil
}

func (r *reader) ReadSegment(segmentName string) ([]byte, error) {
	filename := fmt.Sprintf("%s/%s", r.dir, segmentName)
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	return content, nil
}
