package wal

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"time"
)

type (
	writer struct {
		maxSegmentSize int
		curSegmentSize int
		dir            string
		file           *os.File
	}
)

func NewFsWriter(dir string, segmentSize int) *writer {
	return &writer{dir: dir, maxSegmentSize: segmentSize}
}

func (w *writer) Close() error {
	return w.file.Close()
}

func (w *writer) Write(logs []Log) error {
	if w.file == nil {
		err := w.rotate()
		if err != nil {
			return err
		}
	}

	if w.curSegmentSize >= w.maxSegmentSize {
		err := w.rotate()
		if err != nil {
			return err
		}
	}

	var buf bytes.Buffer
	gb := gob.NewEncoder(&buf)
	err := gb.Encode(logs)
	if err != nil {
		return err
	}
	written, err := w.file.Write(buf.Bytes())
	if err != nil {
		return err
	}

	err = w.file.Sync()
	if err != nil {
		return err
	}

	w.curSegmentSize += written
	return err
}

func (w *writer) rotate() error {
	if w.file != nil {
		err := w.file.Sync()
		if err != nil {
			return err
		}
		err = w.file.Close()
		if err != nil {
			return err
		}
	}
	name := fmt.Sprintf("%s/wal_%d.log", w.dir, time.Now().UnixNano())

	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.curSegmentSize = 0
	w.file = file
	return nil
}
