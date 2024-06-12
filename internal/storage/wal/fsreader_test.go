package wal

import (
	"testing"
)

func Test_reader_Read(t *testing.T) {
	r := NewReader("testdata")
	got, err := r.Read()
	if err != nil {
		t.Errorf("Read() error = %v", err)
		return
	}
	if len(got) == 0 {
		t.Errorf("Read() files must be read. read: %d", len(got))
	}
}
