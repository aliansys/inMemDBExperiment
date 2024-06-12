package wal

import (
	"errors"
	"fmt"
	"testing"
)

func Test_writer_Write(t *testing.T) {
	type fields struct {
		maxSegmentSize int
	}
	type args struct {
		logs []Log
	}

	dir := "./testdata"

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "ok",
			fields: fields{
				maxSegmentSize: 1,
			},
			args: args{
				logs: []Log{
					{
						Id:        1,
						CommandId: 2,
						Args:      []string{"test_key", "test_val"},
					},
					{
						Id:        2,
						CommandId: 2,
						Args:      []string{"test_key", "test_val"},
					},
					{
						Id:        3,
						CommandId: 2,
						Args:      []string{"test_key", "test_val"},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := NewFsWriter(dir, tt.fields.maxSegmentSize)
			if err := w.Write(tt.args.logs); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := w.Write(tt.args.logs); (err != nil) != tt.wantErr {
				t.Errorf("Write() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err := w.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}

		})
	}
}

type (
	fsWriterMock struct{}
)

func (w *fsWriterMock) Write(logs []Log) error {
	for i := range logs {
		fmt.Printf("log[%d]: %+v\n", i, logs[i])
	}

	if len(logs) > 0 && len(logs[0].Args) == 2 && logs[0].Args[1] == "err" {
		return errors.New("test error")
	}

	return nil
}
