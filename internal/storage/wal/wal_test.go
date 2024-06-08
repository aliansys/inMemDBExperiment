package wal

import (
	"errors"
	"testing"
	"time"

	"aliansys/inMemDBExperiment/internal/compute"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

// mockFsReader is a mock implementation of fsReader interface for testing purposes
type mockFsReader struct {
	mock.Mock
}

func (m *mockFsReader) Read() ([]Log, error) {
	args := m.Called()
	return args.Get(0).([]Log), args.Error(1)
}

func setupLogger() *zap.Logger {
	return zap.NewNop()
}

func TestWAL_Set(t *testing.T) {
	writer := new(mockFsWriter)
	logger := setupLogger()

	writer.On("Write", mock.Anything).Return(nil)

	cfg := Config{
		BatcherConfig: BatcherFlushConfig{
			Size:    1,
			Timeout: 5 * time.Second,
		},
	}

	w := &wal{
		config:  cfg,
		batcher: newBatcher(cfg.BatcherConfig, writer),
		logger:  logger,
	}
	go w.Start()

	errChan := w.Set("key", "value")

	err := <-errChan

	assert.NoError(t, err)
	writer.AssertCalled(t, "Write", mock.Anything)
	w.Close()
}

func TestWAL_Del(t *testing.T) {
	writer := new(mockFsWriter)
	logger := setupLogger()

	writer.On("Write", mock.Anything).Return(nil)

	cfg := Config{
		BatcherConfig: BatcherFlushConfig{
			Size:    1,
			Timeout: 5 * time.Second,
		},
	}

	w := &wal{
		config:  cfg,
		batcher: newBatcher(cfg.BatcherConfig, writer),
		logger:  logger,
	}
	go w.Start()

	errChan := w.Del("key")

	err := <-errChan

	assert.NoError(t, err)
	writer.AssertCalled(t, "Write", mock.Anything)
	w.Close()
}

func TestWAL_Restore(t *testing.T) {
	reader := new(mockFsReader)
	logger := setupLogger()

	logs := []Log{
		{Id: 1, CommandId: compute.CommandSet, Args: []string{"key", "value"}},
	}

	reader.On("Read").Return(logs, nil)

	cfg := Config{
		BatcherConfig: BatcherFlushConfig{
			Size:    1,
			Timeout: 5 * time.Second,
		},
	}

	pipe := make(chan []Log)
	w := New(cfg, nil, reader, pipe, logger)
	go w.Start()

	restoredLogs := <-pipe

	assert.Equal(t, logs, restoredLogs)
	reader.AssertCalled(t, "Read")
	w.Close()
}

func TestWAL_RestoreError(t *testing.T) {
	reader := new(mockFsReader)
	logger := setupLogger()

	reader.On("Read").Return(make([]Log, 0), errors.New("read error"))

	cfg := Config{
		BatcherConfig: BatcherFlushConfig{
			Size:    1,
			Timeout: 5 * time.Second,
		},
	}

	pipe := make(chan []Log)
	w := New(cfg, nil, reader, pipe, logger)
	go w.Start()

	select {
	case <-pipe:
		t.Fatal("Expected no logs due to read error")
	case <-time.After(1 * time.Second):
		// Expected case
	}

	reader.AssertCalled(t, "Read")
	w.Close()
}

func TestWAL_Close(t *testing.T) {
	writer := new(mockFsWriter)
	reader := new(mockFsReader)
	logger := setupLogger()

	writer.On("Write", mock.Anything).Return(nil)
	reader.On("Read").Return(make([]Log, 0), nil)

	cfg := Config{
		BatcherConfig: BatcherFlushConfig{
			Size:    2,
			Timeout: 50 * time.Second,
		},
	}

	pipe := make(chan []Log)
	w := New(cfg, writer, reader, pipe, logger)
	go w.Start()

	w.Set("key", "value")
	w.Close()

	writer.AssertExpectations(t)
}
