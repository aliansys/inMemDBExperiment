package wal

import (
	"aliansys/inMemDBExperiment/internal/compute"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockFsWriter struct {
	mock.Mock
}

func (m *mockFsWriter) Write(logs []Log) error {
	args := m.Called(logs)
	return args.Error(0)
}

func TestBatcher_AddAndFlushBySize(t *testing.T) {
	writer := new(mockFsWriter)
	writer.On("Write", mock.Anything).Return(nil)

	cfg := BatcherFlushConfig{
		Size:    2,
		Timeout: 30 * time.Second,
	}

	b := newBatcher(cfg, writer)
	go b.start()

	log1 := Log{
		Id:        1,
		CommandId: compute.CommandSet,
		Args:      []string{"1", "1"},
	}
	log2 := Log{
		Id:        1,
		CommandId: compute.CommandSet,
		Args:      []string{"2", "2"},
	}

	ch1 := b.add(log1)
	ch2 := b.add(log2)

	// Wait for flush to complete
	<-ch1
	<-ch2

	writer.AssertCalled(t, "Write", []Log{log1, log2})
	b.Close()
}

func TestBatcher_FlushByTimeout(t *testing.T) {
	writer := new(mockFsWriter)
	writer.On("Write", mock.Anything).Return(nil)

	cfg := BatcherFlushConfig{
		Size:    10,
		Timeout: 1 * time.Second,
	}

	b := newBatcher(cfg, writer)
	go b.start()

	log := Log{
		Id:        1,
		CommandId: compute.CommandSet,
		Args:      []string{"1", "1"},
	}
	ch := b.add(log)

	// Wait for flush to complete
	<-ch

	writer.AssertCalled(t, "Write", []Log{log})
	b.Close()
}

func TestBatcher_Close(t *testing.T) {
	writer := new(mockFsWriter)
	writer.On("Write", mock.Anything).Return(nil)

	cfg := BatcherFlushConfig{
		Size:    10,
		Timeout: 5 * time.Second,
	}

	b := newBatcher(cfg, writer)
	go b.start()

	log := Log{
		Id:        1,
		CommandId: compute.CommandSet,
		Args:      []string{"1", "1"},
	}
	b.add(log)

	b.Close()

	writer.AssertCalled(t, "Write", []Log{log})
}

func TestBatcher_AddErrorHandling(t *testing.T) {
	writer := new(mockFsWriter)
	writer.On("Write", mock.Anything).Return(errors.New("write error"))

	cfg := BatcherFlushConfig{
		Size:    1,
		Timeout: 5 * time.Second,
	}

	b := newBatcher(cfg, writer)
	go b.start()

	log := Log{
		Id:        1,
		CommandId: compute.CommandSet,
		Args:      []string{"1", "1"},
	}
	ch := b.add(log)

	err := <-ch

	assert.Error(t, err)
	assert.Equal(t, "write error", err.Error())
	b.Close()
}
