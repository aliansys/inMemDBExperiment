package replication

import (
	"aliansys/inMemDBExperiment/internal/network"
	"aliansys/inMemDBExperiment/internal/storage/wal"
	"context"
	"go.uber.org/zap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock implementation for walWriter
type mockWalWriter struct {
	mock.Mock
}

func (m *mockWalWriter) SaveSegment(name string, data []byte) error {
	args := m.Called(name, data)
	return args.Error(0)
}

func (m *mockWalWriter) SegmentNameLength() int {
	args := m.Called()
	return args.Int(0)
}

// Mock implementation for walReader
type mockWalReader struct {
	mock.Mock
}

func (m *mockWalReader) Decode(content []byte) ([]wal.Log, error) {
	args := m.Called(content)
	return args.Get(0).([]wal.Log), args.Error(1)
}

func (m *mockWalReader) NextSegmentName(lastSegmentName string) (string, error) {
	args := m.Called(lastSegmentName)
	return args.String(0), args.Error(1)
}

func (m *mockWalReader) ReadSegment(segmentName string) ([]byte, error) {
	args := m.Called(segmentName)
	return args.Get(0).([]byte), args.Error(1)
}

// Mock implementation for network.TCPClient
type mockTCPClient struct {
	mock.Mock
}

func (m *mockTCPClient) Send(data []byte) (string, error) {
	args := m.Called(data)
	return args.String(0), args.Error(1)
}

func (m *mockTCPClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Mock implementation for network.TCPServer
type mockTCPServer struct {
	mock.Mock
}

func (m *mockTCPServer) Listen() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockTCPServer) Serve(ctx context.Context, handler network.TCPHandler) error {
	args := m.Called(ctx, handler)
	return args.Error(0)
}

func TestNewReplication(t *testing.T) {
	mockWriter := new(mockWalWriter)
	mockReader := new(mockWalReader)
	logger := zap.NewNop()
	restoreDataPipe := make(chan []wal.Log, 1)
	config := Config{
		Host:         "localhost:7777",
		Addresses:    []string{"localhost:7778", "localhost:7779"},
		SyncInterval: time.Second,
	}

	rep, err := New(config, mockWriter, mockReader, "segment_0001", restoreDataPipe, nil, nil, logger)
	assert.NoError(t, err)
	assert.NotNil(t, rep)
}

func TestReplication_IsMaster(t *testing.T) {
	mockWriter := new(mockWalWriter)
	mockReader := new(mockWalReader)
	logger := zap.NewNop()
	restoreDataPipe := make(chan []wal.Log, 1)
	config := Config{
		Host:         "localhost:7777",
		Addresses:    []string{"localhost:7778", "localhost:7779"},
		SyncInterval: time.Second,
	}

	rep, err := New(config, mockWriter, mockReader, "segment_0001", restoreDataPipe, nil, nil, logger)
	assert.NoError(t, err)
	assert.NotNil(t, rep)

	rep.rtype = master
	assert.True(t, rep.IsMaster())

	rep.rtype = slave
	assert.False(t, rep.IsMaster())
}

func TestReplication_Synchronize(t *testing.T) {
	mockWriter := new(mockWalWriter)
	mockReader := new(mockWalReader)
	mockClient := new(mockTCPClient)
	mockServer := new(mockTCPServer)
	logger := zap.NewNop()
	restoreDataPipe := make(chan []wal.Log, 1)
	config := Config{
		Id:           1,
		Host:         "localhost:7777",
		Addresses:    []string{"localhost:7777", "localhost:7778"},
		SyncInterval: time.Second,
	}

	mockServer.On("Listen", mock.Anything, mock.Anything).Return(nil)
	mockServer.On("Serve", mock.Anything, mock.Anything).Return(nil, nil)
	mockClient.On("Send", buildSyncSegmentRequest("segment_0001")).Return("segment_0002111", nil)
	mockClient.On("Send", buildHostInfo(config.Id, config.Host)).Return("2", nil)
	mockReader.On("Decode", mock.Anything).Return([]wal.Log{}, nil)
	mockWriter.On("SegmentNameLength", mock.Anything).Return(len("segment_0002"))
	mockWriter.On("SaveSegment", "segment_0002", mock.Anything).Return(nil)

	rep, err := New(config, mockWriter, mockReader, "segment_0001", restoreDataPipe,
		func(s string) (tcpClient, error) {
			return mockClient, nil
		},
		func() (tcpServer, error) {
			return mockServer, nil
		},
		logger)
	assert.NoError(t, err)
	assert.NotNil(t, rep)
	rep.master = mockClient

	go rep.synchronize()

	select {
	case <-time.After(2 * time.Second):
	case logs := <-restoreDataPipe:
		assert.Empty(t, logs)
	}

	mockClient.AssertExpectations(t)
	mockReader.AssertExpectations(t)
	mockWriter.AssertExpectations(t)
	mockServer.AssertExpectations(t)
}
