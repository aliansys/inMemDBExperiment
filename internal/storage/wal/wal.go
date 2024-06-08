package wal

import (
	"aliansys/inMemDBExperiment/internal/compute"
	"go.uber.org/zap"
	"time"
)

type (
	Log struct {
		Id        int64
		CommandId compute.CommandID
		Args      []string
	}
	wal struct {
		config  Config
		batcher *batcher
		reader  fsReader
		logger  *zap.Logger
	}

	fsReader interface {
		Read() ([]Log, error)
	}

	Config struct {
		BatcherConfig BatcherFlushConfig
	}
)

func New(c Config, wr fsWriter, r fsReader, pipe chan<- []Log, l *zap.Logger) *wal {
	w := &wal{
		config:  c,
		batcher: newBatcher(c.BatcherConfig, wr),
		reader:  r,
		logger:  l,
	}

	go w.restore(pipe)

	return w
}

func (w *wal) Start() {
	w.batcher.start()
}

func (w *wal) Set(key, value string) chan error {
	return w.batcher.add(Log{
		Id:        time.Now().UnixNano(),
		CommandId: compute.CommandSet,
		Args:      []string{key, value},
	})
}

func (w *wal) Del(key string) chan error {
	return w.batcher.add(Log{
		Id:        time.Now().UnixNano(),
		CommandId: compute.CommandDel,
		Args:      []string{key},
	})
}

func (w *wal) restore(pipe chan<- []Log) {
	logs, err := w.reader.Read()
	if err != nil {
		w.logger.Error("wal. failed to restore data", zap.Error(err))
		return
	}

	pipe <- logs
}

func (w *wal) Close() {
	w.batcher.Close()
}
