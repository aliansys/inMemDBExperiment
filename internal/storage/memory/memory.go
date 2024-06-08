package memory

import (
	"aliansys/inMemDBExperiment/internal/compute"
	"aliansys/inMemDBExperiment/internal/storage/wal"
	"go.uber.org/zap"
	"sync"
)

type (
	storage struct {
		data map[string]string
		m    *sync.RWMutex

		logger *zap.Logger
	}
)

func New(logger *zap.Logger, restoreDataPipe <-chan []wal.Log) *storage {
	s := &storage{
		data:   make(map[string]string),
		m:      new(sync.RWMutex),
		logger: logger,
	}

	go s.restore(restoreDataPipe)
	return s
}

func (s *storage) Get(key string) (string, bool) {
	s.m.RLock()
	defer s.m.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

func (s *storage) Set(key, value string) {
	s.m.Lock()
	defer s.m.Unlock()
	s.data[key] = value
}

func (s *storage) Del(key string) {
	s.m.Lock()
	defer s.m.Unlock()
	delete(s.data, key)
}

func (s *storage) restore(dataPipe <-chan []wal.Log) {
	for logs := range dataPipe {
		for _, log := range logs {
			switch log.CommandId {
			case compute.CommandSet:
				s.Set(log.Args[0], log.Args[1])
			case compute.CommandDel:
				s.Del(log.Args[0])
			}
		}
	}
}
