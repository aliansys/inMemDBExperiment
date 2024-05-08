package memory

import (
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

func New(logger *zap.Logger) *storage {
	return &storage{
		data:   make(map[string]string),
		m:      new(sync.RWMutex),
		logger: logger,
	}
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
