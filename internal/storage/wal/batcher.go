package wal

import (
	"sync"
	"time"
)

type (
	BatcherFlushConfig struct {
		Size    int
		Timeout time.Duration
	}

	fsWriter interface {
		Write([]Log) error
	}

	batcher struct {
		config BatcherFlushConfig

		fsWriter fsWriter

		batch     []Log
		mustFlush chan struct{}
		waiting   []chan error

		m       *sync.Mutex
		closing chan struct{}
		closed  chan struct{}
	}
)

func newBatcher(cfg BatcherFlushConfig, writer fsWriter) *batcher {
	return &batcher{
		config:    cfg,
		fsWriter:  writer,
		batch:     make([]Log, 0, cfg.Size),
		mustFlush: make(chan struct{}),
		closing:   make(chan struct{}),
		closed:    make(chan struct{}),
		m:         &sync.Mutex{},
	}
}

func (b *batcher) start() {
	ticker := time.NewTicker(b.config.Timeout)
	for {
		var err error
		select {
		case <-ticker.C:
			err = b.flush()
		case <-b.mustFlush:
			ticker.Reset(b.config.Timeout)
			err = b.flush()
		case <-b.closing:
			_ = b.flush()
			ticker.Stop()

			close(b.closed)
			return
		}

		for i := range b.waiting {
			b.waiting[i] <- err
			close(b.waiting[i])
		}
		b.waiting = b.waiting[:0]
	}
}

func (b *batcher) add(log Log) chan error {
	ch := make(chan error)

	b.m.Lock()
	defer b.m.Unlock()

	b.waiting = append(b.waiting, ch)
	b.batch = append(b.batch, log)

	if len(b.batch) == b.config.Size {
		b.mustFlush <- struct{}{}
	}

	return ch
}

func (b *batcher) flush() error {
	b.m.Lock()
	defer b.m.Unlock()

	if len(b.batch) == 0 {
		return nil
	}

	err := b.fsWriter.Write(b.batch)
	b.batch = b.batch[:0]
	return err
}

func (b *batcher) Close() {
	close(b.closing)
	<-b.closed
}
