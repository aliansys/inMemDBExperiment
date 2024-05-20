package network

import (
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"net"
	"sync"
	"time"
)

type (
	serverConfig struct {
		address string
		maxConn int
		timeout time.Duration
	}

	TCPServer struct {
		semaphore chan struct{}
		config    serverConfig
		logger    *zap.Logger

		listener net.Listener
		closed   chan struct{}
	}

	serverOption func(config *serverConfig)
	TCPHandler   func(ctx context.Context, msg []byte) ([]byte, error)
)

func NewTCPServer(address string, maxConn int, logger *zap.Logger, opts ...serverOption) (*TCPServer, error) {
	cfg := &serverConfig{
		address: address,
		maxConn: maxConn,
		timeout: 30 * time.Second,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	return &TCPServer{
		config:    *cfg,
		semaphore: make(chan struct{}, maxConn),
		logger:    logger,
		closed:    make(chan struct{}),
	}, nil
}

func (s *TCPServer) Listen() error {
	listener, err := net.Listen("tcp", s.config.address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s.listener = listener
	return nil
}

func (s *TCPServer) Serve(ctx context.Context, handler TCPHandler) error {
	if s.listener == nil {
		return errors.New("server is not running")
	}
	// we will wait for all connections to finish
	wg := &sync.WaitGroup{}
	defer func() {
		wg.Wait()
		close(s.closed)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}

			s.logger.Error("Error accepting connection:", zap.Error(err))
			continue
		}

		s.semaphore <- struct{}{}
		wg.Add(1)
		go func(conn net.Conn) {
			defer func() {
				wg.Done()
				<-s.semaphore
			}()

			err := s.handle(ctx, conn, handler)
			if err != nil {
				return
			}
		}(conn)
	}
}

func (s *TCPServer) Close() error {
	err := s.listener.Close()
	if err != nil {
		s.logger.Error("Error closing listener", zap.Error(err))
	}

	<-s.closed

	close(s.semaphore)
	return err
}

func (s *TCPServer) handle(ctx context.Context, conn net.Conn, handler TCPHandler) error {
	defer conn.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		err := conn.SetDeadline(time.Now().Add(s.config.timeout))
		if err != nil {
			return fmt.Errorf("failed to set deadline: %w", err)
		}

		msg, err := readMessage(conn)
		if err != nil {
			return err
		}

		response, err := handler(ctx, msg)
		if err != nil {
			return err
		}

		err = writeMessage(conn, response)
		if err != nil {
			return err
		}
	}
}
