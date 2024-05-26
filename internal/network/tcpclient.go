package network

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

type (
	clientConfig struct {
		address           string
		reconnectMaxTries uint8
		timeout           time.Duration
	}

	TCPClient struct {
		config     clientConfig
		connection net.Conn
	}

	Option func(cfg *clientConfig)
)

func NewTCPClient(address string, options ...Option) (*TCPClient, error) {
	// set defaults
	cfg := &clientConfig{
		address: address,
		timeout: time.Second * 5,
	}

	for _, option := range options {
		option(cfg)
	}

	client := &TCPClient{
		config: *cfg,
	}

	err := client.connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return client, nil
}

func (c *TCPClient) connect() error {
	connection, err := net.Dial("tcp", c.config.address)
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	c.connection = connection
	return nil
}

func (c *TCPClient) possibleReconnect(err error) error {
	if !isErrReconnectable(err) || c.config.reconnectMaxTries == 0 {
		return err
	}

	try := 0
	for try < int(c.config.reconnectMaxTries) {
		try++
		err = c.connect()
		if err == nil {
			return nil
		}
	}

	if err != nil {
		return fmt.Errorf("failed to reconnect to server: %w", err)
	}

	return nil
}

func (c *TCPClient) Send(msg []byte) (string, error) {
	response, err := c.send(msg)
	if err != nil {
		if err := c.possibleReconnect(err); err != nil {
			return "", err
		}

		return c.send(msg)
	}

	return response, nil
}

func (c *TCPClient) send(msg []byte) (string, error) {
	err := c.connection.SetDeadline(time.Now().Add(c.config.timeout))
	if err != nil {
		return "", fmt.Errorf("failed to set deadline: %w", err)
	}

	err = writeMessage(c.connection, msg)
	if err != nil {
		return "", fmt.Errorf("failed to send message: %w", err)
	}

	payload, err := readMessage(c.connection)
	if err != nil {
		return "", fmt.Errorf("failed to read message: %w", err)
	}
	return string(payload), nil
}

func (c *TCPClient) Close() error {
	return c.connection.Close()
}

func isErrReconnectable(err error) bool {
	return errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "broken pipe")
}

func WithTimeout(timeout time.Duration) Option {
	return func(cfg *clientConfig) {
		cfg.timeout = timeout
	}
}

func WithReconnectMaxTries(maxTries uint8) Option {
	return func(cfg *clientConfig) {
		cfg.reconnectMaxTries = maxTries
	}
}
