package replication

import (
	"aliansys/inMemDBExperiment/internal/network"
	"aliansys/inMemDBExperiment/internal/storage/wal"
	"context"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"math"
	"strconv"
	"time"
)

type (
	walWriter interface {
		SaveSegment(name string, data []byte) error
		SegmentNameLength() int
	}

	walReader interface {
		Decode(content []byte) ([]wal.Log, error)
		NextSegmentName(lastSegmentName string) (string, error)
		ReadSegment(segmentName string) ([]byte, error)
	}

	tcpClient interface {
		Send([]byte) (string, error)
		Close() error
	}

	tcpServer interface {
		Listen() error
		Serve(ctx context.Context, handler network.TCPHandler) error
	}

	replication struct {
		config  Config
		id      int
		rtype   replicationType
		address string

		walWriter walWriter
		walReader walReader
		logger    *zap.Logger

		master  tcpClient
		cluster map[string]*host

		lastSegmentName string
		restoreDataPipe chan<- []wal.Log

		closeCh chan struct{}

		clientBuilder func(string) (tcpClient, error)
		serverBuilder func() (tcpServer, error)
	}

	host struct {
		id      int
		address string
		rtype   replicationType
		client  tcpClient
	}

	replicationType int

	Config struct {
		Id           int
		Host         string
		Addresses    []string
		SyncInterval time.Duration
	}
)

const (
	master    = 1
	candidate = 2
	slave     = 3
)

func New(cfg Config, walWriter walWriter, walDecoder walReader, lastSegmentName string,
	restoreDataPipe chan<- []wal.Log,
	clientBuilder func(string) (tcpClient, error),
	serverBuilder func() (tcpServer, error),
	logger *zap.Logger) (*replication, error) {
	if len(cfg.Addresses) == 0 {
		return nil, nil
	}

	rep := &replication{
		rtype:     candidate,
		walWriter: walWriter,
		walReader: walDecoder,
		logger:    logger,
		cluster:   make(map[string]*host),

		config:          cfg,
		lastSegmentName: lastSegmentName,
		restoreDataPipe: restoreDataPipe,

		closeCh: make(chan struct{}),
	}

	if cfg.Id == 0 {
		rep.id = int(time.Now().UnixNano())
	} else {
		rep.id = cfg.Id
	}

	if clientBuilder != nil {
		rep.clientBuilder = clientBuilder
	} else {
		rep.clientBuilder = func(host string) (tcpClient, error) {
			return network.NewTCPClient(host, network.WithReconnectMaxTries(3))
		}
	}

	if serverBuilder != nil {
		rep.serverBuilder = serverBuilder
	} else {
		rep.serverBuilder = func() (tcpServer, error) {
			return network.NewTCPServer(cfg.Host, 5, logger)
		}
	}

	err := rep.runServer()
	if err != nil {
		return nil, err
	}

	rep.selectMaster()

	if !rep.IsMaster() {
		go rep.synchronize()
	}

	// if !master run sync process

	return rep, nil
}

func (r *replication) IsMaster() bool {
	return r.rtype == master
}

func (r *replication) Close() {
	close(r.closeCh)
}

func (r *replication) synchronize() {
	ticker := time.NewTicker(r.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.closeCh:
			return
		default:
		}

		select {
		case <-r.closeCh:
			return
		case <-ticker.C:
			r.sync()
		}
	}
}

func (r *replication) sync() {
	request := buildSyncSegmentRequest(r.lastSegmentName)
	responseRaw, err := r.master.Send(request)
	if err != nil {
		r.logger.Error("failed to send replication request", zap.Error(err))
		return
	}

	r.logger.Info("raw response", zap.String("response", string(responseRaw)))

	r.handleSaveSegmentData([]byte(responseRaw))
}

func (r *replication) broadcastHostInfo() {
	for _, h := range r.config.Addresses {
		if h == r.config.Host {
			continue
		}

		client, err := r.clientBuilder(h)
		if err != nil {
			continue
		}

		idRaw, err := client.Send(buildHostInfo(r.id, r.config.Host))
		if err != nil && errors.Is(err, io.EOF) {
			r.logger.Error("broadcastHostInfo()", zap.Error(err))
		}
		if err != nil {
			continue
		}

		parsed := parseId(idRaw)
		r.cluster[h] = &host{
			id:      parsed,
			address: h,
			client:  client,
		}
	}
}

func (r *replication) runServer() error {
	server, err := r.serverBuilder()
	if err != nil {
		return fmt.Errorf("new tcp server: %w", err)
	}

	err = server.Listen()
	if err != nil {
		return fmt.Errorf("server listen: %w", err)
	}

	go func() {
		err = server.Serve(context.Background(), func(ctx context.Context, msg []byte) ([]byte, error) {
			switch msg[0] {
			case ping: // todo. for checking if master is alive
				return []byte{pong}, nil
			case newHost:
				return r.handleNewHost(msg[1:])
			case syncSegment:
				return r.handleRequestSegmentData(msg[1:])
			}

			return nil, errors.New("unknown message")
		})
	}()

	if err != nil {
		return err
	}

	return nil
}

func (r *replication) selectMaster() {
	r.broadcastHostInfo()

	var masterHost string
	minId := r.id
	for _, host := range r.cluster {
		if host.id < minId {
			minId = host.id
			masterHost = host.address
			continue
		}
	}

	// todo acknowledge mastership.
	// 		due to network lags and other possible issues it's better to check if a mastership was chosen right
	if r.id == minId {
		r.rtype = master
		r.logger.Info("I am the master", zap.Int("id", r.id))
	} else {
		r.rtype = slave
		r.cluster[masterHost].rtype = master
		r.master = r.cluster[masterHost].client
		r.logger.Info("I am a slave", zap.Int("id", r.id), zap.String("masterHost", masterHost))
	}
}

func (r *replication) handleNewHost(msg []byte) ([]byte, error) {
	r.logger.Info("handle new host", zap.String("msg", string(msg)))
	host, err := parseHostInfo(r.id, msg)
	if err != nil {
		return nil, err
	}

	if r.cluster[host.address] != nil {
		_ = r.cluster[host.address].client.Close()
	}

	client, err := r.clientBuilder(host.address)
	if err != nil {
		return nil, err
	}

	host.client = client
	r.cluster[host.address] = host
	response := strconv.Itoa(r.id)
	return []byte(response), nil
}

func (r *replication) handleRequestSegmentData(msg []byte) ([]byte, error) {
	r.logger.Info("handle request segment data", zap.String("msg", string(msg)))
	lastName := string(msg)
	newLastSegment, err := r.walReader.NextSegmentName(lastName)
	if err != nil {
		return nil, err
	}
	if newLastSegment == "" {
		return nil, nil
	}

	content, err := r.walReader.ReadSegment(newLastSegment)
	if err != nil {
		return nil, err
	}

	return buildSegmentResponse(newLastSegment, content), nil
}

func (r *replication) handleSaveSegmentData(msg []byte) {
	r.logger.Info("handle save segment data", zap.String("msg", string(msg)))
	if len(msg) == 0 {
		return
	}

	nameLen := r.walWriter.SegmentNameLength()
	if len(msg) <= nameLen {
		return
	}

	name := string(msg[:nameLen])
	content := msg[nameLen:]

	logs, err := r.walReader.Decode(content)
	if err != nil {
		r.logger.Error("failed to decode replication response", zap.Error(err))
		return
	}

	err = r.walWriter.SaveSegment(name, content)
	if err != nil {
		r.logger.Error("failed to save segment", zap.String("name", name), zap.Error(err))
	}

	r.restoreDataPipe <- logs
}

func buildHostInfo(id int, host string) []byte {
	idMsg := []byte(strconv.Itoa(id))
	msg := append([]byte{newHost}, idMsg...)
	msg = append(msg, host...)
	return msg
}

func buildSyncSegmentRequest(lastSegmentName string) []byte {
	request := []byte{syncSegment}
	request = append(request, []byte(lastSegmentName)...)
	return request
}

func buildSegmentResponse(lastSegmentName string, content []byte) []byte {
	request := make([]byte, 0, len(lastSegmentName)+len(content))
	request = append(request, []byte(lastSegmentName)...)
	return append(request, content...)
}

func parseHostInfo(myId int, msg []byte) (*host, error) {
	gotId, err := strconv.Atoi(string(msg[:18]))
	if err != nil {
		return nil, fmt.Errorf("strconv.Atoi: %w", err)
	}
	if gotId == myId {
		return nil, nil
	}

	hostAddress := string(msg[19:])
	return &host{
		id:      gotId,
		address: hostAddress,
	}, nil
}

func parseId(raw string) int {
	id, err := strconv.Atoi(raw)
	if err != nil {
		return math.MaxInt
	}

	return id
}

type (
	messageType = byte
)

const (
	ping messageType = iota
	pong
	newHost
	syncSegment
)
