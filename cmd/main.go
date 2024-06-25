package main

import (
	"aliansys/inMemDBExperiment/internal/compute"
	"aliansys/inMemDBExperiment/internal/config"
	"aliansys/inMemDBExperiment/internal/network"
	"aliansys/inMemDBExperiment/internal/storage"
	"aliansys/inMemDBExperiment/internal/storage/memory"
	"aliansys/inMemDBExperiment/internal/storage/replication"
	"aliansys/inMemDBExperiment/internal/storage/wal"
	"aliansys/inMemDBExperiment/internal/utils"
	"context"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := config.Get("")
	if err != nil {
		panic(err)
	}

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	maxSegmentSize, err := utils.ParseSize(cfg.WAL.MaxSegmentSize)
	if err != nil {
		panic(err)
	}

	reader := wal.NewReader(cfg.WAL.DataDir)
	writer := wal.NewFsWriter(cfg.WAL.DataDir, maxSegmentSize)

	restoreDataPipe := make(chan []wal.Log)
	defer func() {
		close(restoreDataPipe)
	}()

	lastSegmentName, err := reader.LastSegmentName()
	if err != nil {
		logger.Error("Failed to get last segment name", zap.Error(err))
	}

	replica, err := replication.New(replication.Config{
		Host:         cfg.Replication.Host,
		Addresses:    cfg.Replication.Cluster,
		SyncInterval: cfg.Replication.SyncInterval,
	}, writer, reader, lastSegmentName, restoreDataPipe, nil, nil, logger)

	if err != nil {
		panic(err)
	}

	defer replica.Close()

	parser := compute.NewParser(logger)
	analyzer := compute.NewAnalyzer(logger)

	computer := compute.New(parser, analyzer, logger)
	walJournal := wal.New(
		wal.Config{
			BatcherConfig: wal.BatcherFlushConfig{
				Size:    cfg.WAL.FlushingBatchSize,
				Timeout: cfg.WAL.FlushingBatchTimeout,
			},
		},
		writer,
		reader,
		restoreDataPipe,
		logger,
	)

	if replica != nil {
		if replica.IsMaster() {
			go walJournal.Start()
			defer walJournal.Close()
		}
	} else {
		go walJournal.Start()
		defer walJournal.Close()
	}

	mem := memory.New(logger, restoreDataPipe)

	db := storage.New(mem, walJournal, replica)
	server, err := network.NewTCPServer(cfg.Network.Address, cfg.Network.MaxConnections, logger)
	if err != nil {
		logger.Error("failed to create TCP server", zap.Error(err))
		return
	}

	ctx := context.Background()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
	err = server.Listen()
	if err != nil {
		panic(err)
	}

	defer func() {
		err = server.Close()
		if err != nil {
			logger.Error("failed to close server", zap.Error(err))
		}

		logger.Info("server closed")
	}()

	go func() {
		err = server.Serve(ctx, func(ctx context.Context, msg []byte) ([]byte, error) {
			logger.Debug("[handler] received message", zap.String("message", string(msg)))

			query, err := computer.HandleParse(ctx, string(msg))
			if err != nil {
				return nil, err
			}

			val, err := db.Process(query)
			if err != nil {
				return []byte(err.Error()), nil
			}

			return []byte(val), nil
		})

		if err != nil {
			logger.Error("failed to start server", zap.Error(err))
		}
	}()

	<-quit
}
