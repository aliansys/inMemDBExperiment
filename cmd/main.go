package main

import (
	"aliansys/inMemDBExperiment/internal/compute"
	"aliansys/inMemDBExperiment/internal/config"
	"aliansys/inMemDBExperiment/internal/network"
	"aliansys/inMemDBExperiment/internal/storage"
	"aliansys/inMemDBExperiment/internal/storage/memory"
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

	parser := compute.NewParser(logger)
	analyzer := compute.NewAnalyzer(logger)

	computer := compute.New(parser, analyzer, logger)
	restoreDataPipe := make(chan []wal.Log)
	defer func() {
		close(restoreDataPipe)
	}()

	maxSegmentSize, err := utils.ParseSize(cfg.WAL.MaxSegmentSize)
	if err != nil {
		panic(err)
	}

	reader := wal.NewReader(cfg.WAL.DataDir)
	writer := wal.NewFsWriter(cfg.WAL.DataDir, maxSegmentSize)

	walJournal := wal.New(wal.Config{
		BatcherConfig: wal.BatcherFlushConfig{
			Size:    cfg.WAL.FlushingBatchSize,
			Timeout: cfg.WAL.FlushingBatchTimeout,
		},
	},
		writer,
		reader,
		restoreDataPipe, logger)
	defer walJournal.Close()

	go walJournal.Start()
	mem := memory.New(logger, restoreDataPipe)

	db := storage.New(mem, walJournal)

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
		return
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
