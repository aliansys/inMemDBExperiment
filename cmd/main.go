package main

import (
	"aliansys/inMemDBExperiment/internal/compute"
	"aliansys/inMemDBExperiment/internal/config"
	"aliansys/inMemDBExperiment/internal/network"
	"aliansys/inMemDBExperiment/internal/storage"
	"aliansys/inMemDBExperiment/internal/storage/memory"
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
	mem := memory.New(logger)
	db := storage.New(mem)

	server, err := network.NewTCPServer(cfg.Network.Address, cfg.Network.MaxConnections, logger)
	if err != nil {
		logger.Error("failed to create TCP server", zap.Error(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

			query, err := computer.HandleParse(context.Background(), string(msg))
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
	cancel()

	return
}
