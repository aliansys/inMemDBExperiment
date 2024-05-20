package main

import (
	"aliansys/inMemDBExperiment/internal/network"
	"bufio"
	"flag"
	"fmt"
	"go.uber.org/zap"
	"os"
	"time"
)

type (
	config struct {
		timeout           time.Duration
		address           string
		reconnectMaxTries uint8
	}
)

func main() {
	cfg := &config{}
	parseFlags(cfg)

	logger, _ := zap.NewProduction()
	reader := bufio.NewReader(os.Stdin)

	client, err := network.NewTCPClient(
		cfg.address,
		network.WithTimeout(cfg.timeout),
		network.WithReconnectMaxTries(cfg.reconnectMaxTries),
	)
	if err != nil {
		logger.Fatal("%s", zap.Error(err))
	}
	defer client.Close()

	for {
		fmt.Print("[client] > ")
		request, err := reader.ReadString('\n')
		if err != nil {
			logger.Error("failed to read user query", zap.Error(err))
			continue
		}

		response, err := client.Send([]byte(request))
		if err != nil {
			logger.Error("failed to send query", zap.Error(err))
			continue
		}

		fmt.Printf("\t(responce) > %s\n", response)
	}
}

func parseFlags(cfg *config) {
	address := flag.String("address", "localhost:3223", "Address of the server")
	timeout := flag.Duration("timeout", time.Second*5, "Timeout for connection")
	reconnectTries := flag.Int("reconnect_max_tries", 0, "Max tries for reconnection to server")
	flag.Parse()

	cfg.address = *address
	cfg.timeout = *timeout
	cfg.reconnectMaxTries = uint8(*reconnectTries)
}
