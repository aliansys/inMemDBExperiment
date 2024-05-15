package main

import (
	"aliansys/inMemDBExperiment/internal/compute"
	"aliansys/inMemDBExperiment/internal/storage"
	"aliansys/inMemDBExperiment/internal/storage/memory"
	"bufio"
	"context"
	"fmt"
	"go.uber.org/zap"
	"os"
)

func main() {
	err := read()
	if err != nil {
		fmt.Println(err)
		return
	}
}

func read() error {
	reader := bufio.NewReader(os.Stdin)
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}

	parser := compute.NewParser(logger)
	analyzer := compute.NewAnalyzer(logger)

	computer := compute.New(parser, analyzer, logger)
	mem := memory.New(logger)
	db := storage.New(mem)

	for {
		fmt.Print("> ")
		text, err := reader.ReadString('\n')
		if err != nil {
			return err
		}

		query, err := computer.HandleParse(context.Background(), text)
		if err != nil {
			fmt.Println("parse error:", err)
			continue
		}

		val, err := db.Process(query)
		if err != nil {
			fmt.Println("query process error:", err)
			continue
		}

		if len(val) > 0 {
			fmt.Println("query result:", val)
		}
	}
}
