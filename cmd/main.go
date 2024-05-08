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
	"os/signal"
	"syscall"
)

func main() {
	quit := make(chan os.Signal, 1)

	go func() {
		err := read(quit)
		if err != nil {
			fmt.Println(err)
			return
		}
	}()

	signal.Notify(quit, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-quit
}

func read(quit chan os.Signal) error {
	reader := bufio.NewReader(os.Stdin)
	zp, err := zap.NewProduction()
	if err != nil {
		return err
	}

	parser := compute.NewParser(zp)
	analyzer := compute.NewAnalyzer(zp)

	computer := compute.New(parser, analyzer, zp)
	mem := memory.New(zp)
	db := storage.New(mem)

	for {
		select {
		case <-quit:
			return nil
		default:
		}

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
