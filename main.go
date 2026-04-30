package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"in-memory-key-value-db/internal/database/compute"
	"in-memory-key-value-db/internal/database/storage"
	inmemory "in-memory-key-value-db/internal/database/storage/in_memory"

	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	engine := inmemory.NewEngine()
	store := storage.NewStorage(engine, logger)
	comp := compute.NewCompute(store, logger)

	logger.Info("database started, type SET / GET / DEL or 'exit' to quit")

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if trimmed == "exit" || trimmed == "quit" {
			logger.Info("shutting down")
			return
		}

		if trimmed == "" {
			fmt.Print("> ")
			continue
		}

		response, err := comp.Handle(line)
		if err != nil {
			if errors.Is(err, storage.ErrKeyNotFound) {
				fmt.Println("(nil)")
			} else {
				fmt.Printf("ERR: %v\n", err)
			}
		} else {
			fmt.Println(response)
		}

		fmt.Print("> ")
	}

	if err := scanner.Err(); err != nil {
		logger.Error("scanner failed", zap.Error(err))
	}
}
