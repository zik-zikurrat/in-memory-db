package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"in-memory-key-value-db/internal/config"
	"in-memory-key-value-db/internal/database/compute"
	"in-memory-key-value-db/internal/database/network"
	"in-memory-key-value-db/internal/database/storage"
	"in-memory-key-value-db/internal/database/storage/expiry"
	inmemory "in-memory-key-value-db/internal/database/storage/in_memory"
	"in-memory-key-value-db/internal/database/storage/wal"
	"in-memory-key-value-db/internal/logger"

	"go.uber.org/zap"
)

func main() {
	cfg := config.MustLoad()
	logger, err := logger.SetupLogger(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	walEvents := make(chan wal.WALEvent, 100)
	expiryEvent := make(chan expiry.ExpiryEvent, 100)

	go func() {
		sig := <-sigChan
		logger.Info("got os signal", zap.String("signal", sig.String()))
		cancel()
	}()

	defer signal.Stop(sigChan)

	// нужно при резком завершении или если паника то лог все равно выведется , будет принудительный flush буфферов
	// sync может вернуть ошибку, мы ее игнорируем
	defer func() {
		_ = logger.Sync()
	}()

	// Enegine
	engine := inmemory.NewEngine()
	// Storage
	store := storage.NewStorage(engine, logger)
	// Compute
	comp := compute.NewCompute(store, logger, walEvents, expiryEvent)
	// WAL
	walWorker := wal.NewWorker(logger, walEvents)
	wal, err := wal.NewWAL(cfg, engine)
	if err != nil {
		logger.Error("error to create WAL", zap.Error(err))
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("catch panic from goroutine",
					zap.Any("recovered", r),
				)
			}
		}()
		walWorker.Run(ctx, wal)
	}()

	// Expiry
	expiryWorker := expiry.NewWorker(logger, expiryEvent)
	expiry := expiry.NewExpiry()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("catch panic from goroutine",
					zap.Any("recovered", r),
				)
			}
		}()
		expiryWorker.Run(ctx, expiry, engine)
	}()

	server, err := network.NewTCPServer(cfg, logger)
	if err != nil {
		logger.Error("failed to create new tcp server", zap.Error(err))
		return
	}

	handler := func(request []byte) []byte {
		response, err := comp.Handle(string(request))
		if err != nil {
			return []byte(fmt.Sprintf("ERR: %v", err))
		}
		return []byte(response)
	}
	if err := server.Start(ctx, handler); err != nil {
		logger.Error("server stopped with error", zap.Error(err))
	}
	logger.Info("server stopped")
}
