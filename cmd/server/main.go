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
	"in-memory-key-value-db/internal/database/storage/snapshots"
	"in-memory-key-value-db/internal/database/storage/wal"
	"in-memory-key-value-db/internal/logger"

	"go.uber.org/zap"
)

func openOSFile(name string, flag int, perm os.FileMode) (wal.FileInterface, error) {
	return os.OpenFile(name, flag, perm)
}

func main() {
	cfg := config.MustLoad()
	log, err := logger.SetupLogger(cfg)
	if err != nil {
		if _, err := fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err); err != nil {
			os.Exit(1)
		}
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	changeChan := make(chan struct{}, 100)
	forcedFlush := make(chan struct{}, 1)
	expiryEvent := make(chan expiry.ExpiryEvent, 100)

	go func() {
		sig := <-sigChan
		defer close(changeChan)
		defer close(expiryEvent)
		defer close(forcedFlush)
		log.Info("got os signal", zap.String("signal", sig.String()))
		cancel()
	}()

	defer signal.Stop(sigChan)

	// нужно при резком завершении или если паника то лог все равно выведется , будет принудительный flush буфферов
	// sync может вернуть ошибку, мы ее игнорируем
	defer func() {
		_ = log.Sync()
	}()

	// WAL
	walLog, err := wal.NewWAL(cfg, log, openOSFile)
	if err != nil {
		log.Error("error to create WAL", zap.Error(err))
		return
	}

	// Engine
	cache := inmemory.NewCache(&cfg.Cache)
	engine := inmemory.NewHashBasedPartitionMapEngine(ctx, cache, walLog, log)

	// REPLAY DATA WITH ENGINE
	if err := walLog.Replay(engine.Apply); err != nil {
		log.Error("WAL is corrupted", zap.Error(err))
		return
	}
	walLog.Start(ctx, forcedFlush)

	// Storage
	store := storage.NewStorage(engine, log)

	// Compute
	comp := compute.NewCompute(store, log, walLog, changeChan, expiryEvent)

	// DUMPER
	dumper := snapshots.NewHashBasedPartitionDumper(cfg.Engine.Snapshot.DataDir, cfg.Engine.Snapshot.DBFileName, engine)

	// SNAPSHOT
	snapshotWorker := snapshots.NewSnapshot(dumper, log)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("catch panic from goroutine",
					zap.Any("recovered", r),
				)
			}
		}()
		snapshotWorker.Fork(ctx, cfg, changeChan, forcedFlush)
	}()

	// Expiry
	expiryWorker := expiry.NewWorker(log, expiryEvent)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("catch panic from goroutine",
					zap.Any("recovered", r),
				)
			}
		}()
		expiryWorker.Run(ctx, engine)
	}()

	server, err := network.NewTCPServer(cfg, log)
	if err != nil {
		log.Error("failed to create new tcp server", zap.Error(err))
		return
	}

	handler := func(request []byte) []byte {
		response, err := comp.Handle(string(request))
		if err != nil {
			return fmt.Appendf(nil, "err: %v", err)
		}
		return []byte(response)
	}
	if err := server.Start(ctx, handler); err != nil {
		log.Error("server stopped with error", zap.Error(err))
	}
	log.Info("server stopped")
}
