package network

import (
	"bufio"
	"context"
	"fmt"
	"in-memory-key-value-db/internal/config"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Handler func(request []byte) (response []byte)

type TCPServer struct {
	listener       net.Listener
	log            *zap.Logger
	idleTimeout    time.Duration
	bufferSize     int
	maxConnections int
}

func NewTCPServer(cfg *config.Config, log *zap.Logger) (*TCPServer, error) {
	listener, err := net.Listen("tcp", cfg.Engine.Network.Address)
	if err != nil {
		return nil, fmt.Errorf("Error while listen to tcp: %v", err.Error())
	}
	return &TCPServer{
		listener:       listener,
		log:            log,
		idleTimeout:    cfg.Engine.Network.IdleTimeout,
		bufferSize:     cfg.Engine.Network.MaxMessageSize,
		maxConnections: cfg.Engine.Network.MaxConnections,
	}, nil
}

func (s *TCPServer) Start(ctx context.Context, handler Handler) error {
	sem := make(chan struct{}, s.maxConnections)
	wg := sync.WaitGroup{}
	defer close(sem)

	go func() {
		<-ctx.Done()
		s.listener.Close()
	}()

	for {
		conn, err := s.listener.Accept()

		if err != nil {
			if ctx.Err() != nil {
				wg.Wait()
				return nil
			}
			s.log.Error("error while accepting connection", zap.String("error", err.Error()))
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			conn.Close()
			wg.Wait()
			return nil
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.handleConnection(conn, handler)
		}()
	}
}

func (s *TCPServer) handleConnection(conn net.Conn, handler Handler) {
	defer func() {
		if v := recover(); v != nil {
			s.log.Error("captured panic", zap.Any("panic", v))
		}
	}()
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	buf := make([]byte, s.bufferSize)
	scanner.Buffer(buf, s.bufferSize)
	for {
		conn.SetReadDeadline(time.Now().Add(s.idleTimeout))
		if !scanner.Scan() {
			break
		}
		line := scanner.Bytes()
		resp := handler(line)
		_, err := conn.Write(append(resp, '\n'))
		if err != nil {
			s.log.Error("Error while writing resp", zap.Error(err))
			break
		}
	}
	if err := scanner.Err(); err != nil {
		s.log.Warn("scanner error", zap.Error(err))
	}
}
