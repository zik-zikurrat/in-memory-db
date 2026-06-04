package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"in-memory-key-value-db/internal/database/network"
)

func main() {
	address := flag.String("address", "127.0.0.1:3223", "server address")
	idleTimeout := flag.Duration("idle-timeout", 5*time.Minute, "idle timeout")
	maxMessageSize := flag.Int("max-message-size", 4096, "max message size")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Fprintln(os.Stderr, "got signal:", sig)
		cancel()
	}()

	defer signal.Stop(sigChan)

	fmt.Println("type SET / GET / DEL or 'exit' to quit")

	client, err := network.NewTCPClient(*address, *idleTimeout, *maxMessageSize)
	if err != nil {
		os.Exit(1)
	}
	defer client.Close()

	scanner := bufio.NewScanner(os.Stdin)

	inputCh := make(chan string)
	go func() {
		defer close(inputCh)

		for scanner.Scan() {
			select {
			case inputCh <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "scan error: %v\n", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("bye")
			return

		case line, ok := <-inputCh:
			if !ok {
				return
			}

			trimmed := strings.TrimSpace(line)

			if trimmed == "exit" || trimmed == "quit" {
				cancel()
				return
			}

			if trimmed == "" {
				fmt.Print("> ")
				continue
			}
			response, err := client.Send([]byte(line))
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to connect: %v\n", err)
			} else {
				fmt.Println(string(response))
			}

			fmt.Print("> ")
		}
	}
}
