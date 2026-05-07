package network

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"time"
)

type TCPClient struct {
	connection  net.Conn
	reader      *bufio.Reader
	idleTimeout time.Duration
	bufferSize  int
}

func NewTCPClient(address string, idleTimeout time.Duration, bufferSize int) (*TCPClient, error) {
	connection, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}
	return &TCPClient{
		connection:  connection,
		reader:      bufio.NewReader(connection),
		idleTimeout: idleTimeout,
		bufferSize:  bufferSize,
	}, nil
}

func (c *TCPClient) Send(request []byte) ([]byte, error) {
	if err := c.connection.SetDeadline(time.Now().Add(c.idleTimeout)); err != nil {
		return nil, fmt.Errorf("set deadline: %w", err)
	}

	if _, err := c.connection.Write(append(request, '\n')); err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	resp, err := c.reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}

	return bytes.TrimRight(resp, "\n"), nil
}

func (c *TCPClient) Close() {
	if c.connection != nil {
		_ = c.connection.Close()
	}
}
