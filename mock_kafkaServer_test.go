package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

const MOCK_KAFKA_SERVER_ADDR = "localhost:49090"

type MockKafkaServer struct {
	listener net.Listener
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewMockKafkaServer(t *testing.T) *MockKafkaServer {
	ctx, cancel := context.WithCancel(context.Background())

	listener, err := net.Listen("tcp", MOCK_KAFKA_SERVER_ADDR)
	if err != nil {
		t.Fatalf("Failed to create mock Kafka server: %v", err)
	}

	server := &MockKafkaServer{
		listener: listener,
		ctx:      ctx,
		cancel:   cancel,
	}

	server.wg.Add(1)
	go server.serve()

	return server
}

func (m *MockKafkaServer) serve() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			conn, err := m.listener.Accept()
			if err != nil {
				if m.ctx.Err() != nil {
					// Server is shutting down
					return
				}
				fmt.Printf("Accept error: %v\n", err)
				continue
			}

			m.wg.Add(1)
			go func() {
				defer m.wg.Done()
				m.handleConnection(conn)
			}()
		}
	}
}

func (m *MockKafkaServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
			// Set a read deadline to prevent hanging
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))

			// Read exactly 4 bytes for the message size
			sizeBuf := make([]byte, 4)
			_, err := io.ReadFull(conn, sizeBuf)
			if err != nil {
				if err != io.EOF {
					fmt.Printf("Error reading size: %v\n", err)
				}
				return
			}

			// Calculate message size
			messageSize := uint32(sizeBuf[0])<<24 | uint32(sizeBuf[1])<<16 | uint32(sizeBuf[2])<<8 | uint32(sizeBuf[3])

			// Read the actual message
			messageBuf := make([]byte, messageSize)
			_, err = io.ReadFull(conn, messageBuf)
			if err != nil {
				fmt.Printf("Error reading message: %v\n", err)
				return
			}

			// Create and send response
			response := createKafkaResponse()
			respSize := uint32(len(response))

			// Set write deadline
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

			// Write response size
			conn.Write([]byte{
				byte(respSize >> 24),
				byte(respSize >> 16),
				byte(respSize >> 8),
				byte(respSize),
			})

			// Write response
			_, err = conn.Write(response)
			if err != nil {
				fmt.Printf("Error writing response: %v\n", err)
				return
			}
		}
	}
}

func createKafkaResponse() []byte {
	return []byte{
		0x00, 0x00, 0x00, 0x01, // Correlation ID
		0x00, 0x00, // Error Code (0 = no error)
		0x00, 0x00, 0x00, 0x01, // Broker ID
		0x00, 0x00, 0x00, 0x00, // Controller ID
		0x00, 0x00, // Topic array length (0)
	}
}

func (m *MockKafkaServer) Close() {
	m.cancel()         // Signal all goroutines to stop
	m.listener.Close() // Close listener
	m.wg.Wait()        // Wait for all connections to finish
}
