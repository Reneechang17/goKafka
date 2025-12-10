package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	APIKeyAPIVersions = 18
)

type Header struct {
	Size int32
	APIKey int16
	APIVersion int16
}

type APIVersion struct {
	CorrelationID int32
	ClientID string
	ClientSoftwareName string
	ClientSoftwareVersion string
}

func readAPIVersion(r io.Reader) APIVersion {
	var version APIVersion
	binary.Read(r, binary.BigEndian, &version.CorrelationID)

	var size int16
	binary.Read(r, binary.BigEndian, &size)
	clientID := make([]byte, size)
	binary.Read(r, binary.BigEndian, &clientID)

	binary.Read(r, binary.BigEndian, &size)
	clientSoftwareName := make([]byte, size)
	binary.Read(r, binary.BigEndian, &clientSoftwareName)
	clientSoftwareVersion, _ := io.ReadAll(r)

	return APIVersion{
		ClientID: string(clientID),
		ClientSoftwareName: string(clientSoftwareName),
		ClientSoftwareVersion: string(clientSoftwareVersion),
	}
}

type Message struct {
	data []byte
	// ...
}

type Server struct {
	coffsets map[string]int
	buffer []Message

	ln net.Listener
}

func NewServer() *Server { 
	return &Server{
		coffsets: make(map[string]int),
		buffer: make([]Message, 0),
	}
}

func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", ":9092")
	if err != nil {
		return err
	}
	s.ln = ln
	for {
		conn, err := ln.Accept()
		if err != nil {
			if err == io.EOF {
				return err
			}
			slog.Error("server accept error", "err", err)
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	fmt.Println("new connection", conn.RemoteAddr())

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			slog.Error("connection read error", "err", err)
			return
		}
		rawMsg := buf[:n]
		r := bytes.NewReader(rawMsg)
		var header Header
		binary.Read(r, binary.BigEndian, &header)

		switch header.APIKey {
		case APIKeyAPIVersions:
			version := readAPIVersion(r)
			fmt.Println(version)
		default:
			fmt.Println("unhandled message from the client =>", header.APIKey)
		}

	}
}

func main() {
	server := NewServer()
	go func() {
		log.Fatal(server.Listen())
	}()
	time.Sleep(time.Second)

	fmt.Println("producing....")

	consume()
}

func produce() error{
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		return err
	}

	defer p.Close()
	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "fooTopic"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
	return nil
}

func consume() error{
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return err
	}

	c.SubscribeTopics([]string{"fooTopic"}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	return c.Close()
}
