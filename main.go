package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
)

type Server struct {
	ln net.Listener
}

func NewServer() *Server { return &Server{}}

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
}

func main() {
	server := NewServer()
	server.Listen()
}
