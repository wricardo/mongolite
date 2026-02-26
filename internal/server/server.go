package server

import (
	"fmt"
	"log"
	"net"

	"github.com/wricardo/mongolite/internal/handler"
)

type Server struct {
	addr    string
	handler *handler.Handler
}

func New(addr string, h *handler.Handler) *Server {
	return &Server{addr: addr, handler: h}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	defer ln.Close()

	log.Printf("mongolite listening on %s", s.addr)

	for {
		nc, err := ln.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		c := &conn{nc: nc, handler: s.handler}
		go c.serve()
	}
}
