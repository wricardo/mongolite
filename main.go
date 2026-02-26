package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/wricardo/mongolite/internal/engine"
	"github.com/wricardo/mongolite/internal/handler"
	"github.com/wricardo/mongolite/internal/server"
)

func main() {
	port := flag.Int("port", 27017, "TCP port to listen on")
	file := flag.String("file", "mongolite.json", "path to the data file")
	flag.Parse()

	eng, err := engine.New(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize engine: %v\n", err)
		os.Exit(1)
	}

	h := handler.New(eng)
	addr := fmt.Sprintf(":%d", *port)
	srv := server.New(addr, h)

	log.Printf("mongolite server starting (file: %s)", *file)
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
