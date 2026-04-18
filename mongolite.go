package mongolite

import (
	"fmt"

	"github.com/wricardo/mongolite/internal/engine"
	"github.com/wricardo/mongolite/internal/handler"
	"github.com/wricardo/mongolite/internal/server"
)

// ListenAndServe starts a MongoDB wire-protocol compatible server on addr,
// backed by the JSON file at filePath.
func ListenAndServe(addr, filePath string) error {
	eng, err := engine.New(filePath)
	if err != nil {
		return fmt.Errorf("mongolite: %w", err)
	}
	h := handler.New(eng)
	srv := server.New(addr, h)
	return srv.ListenAndServe()
}
