package main

import (
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/wricardo/mongolite/internal/engine"
	"github.com/wricardo/mongolite/internal/handler"
	"github.com/wricardo/mongolite/internal/server"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "install-skill" {
		if err := installSkill(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

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

// installSkill writes the embedded Claude Code skill to ~/.claude/skills/mongolite/.
func installSkill() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("get home dir: %w", err)
	}

	skillDest := filepath.Join(home, ".claude", "skills")

	const embedRoot = ".claude/skills/mongolite"
	return fs.WalkDir(skillFS, embedRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Strip the embed root prefix to get the relative path under the skill dir.
		// e.g. ".claude/skills/mongolite/SKILL.md" → "mongolite/SKILL.md"
		rel := strings.TrimPrefix(path, ".claude/skills/")
		dest := filepath.Join(skillDest, rel)

		if d.IsDir() {
			return os.MkdirAll(dest, 0755)
		}

		data, err := skillFS.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			return err
		}
		fmt.Println(" ", dest)
		return os.WriteFile(dest, data, 0644)
	})
}
