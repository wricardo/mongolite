package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/wricardo/mongolite/internal/engine"
	"github.com/wricardo/mongolite/internal/handler"
	"github.com/wricardo/mongolite/internal/server"
)

const usage = `mongolite — MongoDB-compatible single-file database

Usage:
  mongolite [--file FILE] [--db DATABASE] <command> [flags]

Global flags:
  --file FILE    data file path (default: mongolite.json)
  --db DATABASE  database name (default: test)

Commands:
  serve                    Start the MongoDB-compatible server (accepts --port)
  find <collection>        [--filter JSON] [--sort JSON] [--limit N] [--skip N]
  insert <collection>      (--doc JSON | --doc-file FILE)
  insert-many <collection> (--docs JSON | --docs-file FILE)
  update <collection>      (--filter JSON) (--update JSON) [--multi]
  delete <collection>      (--filter JSON) [--multi]
  aggregate <collection>   (--pipeline JSON | --pipeline-file FILE)
  count <collection>       [--filter JSON]
  list-dbs
  list-collections
  install-skill            Install the Claude Code skill to ~/.claude/skills/mongolite/

Output: newline-delimited JSON (one document per line), suitable for piping to jq.
`

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, w io.Writer) error {
	gfs := flag.NewFlagSet("mongolite", flag.ContinueOnError)
	gfs.SetOutput(io.Discard)
	filePath := gfs.String("file", "mongolite.json", "data file path")
	dbName := gfs.String("db", "test", "database name")
	if err := gfs.Parse(args); err != nil {
		return err
	}

	remaining := gfs.Args()
	if len(remaining) == 0 {
		fmt.Fprint(w, usage)
		return nil
	}

	cmd := remaining[0]
	cmdArgs := remaining[1:]

	switch cmd {
	case "serve":
		return runServe(*filePath, cmdArgs)
	case "install-skill":
		return installSkill()
	case "find":
		eng, err := engine.New(*filePath)
		if err != nil {
			return fmt.Errorf("open %s: %w", *filePath, err)
		}
		return doFind(eng, *dbName, cmdArgs, w)
	case "insert":
		eng, err := engine.New(*filePath)
		if err != nil {
			return fmt.Errorf("open %s: %w", *filePath, err)
		}
		return doInsert(eng, *dbName, cmdArgs, w)
	case "insert-many":
		eng, err := engine.New(*filePath)
		if err != nil {
			return fmt.Errorf("open %s: %w", *filePath, err)
		}
		return doInsertMany(eng, *dbName, cmdArgs, w)
	case "update":
		eng, err := engine.New(*filePath)
		if err != nil {
			return fmt.Errorf("open %s: %w", *filePath, err)
		}
		return doUpdate(eng, *dbName, cmdArgs, w)
	case "delete":
		eng, err := engine.New(*filePath)
		if err != nil {
			return fmt.Errorf("open %s: %w", *filePath, err)
		}
		return doDelete(eng, *dbName, cmdArgs, w)
	case "aggregate":
		eng, err := engine.New(*filePath)
		if err != nil {
			return fmt.Errorf("open %s: %w", *filePath, err)
		}
		return doAggregate(eng, *dbName, cmdArgs, w)
	case "count":
		eng, err := engine.New(*filePath)
		if err != nil {
			return fmt.Errorf("open %s: %w", *filePath, err)
		}
		return doCount(eng, *dbName, cmdArgs, w)
	case "list-dbs":
		eng, err := engine.New(*filePath)
		if err != nil {
			return fmt.Errorf("open %s: %w", *filePath, err)
		}
		return doListDbs(eng, w)
	case "list-collections":
		eng, err := engine.New(*filePath)
		if err != nil {
			return fmt.Errorf("open %s: %w", *filePath, err)
		}
		return doListCollections(eng, *dbName, w)
	case "-h", "--help", "help":
		fmt.Fprint(w, usage)
		return nil
	default:
		return fmt.Errorf("unknown command %q\n\n%s", cmd, usage)
	}
}

// --- serve ---

func runServe(filePath string, args []string) error {
	sfs := flag.NewFlagSet("serve", flag.ContinueOnError)
	port := sfs.Int("port", 27017, "TCP port to listen on")
	// --file can also be specified after serve, overriding the global one
	file := sfs.String("file", filePath, "path to the data file")
	if err := sfs.Parse(args); err != nil {
		return err
	}

	eng, err := engine.New(*file)
	if err != nil {
		return fmt.Errorf("failed to initialize engine: %w", err)
	}

	h := handler.New(eng)
	addr := fmt.Sprintf(":%d", *port)
	srv := server.New(addr, h)

	log.Printf("mongolite server starting on %s (file: %s)", addr, *file)
	return srv.ListenAndServe()
}

// --- CLI commands ---

func extractCollection(args []string, cmdName string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, fmt.Errorf("%s requires a collection name", cmdName)
	}
	return args[0], args[1:], nil
}

func doFind(eng *engine.Engine, dbName string, args []string, w io.Writer) error {
	collName, flagArgs, err := extractCollection(args, "find")
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("find", flag.ContinueOnError)
	filter := fs.String("filter", "{}", "filter document (JSON)")
	filterFile := fs.String("filter-file", "", "filter document from file")
	sortSpec := fs.String("sort", "", "sort document (JSON)")
	sortFile := fs.String("sort-file", "", "sort document from file")
	limit := fs.Int64("limit", 0, "max documents to return")
	skip := fs.Int64("skip", 0, "documents to skip")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	filterDoc, err := parseJSONArg(*filter, *filterFile)
	if err != nil {
		return err
	}

	var sortDoc bson.D
	sortStr, err := readArg(*sortSpec, *sortFile)
	if err != nil {
		return err
	}
	if sortStr != "" {
		if err := bson.UnmarshalExtJSON([]byte(sortStr), false, &sortDoc); err != nil {
			return fmt.Errorf("parse sort: %w", err)
		}
	}

	results, err := eng.Find(dbName, collName, filterDoc, sortDoc, *skip, *limit)
	if err != nil {
		return fmt.Errorf("find: %w", err)
	}
	for _, doc := range results {
		if err := writeDoc(w, doc); err != nil {
			return err
		}
	}
	return nil
}

func doInsert(eng *engine.Engine, dbName string, args []string, w io.Writer) error {
	collName, flagArgs, err := extractCollection(args, "insert")
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("insert", flag.ContinueOnError)
	doc := fs.String("doc", "", "document (JSON)")
	docFile := fs.String("doc-file", "", "document from file")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	docVal, err := parseJSONArg(*doc, *docFile)
	if err != nil {
		return err
	}
	if len(docVal) == 0 {
		return fmt.Errorf("insert requires --doc or --doc-file")
	}

	ids, err := eng.Insert(dbName, collName, []bson.D{docVal})
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	if len(ids) > 0 {
		return writeJSON(w, bson.D{{Key: "insertedId", Value: ids[0]}})
	}
	return nil
}

func doInsertMany(eng *engine.Engine, dbName string, args []string, w io.Writer) error {
	collName, flagArgs, err := extractCollection(args, "insert-many")
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("insert-many", flag.ContinueOnError)
	docs := fs.String("docs", "", "documents array (JSON)")
	docsFile := fs.String("docs-file", "", "documents array from file")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	docsStr, err := readArg(*docs, *docsFile)
	if err != nil {
		return err
	}
	if docsStr == "" {
		return fmt.Errorf("insert-many requires --docs or --docs-file")
	}

	var arr []bson.D
	if err := bson.UnmarshalExtJSON([]byte(docsStr), false, &arr); err != nil {
		return fmt.Errorf("parse docs: %w", err)
	}

	ids, err := eng.Insert(dbName, collName, arr)
	if err != nil {
		return fmt.Errorf("insert-many: %w", err)
	}
	return writeJSON(w, bson.D{{Key: "insertedCount", Value: len(ids)}})
}

func doUpdate(eng *engine.Engine, dbName string, args []string, w io.Writer) error {
	collName, flagArgs, err := extractCollection(args, "update")
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("update", flag.ContinueOnError)
	filter := fs.String("filter", "{}", "filter document (JSON)")
	filterFile := fs.String("filter-file", "", "filter document from file")
	update := fs.String("update", "", "update document (JSON)")
	updateFile := fs.String("update-file", "", "update document from file")
	multi := fs.Bool("multi", false, "update multiple documents")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	filterDoc, err := parseJSONArg(*filter, *filterFile)
	if err != nil {
		return err
	}
	updateDoc, err := parseJSONArg(*update, *updateFile)
	if err != nil {
		return err
	}
	if len(updateDoc) == 0 {
		return fmt.Errorf("update requires --update or --update-file")
	}

	matched, modified, _, err := eng.Update(dbName, collName, filterDoc, updateDoc, *multi, false)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	return writeJSON(w, bson.D{
		{Key: "matchedCount", Value: matched},
		{Key: "modifiedCount", Value: modified},
	})
}

func doDelete(eng *engine.Engine, dbName string, args []string, w io.Writer) error {
	collName, flagArgs, err := extractCollection(args, "delete")
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("delete", flag.ContinueOnError)
	filter := fs.String("filter", "{}", "filter document (JSON)")
	filterFile := fs.String("filter-file", "", "filter document from file")
	multi := fs.Bool("multi", false, "delete multiple documents")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	filterDoc, err := parseJSONArg(*filter, *filterFile)
	if err != nil {
		return err
	}

	deleted, err := eng.Delete(dbName, collName, filterDoc, *multi)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return writeJSON(w, bson.D{{Key: "deletedCount", Value: deleted}})
}

func doAggregate(eng *engine.Engine, dbName string, args []string, w io.Writer) error {
	collName, flagArgs, err := extractCollection(args, "aggregate")
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("aggregate", flag.ContinueOnError)
	pipeline := fs.String("pipeline", "", "pipeline array (JSON)")
	pipelineFile := fs.String("pipeline-file", "", "pipeline array from file")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	pipelineStr, err := readArg(*pipeline, *pipelineFile)
	if err != nil {
		return err
	}
	if pipelineStr == "" {
		return fmt.Errorf("aggregate requires --pipeline or --pipeline-file")
	}

	var stages []bson.D
	if err := bson.UnmarshalExtJSON([]byte(pipelineStr), false, &stages); err != nil {
		return fmt.Errorf("parse pipeline: %w", err)
	}

	results, err := eng.Aggregate(dbName, collName, stages)
	if err != nil {
		return fmt.Errorf("aggregate: %w", err)
	}
	for _, doc := range results {
		if err := writeDoc(w, doc); err != nil {
			return err
		}
	}
	return nil
}

func doCount(eng *engine.Engine, dbName string, args []string, w io.Writer) error {
	collName, flagArgs, err := extractCollection(args, "count")
	if err != nil {
		return err
	}

	fs := flag.NewFlagSet("count", flag.ContinueOnError)
	filter := fs.String("filter", "{}", "filter document (JSON)")
	filterFile := fs.String("filter-file", "", "filter document from file")
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}

	filterDoc, err := parseJSONArg(*filter, *filterFile)
	if err != nil {
		return err
	}

	n, err := eng.Count(dbName, collName, filterDoc)
	if err != nil {
		return fmt.Errorf("count: %w", err)
	}
	return writeJSON(w, bson.D{{Key: "count", Value: n}})
}

func doListDbs(eng *engine.Engine, w io.Writer) error {
	for _, name := range eng.ListDatabases() {
		if err := writeJSON(w, bson.D{{Key: "name", Value: name}}); err != nil {
			return err
		}
	}
	return nil
}

func doListCollections(eng *engine.Engine, dbName string, w io.Writer) error {
	for _, name := range eng.ListCollections(dbName) {
		if err := writeJSON(w, bson.D{{Key: "name", Value: name}}); err != nil {
			return err
		}
	}
	return nil
}

// --- install-skill ---

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

// --- helpers ---

func parseJSONArg(inline, filePath string) (bson.D, error) {
	s, err := readArg(inline, filePath)
	if err != nil {
		return nil, err
	}
	if s == "" {
		return bson.D{}, nil
	}
	var doc bson.D
	if err := bson.UnmarshalExtJSON([]byte(s), false, &doc); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	return doc, nil
}

func readArg(inline, filePath string) (string, error) {
	if filePath != "" {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("read file %s: %w", filePath, err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	return inline, nil
}

func writeDoc(w io.Writer, doc bson.D) error {
	ejson, err := bson.MarshalExtJSON(doc, false, false)
	if err != nil {
		return fmt.Errorf("marshal JSON: %w", err)
	}
	_, err = fmt.Fprintln(w, string(ejson))
	return err
}

func writeJSON(w io.Writer, doc bson.D) error {
	data, err := bson.MarshalExtJSON(doc, false, false)
	if err != nil {
		return fmt.Errorf("marshal JSON: %w", err)
	}
	var compact json.RawMessage = data
	out, err := json.Marshal(compact)
	if err != nil {
		return fmt.Errorf("marshal JSON: %w", err)
	}
	_, err = fmt.Fprintln(w, string(out))
	return err
}
