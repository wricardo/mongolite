package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/wricardo/mongolite/internal/engine"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, w io.Writer) error {
	fs := flag.NewFlagSet("mongolite-cli", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	filePath := fs.String("file", "mongolite.json", "data file path")
	dbName := fs.String("db", "test", "database name")
	if err := fs.Parse(args); err != nil {
		return err
	}

	remaining := fs.Args()
	if len(remaining) == 0 {
		printUsage(w)
		return fmt.Errorf("no command specified")
	}

	eng, err := engine.New(*filePath)
	if err != nil {
		return fmt.Errorf("open %s: %w", *filePath, err)
	}

	cmd := remaining[0]
	cmdArgs := remaining[1:]

	switch cmd {
	case "find":
		return doFind(eng, *dbName, cmdArgs, w)
	case "insert":
		return doInsert(eng, *dbName, cmdArgs, w)
	case "insert-many":
		return doInsertMany(eng, *dbName, cmdArgs, w)
	case "update":
		return doUpdate(eng, *dbName, cmdArgs, w)
	case "delete":
		return doDelete(eng, *dbName, cmdArgs, w)
	case "aggregate":
		return doAggregate(eng, *dbName, cmdArgs, w)
	case "count":
		return doCount(eng, *dbName, cmdArgs, w)
	case "list-dbs":
		return doListDbs(eng, w)
	case "list-collections":
		return doListCollections(eng, *dbName, w)
	default:
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

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

func printUsage(w io.Writer) {
	fmt.Fprintf(w, `mongolite-cli — operates directly on the data file, no server required

Usage: mongolite-cli [--file FILE] [--db DATABASE] <command> [args]

Global flags:
  --file FILE    data file path (default: mongolite.json)
  --db DATABASE  database name (default: test)

Commands:
  find <collection>        [--filter JSON | --filter-file FILE] [--sort JSON | --sort-file FILE] [--limit N] [--skip N]
  insert <collection>      (--doc JSON | --doc-file FILE)
  insert-many <collection> (--docs JSON | --docs-file FILE)
  update <collection>      (--filter JSON | --filter-file FILE) (--update JSON | --update-file FILE) [--multi]
  delete <collection>      (--filter JSON | --filter-file FILE) [--multi]
  aggregate <collection>   (--pipeline JSON | --pipeline-file FILE)
  count <collection>       [--filter JSON | --filter-file FILE]
  list-dbs
  list-collections

Output: newline-delimited JSON (one document per line), suitable for piping to jq.
`)
}
