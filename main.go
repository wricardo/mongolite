package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/wricardo/mongolite/internal/engine"
	"github.com/wricardo/mongolite/internal/handler"
	"github.com/wricardo/mongolite/internal/server"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, w io.Writer) error {
	app := &cli.App{
		Name:  "mongolite",
		Usage: "MongoDB-compatible single-file database and CLI toolkit",
		Description: `mongolite keeps MongoDB-style databases inside a single JSON file so agents and scripts can run a local datastore without provisioning a server.

Use the CRUD/aggregation commands (find, insert, insert-many, update, delete, aggregate, count) to manipulate collections directly from the CLI. Inspect structure with list-dbs and list-collections, and manage schema metadata with set-schema/get-schema/delete-schema/list-schemas to document or validate expected shapes for each collection (describe every field you plan to persist so downstream agents can trust the contract—schemas should enumerate all expected keys, types, and constraints). All commands accept --file to point at the backing document store and --db to select the default database.

Examples:
- mongolite --file state.json insert tasks --doc '{"task_id":"123","status":"pending"}'
- mongolite --file state.json find tasks --filter '{"status":"pending"}' --sort '{"created_at":1}'
- mongolite --file state.json set-schema tasks --schema '{"bsonType":"object","required":["task_id"],"properties":{"task_id":{"bsonType":"string"},"status":{"enum":["pending","done"]}}}'`,
		Writer:          w,
		ErrWriter:       io.Discard,
		HideHelpCommand: true,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "file", Value: "mongolite.json", Usage: "data file path"},
			&cli.StringFlag{Name: "db", Value: "test", Usage: "database name"},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() > 0 {
				return fmt.Errorf("unknown command %q", c.Args().First())
			}
			return cli.ShowAppHelp(c)
		},
		Commands: []*cli.Command{
			{
				Name:  "serve",
				Usage: "start the MongoDB-compatible server",
				Flags: []cli.Flag{
					&cli.IntFlag{Name: "port", Value: 27017, Usage: "TCP port to listen on"},
				},
				Action: func(c *cli.Context) error {
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("failed to initialize engine: %w", err)
					}
					h := handler.New(eng)
					addr := fmt.Sprintf(":%d", c.Int("port"))
					srv := server.New(addr, h)
					log.Printf("mongolite server starting on %s (file: %s)", addr, c.String("file"))
					return srv.ListenAndServe()
				},
			},
			{
				Name:  "find",
				Usage: "query documents in a collection",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "filter", Value: "{}", Usage: "filter document (JSON)"},
					&cli.StringFlag{Name: "filter-file", Usage: "filter document from file"},
					&cli.StringFlag{Name: "sort", Usage: "sort document (JSON)"},
					&cli.StringFlag{Name: "sort-file", Usage: "sort document from file"},
					&cli.Int64Flag{Name: "limit", Usage: "max documents to return"},
					&cli.Int64Flag{Name: "skip", Usage: "documents to skip"},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() == 0 {
						return fmt.Errorf("find requires a collection name")
					}
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doFind(eng, c.String("db"), c.Args().First(), c, c.App.Writer)
				},
			},
			{
				Name:  "insert",
				Usage: "insert a document into a collection",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "doc", Usage: "document (JSON)"},
					&cli.StringFlag{Name: "doc-file", Usage: "document from file"},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() == 0 {
						return fmt.Errorf("insert requires a collection name")
					}
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doInsert(eng, c.String("db"), c.Args().First(), c, c.App.Writer)
				},
			},
			{
				Name:  "insert-many",
				Usage: "insert multiple documents into a collection",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "docs", Usage: "documents array (JSON)"},
					&cli.StringFlag{Name: "docs-file", Usage: "documents array from file"},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() == 0 {
						return fmt.Errorf("insert-many requires a collection name")
					}
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doInsertMany(eng, c.String("db"), c.Args().First(), c, c.App.Writer)
				},
			},
			{
				Name:  "update",
				Usage: "update documents in a collection",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "filter", Value: "{}", Usage: "filter document (JSON)"},
					&cli.StringFlag{Name: "filter-file", Usage: "filter document from file"},
					&cli.StringFlag{Name: "update", Usage: "update document (JSON)"},
					&cli.StringFlag{Name: "update-file", Usage: "update document from file"},
					&cli.BoolFlag{Name: "multi", Usage: "update multiple documents"},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() == 0 {
						return fmt.Errorf("update requires a collection name")
					}
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doUpdate(eng, c.String("db"), c.Args().First(), c, c.App.Writer)
				},
			},
			{
				Name:  "delete",
				Usage: "delete documents from a collection",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "filter", Value: "{}", Usage: "filter document (JSON)"},
					&cli.StringFlag{Name: "filter-file", Usage: "filter document from file"},
					&cli.BoolFlag{Name: "multi", Usage: "delete multiple documents"},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() == 0 {
						return fmt.Errorf("delete requires a collection name")
					}
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doDelete(eng, c.String("db"), c.Args().First(), c, c.App.Writer)
				},
			},
			{
				Name:  "aggregate",
				Usage: "run an aggregation pipeline on a collection",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "pipeline", Usage: "pipeline array (JSON)"},
					&cli.StringFlag{Name: "pipeline-file", Usage: "pipeline array from file"},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() == 0 {
						return fmt.Errorf("aggregate requires a collection name")
					}
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doAggregate(eng, c.String("db"), c.Args().First(), c, c.App.Writer)
				},
			},
			{
				Name:  "count",
				Usage: "count documents in a collection",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "filter", Value: "{}", Usage: "filter document (JSON)"},
					&cli.StringFlag{Name: "filter-file", Usage: "filter document from file"},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() == 0 {
						return fmt.Errorf("count requires a collection name")
					}
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doCount(eng, c.String("db"), c.Args().First(), c, c.App.Writer)
				},
			},
			{
				Name:  "list-dbs",
				Usage: "list all databases",
				Action: func(c *cli.Context) error {
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doListDbs(eng, c.App.Writer)
				},
			},
			{
				Name:  "list-collections",
				Usage: "list collections in the database",
				Action: func(c *cli.Context) error {
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doListCollections(eng, c.String("db"), c.App.Writer)
				},
			},
			{
				Name:  "set-schema",
				Usage: "set schema for a collection",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "schema", Usage: "schema (JSON)"},
					&cli.StringFlag{Name: "schema-file", Usage: "schema from file"},
					&cli.StringFlag{Name: "description", Usage: "description text"},
				},
				Action: func(c *cli.Context) error {
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doSetSchema(eng, c.String("db"), c.Args().First(), c, c.App.Writer)
				},
			},
			{
				Name:  "get-schema",
				Usage: "get schema for a collection",
				Action: func(c *cli.Context) error {
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doGetSchema(eng, c.String("db"), c.Args().First(), c.App.Writer)
				},
			},
			{
				Name:  "delete-schema",
				Usage: "delete schema for a collection",
				Action: func(c *cli.Context) error {
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doDeleteSchema(eng, c.String("db"), c.Args().First(), c.App.Writer)
				},
			},
			{
				Name:  "list-schemas",
				Usage: "list all schemas",
				Action: func(c *cli.Context) error {
					eng, err := engine.New(c.String("file"))
					if err != nil {
						return fmt.Errorf("open: %w", err)
					}
					return doListSchemas(eng, c.App.Writer)
				},
			},
			{
				Name:  "install-skill",
				Usage: "install the Claude Code skill to ~/.claude/skills/mongolite/",
				Action: func(c *cli.Context) error {
					return installSkill()
				},
			},
		},
	}
	return app.Run(append([]string{"mongolite"}, args...))
}

// --- CLI commands ---

func doFind(eng *engine.Engine, dbName, collName string, c *cli.Context, w io.Writer) error {
	filterDoc, err := parseJSONArg(c.String("filter"), c.String("filter-file"))
	if err != nil {
		return err
	}

	var sortDoc bson.D
	sortStr, err := readArg(c.String("sort"), c.String("sort-file"))
	if err != nil {
		return err
	}
	if sortStr != "" {
		if err := bson.UnmarshalExtJSON([]byte(sortStr), false, &sortDoc); err != nil {
			return fmt.Errorf("parse sort: %w", err)
		}
	}

	results, err := eng.Find(dbName, collName, filterDoc, sortDoc, c.Int64("skip"), c.Int64("limit"))
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

func doInsert(eng *engine.Engine, dbName, collName string, c *cli.Context, w io.Writer) error {
	docVal, err := parseJSONArg(c.String("doc"), c.String("doc-file"))
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

func doInsertMany(eng *engine.Engine, dbName, collName string, c *cli.Context, w io.Writer) error {
	docsStr, err := readArg(c.String("docs"), c.String("docs-file"))
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

func doUpdate(eng *engine.Engine, dbName, collName string, c *cli.Context, w io.Writer) error {
	filterDoc, err := parseJSONArg(c.String("filter"), c.String("filter-file"))
	if err != nil {
		return err
	}
	updateDoc, err := parseJSONArg(c.String("update"), c.String("update-file"))
	if err != nil {
		return err
	}
	if len(updateDoc) == 0 {
		return fmt.Errorf("update requires --update or --update-file")
	}

	matched, modified, _, err := eng.Update(dbName, collName, filterDoc, updateDoc, c.Bool("multi"), false)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	return writeJSON(w, bson.D{
		{Key: "matchedCount", Value: matched},
		{Key: "modifiedCount", Value: modified},
	})
}

func doDelete(eng *engine.Engine, dbName, collName string, c *cli.Context, w io.Writer) error {
	filterDoc, err := parseJSONArg(c.String("filter"), c.String("filter-file"))
	if err != nil {
		return err
	}

	deleted, err := eng.Delete(dbName, collName, filterDoc, c.Bool("multi"))
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return writeJSON(w, bson.D{{Key: "deletedCount", Value: deleted}})
}

func doAggregate(eng *engine.Engine, dbName, collName string, c *cli.Context, w io.Writer) error {
	pipelineStr, err := readArg(c.String("pipeline"), c.String("pipeline-file"))
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

func doCount(eng *engine.Engine, dbName, collName string, c *cli.Context, w io.Writer) error {
	filterDoc, err := parseJSONArg(c.String("filter"), c.String("filter-file"))
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

// --- schema commands ---

func doSetSchema(eng *engine.Engine, dbName, collName string, c *cli.Context, w io.Writer) error {
	schStr, err := readArg(c.String("schema"), c.String("schema-file"))
	if err != nil {
		return err
	}
	description := c.String("description")
	if schStr == "" && description == "" {
		return fmt.Errorf("set-schema requires at least --schema, --schema-file, or --description")
	}

	var schemaJSON json.RawMessage
	if schStr != "" {
		schemaJSON = json.RawMessage(schStr)
	}

	if err := eng.SetSchema(dbName, collName, schemaJSON, description); err != nil {
		return fmt.Errorf("set-schema: %w", err)
	}
	return writeJSON(w, bson.D{{Key: "ok", Value: 1}})
}

func doGetSchema(eng *engine.Engine, dbName, collName string, w io.Writer) error {
	schemaJSON, description, err := eng.GetSchema(dbName, collName)
	if err != nil {
		return fmt.Errorf("get-schema: %w", err)
	}
	if schemaJSON == nil && description == "" {
		return fmt.Errorf("no schema or description found for %s.%s", dbName, collName)
	}

	result := bson.D{{Key: "db", Value: dbName}, {Key: "collection", Value: collName}}
	if schemaJSON != nil {
		var schemaVal interface{}
		if err := bson.UnmarshalExtJSON(schemaJSON, false, &schemaVal); err != nil {
			return fmt.Errorf("parse schema: %w", err)
		}
		result = append(result, bson.E{Key: "schema", Value: schemaVal})
	}
	if description != "" {
		result = append(result, bson.E{Key: "description", Value: description})
	}
	return writeDoc(w, result)
}

func doDeleteSchema(eng *engine.Engine, dbName, collName string, w io.Writer) error {
	if err := eng.DeleteSchema(dbName, collName); err != nil {
		return fmt.Errorf("delete-schema: %w", err)
	}
	return writeJSON(w, bson.D{{Key: "ok", Value: 1}})
}

func doListSchemas(eng *engine.Engine, w io.Writer) error {
	docs := eng.ListSchemas()
	for _, doc := range docs {
		if err := writeDoc(w, doc); err != nil {
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
