package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/wricardo/mongolite/internal/engine"
)

// newTestEngine creates a fresh engine backed by a temp file.
func newTestEngine(t *testing.T) (*engine.Engine, string) {
	t.Helper()
	f := filepath.Join(t.TempDir(), "test.json")
	eng, err := engine.New(f)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	return eng, f
}

// runWith calls run() with the given file and extra args, returns stdout.
func runWith(t *testing.T, file string, args ...string) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	fullArgs := append([]string{"--file", file, "--db", "test"}, args...)
	err := run(fullArgs, &buf)
	return buf.String(), err
}

// decodeLines parses each non-empty line of ndjson output into a map.
func decodeLines(t *testing.T, s string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("decode line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// --- readArg ---

func TestReadArg_Inline(t *testing.T) {
	got, err := readArg("hello", "")
	if err != nil || got != "hello" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestReadArg_File(t *testing.T) {
	f := filepath.Join(t.TempDir(), "arg.txt")
	os.WriteFile(f, []byte("  from file  \n"), 0644)
	got, err := readArg("", f)
	if err != nil || got != "from file" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestReadArg_FilePreferredOverInline(t *testing.T) {
	f := filepath.Join(t.TempDir(), "arg.txt")
	os.WriteFile(f, []byte("file-value"), 0644)
	got, err := readArg("inline-value", f)
	if err != nil || got != "file-value" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestReadArg_MissingFile(t *testing.T) {
	_, err := readArg("", "/nonexistent/file.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

// --- parseJSONArg ---

func TestParseJSONArg_EmptyInline(t *testing.T) {
	doc, err := parseJSONArg("", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(doc) != 0 {
		t.Fatalf("expected empty doc, got %v", doc)
	}
}

func TestParseJSONArg_Inline(t *testing.T) {
	doc, err := parseJSONArg(`{"x": 1}`, "")
	if err != nil {
		t.Fatal(err)
	}
	v, _ := bson.Raw(must(bson.Marshal(doc))).Lookup("x").Int32OK()
	if v != 1 {
		t.Fatalf("expected x=1, got %v", doc)
	}
}

func TestParseJSONArg_FromFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "filter.json")
	os.WriteFile(f, []byte(`{"y": 42}`), 0644)
	doc, err := parseJSONArg("", f)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := bson.Raw(must(bson.Marshal(doc))).Lookup("y").Int32OK()
	if !ok || v != 42 {
		t.Fatalf("expected y=42, got doc=%v", doc)
	}
}

func TestParseJSONArg_BadJSON(t *testing.T) {
	_, err := parseJSONArg(`{bad json}`, "")
	if err == nil {
		t.Fatal("expected parse error")
	}
}

// --- doFind ---

func TestDoFind_Empty(t *testing.T) {
	_, f := newTestEngine(t)
	out, err := runWith(t, f, "find", "users")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "" {
		t.Fatalf("expected empty output, got %q", out)
	}
}

func TestDoFind_All(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "name", Value: "Alice"}},
		{{Key: "name", Value: "Bob"}},
	})

	out, err := runWith(t, f, "find", "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(rows))
	}
}

func TestDoFind_Filter(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "name", Value: "Alice"}, {Key: "age", Value: int32(30)}},
		{{Key: "name", Value: "Bob"}, {Key: "age", Value: int32(20)}},
	})

	out, err := runWith(t, f, "find", "--filter", `{"age": {"$gt": 25}}`, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 || rows[0]["name"] != "Alice" {
		t.Fatalf("expected [Alice], got %v", rows)
	}
}

func TestDoFind_Limit(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "items", []bson.D{
		{{Key: "n", Value: int32(1)}},
		{{Key: "n", Value: int32(2)}},
		{{Key: "n", Value: int32(3)}},
	})

	out, err := runWith(t, f, "find", "--limit", "2", "items")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(rows))
	}
}

func TestDoFind_Skip(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "items", []bson.D{
		{{Key: "n", Value: int32(1)}},
		{{Key: "n", Value: int32(2)}},
		{{Key: "n", Value: int32(3)}},
	})

	out, err := runWith(t, f, "find", "--skip", "2", "items")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 {
		t.Fatalf("expected 1 doc after skip, got %d", len(rows))
	}
}

func TestDoFind_Sort(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "name", Value: "Charlie"}, {Key: "age", Value: int32(25)}},
		{{Key: "name", Value: "Alice"}, {Key: "age", Value: int32(30)}},
	})

	out, err := runWith(t, f, "find", "--sort", `{"age": 1}`, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if rows[0]["name"] != "Charlie" {
		t.Fatalf("expected Charlie first (age 25), got %v", rows[0]["name"])
	}
}

func TestDoFind_FilterFile(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "status", Value: "active"}, {Key: "score", Value: int32(10)}},
		{{Key: "status", Value: "inactive"}, {Key: "score", Value: int32(20)}},
	})

	filterFile := filepath.Join(t.TempDir(), "filter.json")
	os.WriteFile(filterFile, []byte(`{"status": "active"}`), 0644)

	out, err := runWith(t, f, "find", "--filter-file", filterFile, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 {
		t.Fatalf("expected 1 doc, got %d: %v", len(rows), rows)
	}
	if rows[0]["status"] != "active" {
		t.Fatalf("expected status=active, got %v", rows[0]["status"])
	}
	if rows[0]["score"].(float64) != 10 {
		t.Fatalf("expected score=10, got %v", rows[0]["score"])
	}
}

func TestDoFind_SortFile(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "name", Value: "Alice"}, {Key: "age", Value: int32(30)}},
		{{Key: "name", Value: "Bob"}, {Key: "age", Value: int32(20)}},
	})

	sortFile := filepath.Join(t.TempDir(), "sort.json")
	os.WriteFile(sortFile, []byte(`{"age": 1}`), 0644)

	out, err := runWith(t, f, "find", "--sort-file", sortFile, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 2 || rows[0]["name"] != "Bob" {
		t.Fatalf("expected Bob first after sort-file, got %v", rows)
	}
}

func TestDoFind_Projection(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "name", Value: "Alice"}, {Key: "age", Value: int32(30)}, {Key: "role", Value: "admin"}},
	})

	out, err := runWith(t, f, "find", "--projection", `{"name": 1, "_id": 0}`, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(rows))
	}
	if rows[0]["name"] != "Alice" {
		t.Fatalf("expected projected name field, got %v", rows[0])
	}
	if _, ok := rows[0]["age"]; ok {
		t.Fatalf("did not expect age in projection result: %v", rows[0])
	}
	if _, ok := rows[0]["_id"]; ok {
		t.Fatalf("did not expect _id in projection result: %v", rows[0])
	}
}

func TestDoFind_ProjectionFile(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "first", Value: "Ada"}, {Key: "last", Value: "Lovelace"}},
	})

	projectionFile := filepath.Join(t.TempDir(), "projection.json")
	os.WriteFile(projectionFile, []byte(`{"full": {"$concat": ["$first", " ", "$last"]}, "_id": 0}`), 0644)

	out, err := runWith(t, f, "find", "--projection-file", projectionFile, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 || rows[0]["full"] != "Ada Lovelace" {
		t.Fatalf("expected computed projection result, got %v", rows)
	}
}

func TestDoFind_MissingCollection(t *testing.T) {
	_, f := newTestEngine(t)
	_, err := runWith(t, f, "find")
	if err == nil {
		t.Fatal("expected error for missing collection name")
	}
}

// --- doInsert ---

func TestDoInsert_OK(t *testing.T) {
	_, f := newTestEngine(t)
	out, err := runWith(t, f, "insert", "--doc", `{"name": "Alice"}`, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 || rows[0]["insertedId"] == nil {
		t.Fatalf("expected insertedId, got %v", rows)
	}
}

func TestDoInsert_PersistsToFile(t *testing.T) {
	_, f := newTestEngine(t)
	runWith(t, f, "insert", "--doc", `{"name": "Alice"}`, "users")

	out, err := runWith(t, f, "find", "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 || rows[0]["name"] != "Alice" {
		t.Fatalf("expected Alice, got %v", rows)
	}
}

func TestDoInsert_MissingDoc(t *testing.T) {
	_, f := newTestEngine(t)
	_, err := runWith(t, f, "insert", "users")
	if err == nil {
		t.Fatal("expected error for missing --doc")
	}
}

func TestDoInsert_DocFile(t *testing.T) {
	_, f := newTestEngine(t)
	docFile := filepath.Join(t.TempDir(), "doc.json")
	os.WriteFile(docFile, []byte(`{"name": "Bob"}`), 0644)

	out, err := runWith(t, f, "insert", "--doc-file", docFile, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 || rows[0]["insertedId"] == nil {
		t.Fatalf("expected insertedId field, got %v", rows)
	}

	findOut, err := runWith(t, f, "find", "users")
	if err != nil {
		t.Fatal(err)
	}
	found := decodeLines(t, findOut)
	if len(found) != 1 || found[0]["name"] != "Bob" {
		t.Fatalf("expected Bob in collection, got %v", found)
	}
}

// --- doInsertMany ---

func TestDoInsertMany_OK(t *testing.T) {
	_, f := newTestEngine(t)
	out, err := runWith(t, f, "insert-many", "--docs", `[{"name":"A"},{"name":"B"},{"name":"C"}]`, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 {
		t.Fatalf("expected 1 result line, got %d", len(rows))
	}
	if rows[0]["insertedCount"].(float64) != 3 {
		t.Fatalf("expected insertedCount=3, got %v", rows[0])
	}
}

func TestDoInsertMany_MissingDocs(t *testing.T) {
	_, f := newTestEngine(t)
	_, err := runWith(t, f, "insert-many", "users")
	if err == nil {
		t.Fatal("expected error for missing --docs")
	}
}

func TestDoInsertMany_BadJSON(t *testing.T) {
	_, f := newTestEngine(t)
	_, err := runWith(t, f, "insert-many", "--docs", `not json`, "users")
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestDoInsertMany_DocsFile(t *testing.T) {
	_, f := newTestEngine(t)
	docsFile := filepath.Join(t.TempDir(), "docs.json")
	os.WriteFile(docsFile, []byte(`[{"name":"A"},{"name":"B"}]`), 0644)

	out, err := runWith(t, f, "insert-many", "--docs-file", docsFile, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 || rows[0]["insertedCount"].(float64) != 2 {
		t.Fatalf("expected insertedCount=2, got %v", rows)
	}
}

// --- doUpdate ---

func TestDoUpdate_Single(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "name", Value: "Alice"}, {Key: "age", Value: int32(30)}},
		{{Key: "name", Value: "Bob"}, {Key: "age", Value: int32(25)}},
	})

	out, err := runWith(t, f, "update",
		"--filter", `{"name": "Alice"}`,
		"--update", `{"$set": {"age": 31}}`,
		"users",
	)
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if rows[0]["matchedCount"].(float64) != 1 || rows[0]["modifiedCount"].(float64) != 1 {
		t.Fatalf("unexpected counts: %v", rows[0])
	}

	findOut, err := runWith(t, f, "find", "--filter", `{"name": "Alice"}`, "users")
	if err != nil {
		t.Fatal(err)
	}
	found := decodeLines(t, findOut)
	if len(found) != 1 || found[0]["age"].(float64) != 31 {
		t.Fatalf("expected Alice age=31 after update, got %v", found)
	}
}

func TestDoUpdate_Multi(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "role", Value: "user"}},
		{{Key: "role", Value: "user"}},
		{{Key: "role", Value: "admin"}},
	})

	out, err := runWith(t, f, "update",
		"--filter", `{"role": "user"}`,
		"--update", `{"$set": {"role": "member"}}`,
		"--multi",
		"users",
	)
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if rows[0]["modifiedCount"].(float64) != 2 {
		t.Fatalf("expected modifiedCount=2, got %v", rows[0])
	}
}

func TestDoUpdate_NoMatch(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{{{Key: "name", Value: "Alice"}}})

	out, err := runWith(t, f, "update",
		"--filter", `{"name": "Nobody"}`,
		"--update", `{"$set": {"x": 1}}`,
		"users",
	)
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if rows[0]["matchedCount"].(float64) != 0 {
		t.Fatalf("expected matchedCount=0, got %v", rows[0])
	}
}

func TestDoUpdate_MissingUpdate(t *testing.T) {
	_, f := newTestEngine(t)
	_, err := runWith(t, f, "update", "--filter", `{}`, "users")
	if err == nil {
		t.Fatal("expected error for missing --update")
	}
}

func TestDoUpdate_FilterAndUpdateFiles(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "name", Value: "Alice"}, {Key: "age", Value: int32(30)}},
		{{Key: "name", Value: "Bob"}, {Key: "age", Value: int32(25)}},
	})

	filterFile := filepath.Join(t.TempDir(), "filter.json")
	updateFile := filepath.Join(t.TempDir(), "update.json")
	os.WriteFile(filterFile, []byte(`{"name": "Bob"}`), 0644)
	os.WriteFile(updateFile, []byte(`{"$set": {"age": 26}}`), 0644)

	out, err := runWith(t, f, "update", "--filter-file", filterFile, "--update-file", updateFile, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 || rows[0]["modifiedCount"].(float64) != 1 {
		t.Fatalf("expected modifiedCount=1, got %v", rows)
	}
}

// --- doDelete ---

func TestDoDelete_Single(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "name", Value: "Alice"}},
		{{Key: "name", Value: "Bob"}},
	})

	out, err := runWith(t, f, "delete", "--filter", `{"name": "Alice"}`, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if rows[0]["deletedCount"].(float64) != 1 {
		t.Fatalf("expected deletedCount=1, got %v", rows[0])
	}

	findOut, err := runWith(t, f, "find", "users")
	if err != nil {
		t.Fatal(err)
	}
	remaining := decodeLines(t, findOut)
	if len(remaining) != 1 || remaining[0]["name"] != "Bob" {
		t.Fatalf("expected only Bob remaining, got %v", remaining)
	}
}

func TestDoDelete_Multi(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "role", Value: "user"}},
		{{Key: "role", Value: "user"}},
		{{Key: "role", Value: "admin"}},
	})

	out, err := runWith(t, f, "delete", "--filter", `{"role": "user"}`, "--multi", "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if rows[0]["deletedCount"].(float64) != 2 {
		t.Fatalf("expected deletedCount=2, got %v", rows[0])
	}
}

func TestDoDelete_NoMatch(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{{{Key: "name", Value: "Alice"}}})

	out, err := runWith(t, f, "delete", "--filter", `{"name": "Nobody"}`, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if rows[0]["deletedCount"].(float64) != 0 {
		t.Fatalf("expected deletedCount=0, got %v", rows[0])
	}
}

func TestDoDelete_FilterFile(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "name", Value: "Alice"}},
		{{Key: "name", Value: "Bob"}},
	})

	filterFile := filepath.Join(t.TempDir(), "filter.json")
	os.WriteFile(filterFile, []byte(`{"name": "Bob"}`), 0644)

	out, err := runWith(t, f, "delete", "--filter-file", filterFile, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 || rows[0]["deletedCount"].(float64) != 1 {
		t.Fatalf("expected deletedCount=1, got %v", rows)
	}
}

// --- doAggregate ---

func TestDoAggregate_Group(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "orders", []bson.D{
		{{Key: "city", Value: "NY"}, {Key: "amount", Value: int32(10)}},
		{{Key: "city", Value: "NY"}, {Key: "amount", Value: int32(20)}},
		{{Key: "city", Value: "LA"}, {Key: "amount", Value: int32(5)}},
	})

	out, err := runWith(t, f, "aggregate",
		"--pipeline", `[{"$group": {"_id": "$city", "total": {"$sum": "$amount"}}}]`,
		"orders",
	)
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups, got %d: %v", len(rows), rows)
	}

	totals := map[string]float64{}
	for _, r := range rows {
		totals[r["_id"].(string)] = r["total"].(float64)
	}
	if totals["NY"] != 30 {
		t.Fatalf("expected NY total=30, got %v", totals["NY"])
	}
	if totals["LA"] != 5 {
		t.Fatalf("expected LA total=5, got %v", totals["LA"])
	}
}

func TestDoAggregate_MissingPipeline(t *testing.T) {
	_, f := newTestEngine(t)
	_, err := runWith(t, f, "aggregate", "orders")
	if err == nil {
		t.Fatal("expected error for missing --pipeline")
	}
}

func TestDoAggregate_BadPipeline(t *testing.T) {
	_, f := newTestEngine(t)
	_, err := runWith(t, f, "aggregate", "--pipeline", `not json`, "orders")
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestDoAggregate_PipelineFile(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "orders", []bson.D{
		{{Key: "status", Value: "open"}},
		{{Key: "status", Value: "open"}},
		{{Key: "status", Value: "closed"}},
	})

	pipelineFile := filepath.Join(t.TempDir(), "pipeline.json")
	os.WriteFile(pipelineFile, []byte(`[{"$sortByCount": "$status"}]`), 0644)

	out, err := runWith(t, f, "aggregate", "--pipeline-file", pipelineFile, "orders")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 2 || rows[0]["_id"] != "open" || rows[0]["count"].(float64) != 2 {
		t.Fatalf("expected sortByCount results, got %v", rows)
	}
}

// --- doDistinct ---

func TestDoDistinct_Basic(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "role", Value: "admin"}},
		{{Key: "role", Value: "user"}},
		{{Key: "role", Value: "user"}},
	})

	out, err := runWith(t, f, "distinct", "--field", "role", "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 {
		t.Fatalf("expected one result line, got %d", len(rows))
	}
	values, ok := rows[0]["values"].([]any)
	if !ok || len(values) != 2 {
		t.Fatalf("expected 2 distinct values, got %v", rows[0])
	}
}

func TestDoDistinct_FilterFile(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{
		{{Key: "team", Value: "red"}, {Key: "role", Value: "admin"}},
		{{Key: "team", Value: "red"}, {Key: "role", Value: "user"}},
		{{Key: "team", Value: "blue"}, {Key: "role", Value: "guest"}},
	})

	filterFile := filepath.Join(t.TempDir(), "filter.json")
	os.WriteFile(filterFile, []byte(`{"team": "red"}`), 0644)

	out, err := runWith(t, f, "distinct", "--field", "role", "--filter-file", filterFile, "users")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	values, ok := rows[0]["values"].([]any)
	if !ok || len(values) != 2 {
		t.Fatalf("expected filtered distinct values, got %v", rows)
	}
}

// --- doCount ---

func TestDoCount_All(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "items", []bson.D{
		{{Key: "x", Value: int32(1)}},
		{{Key: "x", Value: int32(2)}},
		{{Key: "x", Value: int32(3)}},
	})

	out, err := runWith(t, f, "count", "items")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if rows[0]["count"].(float64) != 3 {
		t.Fatalf("expected count=3, got %v", rows[0])
	}
}

func TestDoCount_WithFilter(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "items", []bson.D{
		{{Key: "active", Value: true}},
		{{Key: "active", Value: false}},
		{{Key: "active", Value: true}},
	})

	out, err := runWith(t, f, "count", "--filter", `{"active": true}`, "items")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if rows[0]["count"].(float64) != 2 {
		t.Fatalf("expected count=2, got %v", rows[0])
	}
}

func TestDoCount_Empty(t *testing.T) {
	_, f := newTestEngine(t)
	out, err := runWith(t, f, "count", "nothing")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if rows[0]["count"].(float64) != 0 {
		t.Fatalf("expected count=0, got %v", rows[0])
	}
}

func TestDoCount_FilterFile(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "items", []bson.D{
		{{Key: "active", Value: true}},
		{{Key: "active", Value: false}},
	})

	filterFile := filepath.Join(t.TempDir(), "filter.json")
	os.WriteFile(filterFile, []byte(`{"active": false}`), 0644)

	out, err := runWith(t, f, "count", "--filter-file", filterFile, "items")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 || rows[0]["count"].(float64) != 1 {
		t.Fatalf("expected count=1, got %v", rows)
	}
}

// --- doListDbs ---

func TestDoListDbs_Empty(t *testing.T) {
	_, f := newTestEngine(t)
	out, err := runWith(t, f, "list-dbs")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "" {
		t.Fatalf("expected empty output, got %q", out)
	}
}

func TestDoListDbs_WithData(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("dbA", "col", []bson.D{{{Key: "x", Value: 1}}})
	eng.Insert("dbB", "col", []bson.D{{{Key: "x", Value: 1}}})

	out, err := runWith(t, f, "list-dbs")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 2 {
		t.Fatalf("expected 2 dbs, got %d: %v", len(rows), rows)
	}
	names := map[string]bool{}
	for _, r := range rows {
		names[r["name"].(string)] = true
	}
	if !names["dbA"] || !names["dbB"] {
		t.Fatalf("missing expected db names: %v", names)
	}
}

// --- doListCollections ---

func TestDoListCollections_Empty(t *testing.T) {
	_, f := newTestEngine(t)
	out, err := runWith(t, f, "list-collections")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != "" {
		t.Fatalf("expected empty output, got %q", out)
	}
}

func TestDoListCollections_WithData(t *testing.T) {
	eng, f := newTestEngine(t)
	eng.Insert("test", "users", []bson.D{{{Key: "x", Value: 1}}})
	eng.Insert("test", "orders", []bson.D{{{Key: "x", Value: 1}}})

	out, err := runWith(t, f, "list-collections")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 2 {
		t.Fatalf("expected 2 collections, got %d: %v", len(rows), rows)
	}
	names := map[string]bool{}
	for _, r := range rows {
		names[r["name"].(string)] = true
	}
	if !names["users"] || !names["orders"] {
		t.Fatalf("missing expected collection names: %v", names)
	}
}

// --- schema commands ---

func TestDoSchema_RoundTrip(t *testing.T) {
	_, f := newTestEngine(t)
	schemaFile := filepath.Join(t.TempDir(), "schema.json")
	os.WriteFile(schemaFile, []byte(`{"bsonType":"object","required":["_id"],"properties":{"_id":{"bsonType":"string"},"status":{"enum":["pending","done"]}}}`), 0644)

	out, err := runWith(t, f, "set-schema", "--schema-file", schemaFile, "--description", "Task collection", "tasks")
	if err != nil {
		t.Fatal(err)
	}
	rows := decodeLines(t, out)
	if len(rows) != 1 || rows[0]["ok"].(float64) != 1 {
		t.Fatalf("expected ok=1 from set-schema, got %v", rows)
	}

	out, err = runWith(t, f, "get-schema", "tasks")
	if err != nil {
		t.Fatal(err)
	}
	rows = decodeLines(t, out)
	if len(rows) != 1 {
		t.Fatalf("expected one get-schema row, got %d", len(rows))
	}
	if rows[0]["collection"] != "tasks" || rows[0]["description"] != "Task collection" {
		t.Fatalf("unexpected get-schema row: %v", rows[0])
	}

	schema, ok := rows[0]["schema"].(map[string]any)
	if !ok {
		t.Fatalf("expected schema object, got %T (%v)", rows[0]["schema"], rows[0]["schema"])
	}
	if schema["bsonType"] != "object" {
		t.Fatalf("expected bsonType=object, got %v", schema)
	}

	out, err = runWith(t, f, "list-schemas")
	if err != nil {
		t.Fatal(err)
	}
	rows = decodeLines(t, out)
	if len(rows) != 1 || rows[0]["collection"] != "tasks" {
		t.Fatalf("expected tasks in list-schemas, got %v", rows)
	}

	out, err = runWith(t, f, "delete-schema", "tasks")
	if err != nil {
		t.Fatal(err)
	}
	rows = decodeLines(t, out)
	if len(rows) != 1 || rows[0]["ok"].(float64) != 1 {
		t.Fatalf("expected ok=1 from delete-schema, got %v", rows)
	}

	_, err = runWith(t, f, "get-schema", "tasks")
	if err == nil {
		t.Fatal("expected get-schema to fail after delete")
	}
}

// --- error paths via run() ---

func TestRun_UnknownCommand(t *testing.T) {
	_, f := newTestEngine(t)
	_, err := runWith(t, f, "nope")
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("expected 'unknown command' error, got %v", err)
	}
}

func TestRun_NoCommand_PrintsHelp(t *testing.T) {
	_, f := newTestEngine(t)
	out, err := runWith(t, f)
	if err != nil {
		t.Fatalf("expected no error for no-command (help), got %v", err)
	}
	if !strings.Contains(out, "USAGE:") {
		t.Fatalf("expected usage in output, got %q", out)
	}
}

func TestRun_BadFilterJSON(t *testing.T) {
	_, f := newTestEngine(t)
	_, err := runWith(t, f, "find", "--filter", `{bad}`, "users")
	if err == nil {
		t.Fatal("expected JSON parse error")
	}
}

func TestRun_FilterFileMissing(t *testing.T) {
	_, f := newTestEngine(t)
	_, err := runWith(t, f, "find", "--filter-file", "/no/such/file.json", "users")
	if err == nil {
		t.Fatal("expected error for missing filter file")
	}
}

// --- normalizeArgs ---

func TestNormalizeArgs_NewSyntax(t *testing.T) {
	// Flags already before collection — no change expected.
	in := []string{"--file", "f.json", "find", "--filter", "{}", "users"}
	got := normalizeArgs(in)
	want := []string{"--file", "f.json", "find", "--filter", "{}", "users"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNormalizeArgs_OldSyntax(t *testing.T) {
	// Collection before flags — should be moved to end.
	in := []string{"--file", "f.json", "insert", "tasks", "--doc", `{"x":1}`}
	got := normalizeArgs(in)
	want := []string{"--file", "f.json", "insert", "--doc", `{"x":1}`, "tasks"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNormalizeArgs_NoFlags(t *testing.T) {
	// Collection only, no command flags — no change.
	in := []string{"find", "users"}
	got := normalizeArgs(in)
	want := []string{"find", "users"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestNormalizeArgs_GlobalFlagEquals(t *testing.T) {
	// --file=f.json form (no look-ahead).
	in := []string{"--file=f.json", "find", "users", "--limit", "5"}
	got := normalizeArgs(in)
	want := []string{"--file=f.json", "find", "--limit", "5", "users"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestRun_OldSyntax verifies that collection-before-flags syntax still works.
func TestRun_OldSyntax(t *testing.T) {
	_, f := newTestEngine(t)
	// Old syntax: insert tasks --doc '...'
	out, err := runWith(t, f, "insert", "tasks", "--doc", `{"x":1}`)
	if err != nil {
		t.Fatalf("insert (old syntax): %v", err)
	}
	if !strings.Contains(out, "insertedId") {
		t.Fatalf("unexpected output: %s", out)
	}
	// Old syntax: find tasks --filter '{}'
	out, err = runWith(t, f, "find", "tasks", "--filter", "{}")
	if err != nil {
		t.Fatalf("find (old syntax): %v", err)
	}
	docs := decodeLines(t, out)
	if len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d", len(docs))
	}
}

// --- helpers ---

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}
