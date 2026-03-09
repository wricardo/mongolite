package handler

import (
	"path/filepath"
	"testing"

	"github.com/wricardo/mongolite/internal/engine"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newHandler(t *testing.T) *Handler {
	t.Helper()
	f := filepath.Join(t.TempDir(), "test.json")
	eng, err := engine.New(f)
	if err != nil {
		t.Fatal(err)
	}
	return New(eng)
}

// seed inserts docs into db.coll and fatals on error.
func seed(t *testing.T, h *Handler, db, coll string, docs ...bson.D) {
	t.Helper()
	_, err := h.Engine.Insert(db, coll, docs)
	if err != nil {
		t.Fatal(err)
	}
}

func getField(doc bson.D, key string) any {
	for _, e := range doc {
		if e.Key == key {
			return e.Value
		}
	}
	return nil
}

func assertOK(t *testing.T, resp bson.D) {
	t.Helper()
	ok, _ := getField(resp, "ok").(float64)
	if ok != 1 {
		t.Fatalf("expected ok=1, got %v (errmsg=%v)", ok, getField(resp, "errmsg"))
	}
}

func assertErr(t *testing.T, resp bson.D) {
	t.Helper()
	ok, _ := getField(resp, "ok").(float64)
	if ok != 0 {
		t.Fatalf("expected ok=0 (error), got ok=%v", ok)
	}
}

// ── cmdInsert ─────────────────────────────────────────────────────────────────

func TestCmdInsert_EmptyCollName(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdInsert(h, "db", bson.D{{Key: "insert", Value: ""}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertErr(t, resp)
}

func TestCmdInsert_NoDocs(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdInsert(h, "db", bson.D{
		{Key: "insert", Value: "col"},
		{Key: "documents", Value: bson.A{}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertErr(t, resp)
}

func TestCmdInsert_SingleDoc(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdInsert(h, "db", bson.D{
		{Key: "insert", Value: "col"},
		{Key: "documents", Value: bson.A{bson.D{{Key: "x", Value: int32(1)}}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "n").(int32); n != 1 {
		t.Fatalf("expected n=1, got %v", n)
	}
}

func TestCmdInsert_MultipleDocs(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdInsert(h, "db", bson.D{
		{Key: "insert", Value: "col"},
		{Key: "documents", Value: bson.A{
			bson.D{{Key: "x", Value: int32(1)}},
			bson.D{{Key: "x", Value: int32(2)}},
			bson.D{{Key: "x", Value: int32(3)}},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "n").(int32); n != 3 {
		t.Fatalf("expected n=3, got %v", n)
	}
}

// ── cmdFind ───────────────────────────────────────────────────────────────────

func TestCmdFind_EmptyCollName(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdFind(h, "db", bson.D{{Key: "find", Value: ""}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertErr(t, resp)
}

func TestCmdFind_EmptyCollection(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdFind(h, "db", bson.D{{Key: "find", Value: "col"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	cursor, _ := getField(resp, "cursor").(bson.D)
	batch, _ := getField(cursor, "firstBatch").(bson.A)
	if len(batch) != 0 {
		t.Fatalf("expected empty batch, got %d docs", len(batch))
	}
}

func TestCmdFind_WithFilter(t *testing.T) {
	h := newHandler(t)
	seed(t, h, "db", "col",
		bson.D{{Key: "v", Value: int32(1)}},
		bson.D{{Key: "v", Value: int32(2)}},
		bson.D{{Key: "v", Value: int32(3)}},
	)
	resp, err := cmdFind(h, "db", bson.D{
		{Key: "find", Value: "col"},
		{Key: "filter", Value: bson.D{{Key: "v", Value: bson.D{{Key: "$gt", Value: int32(1)}}}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	cursor, _ := getField(resp, "cursor").(bson.D)
	batch, _ := getField(cursor, "firstBatch").(bson.A)
	if len(batch) != 2 {
		t.Fatalf("expected 2 results, got %d", len(batch))
	}
}

func TestCmdFind_Projection(t *testing.T) {
	h := newHandler(t)
	seed(t, h, "db", "col", bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}})
	resp, err := cmdFind(h, "db", bson.D{
		{Key: "find", Value: "col"},
		{Key: "projection", Value: bson.D{{Key: "a", Value: int32(1)}, {Key: "_id", Value: int32(0)}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	cursor, _ := getField(resp, "cursor").(bson.D)
	batch, _ := getField(cursor, "firstBatch").(bson.A)
	if len(batch) != 1 {
		t.Fatal("expected 1 doc")
	}
	doc, _ := batch[0].(bson.D)
	if getField(doc, "b") != nil {
		t.Error("field 'b' should have been projected out")
	}
	if getField(doc, "a") == nil {
		t.Error("field 'a' should be present")
	}
}

func TestCmdFind_SkipAndLimit(t *testing.T) {
	h := newHandler(t)
	for i := range 5 {
		seed(t, h, "db", "col", bson.D{{Key: "i", Value: int32(i)}})
	}
	resp, err := cmdFind(h, "db", bson.D{
		{Key: "find", Value: "col"},
		{Key: "skip", Value: int32(1)},
		{Key: "limit", Value: int32(2)},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	cursor, _ := getField(resp, "cursor").(bson.D)
	batch, _ := getField(cursor, "firstBatch").(bson.A)
	if len(batch) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(batch))
	}
}

func TestCmdFind_BatchSizeUsedWhenLimitZero(t *testing.T) {
	h := newHandler(t)
	for i := range 5 {
		seed(t, h, "db", "col", bson.D{{Key: "i", Value: int32(i)}})
	}
	resp, err := cmdFind(h, "db", bson.D{
		{Key: "find", Value: "col"},
		{Key: "batchSize", Value: int32(2)},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	cursor, _ := getField(resp, "cursor").(bson.D)
	batch, _ := getField(cursor, "firstBatch").(bson.A)
	if len(batch) != 2 {
		t.Fatalf("batchSize not applied: expected 2 docs, got %d", len(batch))
	}
}

// ── cmdUpdate ─────────────────────────────────────────────────────────────────

func TestCmdUpdate_EmptyCollName(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdUpdate(h, "db", bson.D{{Key: "update", Value: ""}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertErr(t, resp)
}

func TestCmdUpdate_NoUpdatesArray(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdUpdate(h, "db", bson.D{{Key: "update", Value: "col"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "n").(int32); n != 0 {
		t.Fatalf("expected n=0, got %v", n)
	}
}

func TestCmdUpdate_SingleUpdate(t *testing.T) {
	h := newHandler(t)
	seed(t, h, "db", "col", bson.D{{Key: "x", Value: int32(1)}})
	resp, err := cmdUpdate(h, "db", bson.D{
		{Key: "update", Value: "col"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "x", Value: int32(1)}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(99)}}}}},
			},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "nModified").(int32); n != 1 {
		t.Fatalf("expected nModified=1, got %v", n)
	}
}

func TestCmdUpdate_Upsert(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdUpdate(h, "db", bson.D{
		{Key: "update", Value: "col"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "x", Value: int32(999)}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(999)}}}}},
				{Key: "upsert", Value: true},
			},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	upserted, _ := getField(resp, "upserted").(bson.A)
	if len(upserted) != 1 {
		t.Fatalf("expected upserted array with 1 entry, got %v", upserted)
	}
}

func TestCmdUpdate_MultiFlag(t *testing.T) {
	h := newHandler(t)
	seed(t, h, "db", "col",
		bson.D{{Key: "x", Value: int32(1)}},
		bson.D{{Key: "x", Value: int32(1)}},
	)
	resp, err := cmdUpdate(h, "db", bson.D{
		{Key: "update", Value: "col"},
		{Key: "updates", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "x", Value: int32(1)}}},
				{Key: "u", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(2)}}}}},
				{Key: "multi", Value: true},
			},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "nModified").(int32); n != 2 {
		t.Fatalf("expected nModified=2, got %v", n)
	}
}

// ── cmdDelete ─────────────────────────────────────────────────────────────────

func TestCmdDelete_EmptyCollName(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdDelete(h, "db", bson.D{{Key: "delete", Value: ""}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertErr(t, resp)
}

func TestCmdDelete_Limit1_DeletesOnlyOne(t *testing.T) {
	h := newHandler(t)
	seed(t, h, "db", "col",
		bson.D{{Key: "x", Value: int32(1)}},
		bson.D{{Key: "x", Value: int32(1)}},
	)
	resp, err := cmdDelete(h, "db", bson.D{
		{Key: "delete", Value: "col"},
		{Key: "deletes", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "x", Value: int32(1)}}},
				{Key: "limit", Value: int32(1)},
			},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "n").(int32); n != 1 {
		t.Fatalf("limit=1 should delete exactly 1, got n=%v", n)
	}
}

// This test documents the current limit=0 → multi-delete semantics.
// A missing "limit" field causes getInt64Field to return 0, which means
// limitVal==0 → multi=true → all matching docs are deleted.
func TestCmdDelete_Limit0_DeletesAll(t *testing.T) {
	h := newHandler(t)
	seed(t, h, "db", "col",
		bson.D{{Key: "x", Value: int32(1)}},
		bson.D{{Key: "x", Value: int32(1)}},
		bson.D{{Key: "x", Value: int32(2)}},
	)
	resp, err := cmdDelete(h, "db", bson.D{
		{Key: "delete", Value: "col"},
		{Key: "deletes", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "x", Value: int32(1)}}},
				{Key: "limit", Value: int32(0)},
			},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "n").(int32); n != 2 {
		t.Fatalf("limit=0 should delete all matching (2), got n=%v", n)
	}
}

// This test documents that omitting "limit" has the same effect as limit=0.
// This is the silent multi-delete footgun: a client that forgets to set limit
// will delete all matching documents instead of one.
func TestCmdDelete_MissingLimit_DeletesAll(t *testing.T) {
	h := newHandler(t)
	seed(t, h, "db", "col",
		bson.D{{Key: "x", Value: int32(1)}},
		bson.D{{Key: "x", Value: int32(1)}},
	)
	resp, err := cmdDelete(h, "db", bson.D{
		{Key: "delete", Value: "col"},
		{Key: "deletes", Value: bson.A{
			bson.D{
				{Key: "q", Value: bson.D{{Key: "x", Value: int32(1)}}},
				// no "limit" field
			},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "n").(int32); n != 2 {
		t.Fatalf("missing limit should behave as limit=0 (delete all), got n=%v", n)
	}
}

func TestCmdDelete_NoDeletesArray(t *testing.T) {
	h := newHandler(t)
	seed(t, h, "db", "col", bson.D{{Key: "x", Value: int32(1)}})
	resp, err := cmdDelete(h, "db", bson.D{{Key: "delete", Value: "col"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "n").(int32); n != 0 {
		t.Fatalf("expected n=0, got %v", n)
	}
}

// ── cmdBulkWrite ──────────────────────────────────────────────────────────────

func TestCmdBulkWrite_InsertOne(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdBulkWrite(h, "db", bson.D{
		{Key: "bulkWrite", Value: "col"},
		{Key: "ops", Value: bson.A{
			bson.D{{Key: "insertOne", Value: bson.D{{Key: "document", Value: bson.D{{Key: "x", Value: int32(1)}}}}}},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "nInserted").(int32); n != 1 {
		t.Fatalf("expected nInserted=1, got %v", n)
	}
}

func TestCmdBulkWrite_MixedOps(t *testing.T) {
	h := newHandler(t)
	seed(t, h, "db", "col",
		bson.D{{Key: "x", Value: int32(1)}},
		bson.D{{Key: "x", Value: int32(2)}},
	)
	resp, err := cmdBulkWrite(h, "db", bson.D{
		{Key: "bulkWrite", Value: "col"},
		{Key: "ops", Value: bson.A{
			bson.D{{Key: "insertOne", Value: bson.D{{Key: "document", Value: bson.D{{Key: "x", Value: int32(3)}}}}}},
			bson.D{{Key: "updateOne", Value: bson.D{
				{Key: "filter", Value: bson.D{{Key: "x", Value: int32(1)}}},
				{Key: "update", Value: bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(10)}}}}},
			}}},
			bson.D{{Key: "deleteOne", Value: bson.D{
				{Key: "filter", Value: bson.D{{Key: "x", Value: int32(2)}}},
			}}},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	if n, _ := getField(resp, "nInserted").(int32); n != 1 {
		t.Fatalf("expected nInserted=1, got %v", n)
	}
	if n, _ := getField(resp, "nModified").(int32); n != 1 {
		t.Fatalf("expected nModified=1, got %v", n)
	}
	if n, _ := getField(resp, "nRemoved").(int32); n != 1 {
		t.Fatalf("expected nRemoved=1, got %v", n)
	}
}

func TestCmdBulkWrite_UnknownOpSkipped(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdBulkWrite(h, "db", bson.D{
		{Key: "bulkWrite", Value: "col"},
		{Key: "ops", Value: bson.A{
			bson.D{{Key: "unknownOp", Value: bson.D{{Key: "x", Value: int32(1)}}}},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
}

// ── cmdAggregate ──────────────────────────────────────────────────────────────

func TestCmdAggregate_EmptyCollName(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdAggregate(h, "db", bson.D{{Key: "aggregate", Value: ""}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertErr(t, resp)
}

func TestCmdAggregate_NoPipeline(t *testing.T) {
	h := newHandler(t)
	resp, err := cmdAggregate(h, "db", bson.D{{Key: "aggregate", Value: "col"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertErr(t, resp)
}

func TestCmdAggregate_MatchStage(t *testing.T) {
	h := newHandler(t)
	seed(t, h, "db", "col",
		bson.D{{Key: "v", Value: int32(1)}},
		bson.D{{Key: "v", Value: int32(2)}},
	)
	resp, err := cmdAggregate(h, "db", bson.D{
		{Key: "aggregate", Value: "col"},
		{Key: "pipeline", Value: bson.A{
			bson.D{{Key: "$match", Value: bson.D{{Key: "v", Value: int32(1)}}}},
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertOK(t, resp)
	cursor, _ := getField(resp, "cursor").(bson.D)
	batch, _ := getField(cursor, "firstBatch").(bson.A)
	if len(batch) != 1 {
		t.Fatalf("expected 1 result, got %d", len(batch))
	}
}
