package engine

import (
	"errors"
	"path/filepath"
	"sort"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ---- helpers ----

func newEng(t *testing.T) (*Engine, string) {
	t.Helper()
	f := filepath.Join(t.TempDir(), "test.json")
	eng, err := New(f)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return eng, f
}

func reloadEng(t *testing.T, path string) *Engine {
	t.Helper()
	eng, err := New(path)
	if err != nil {
		t.Fatalf("reloadEng: %v", err)
	}
	return eng
}

func mustInsert(t *testing.T, eng *Engine, db, coll string, docs ...bson.D) {
	t.Helper()
	_, err := eng.Insert(db, coll, docs)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

// ---- Engine.New ----

func TestNew_MissingFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "new.json")
	eng, err := New(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if eng == nil {
		t.Fatal("expected non-nil engine")
	}
}

// ---- Engine.Insert ----

func TestInsert_AutoID(t *testing.T) {
	eng, _ := newEng(t)
	ids, err := eng.Insert("db", "col", []bson.D{
		{{Key: "name", Value: "Alice"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] == nil {
		t.Fatalf("expected 1 non-nil id, got %v", ids)
	}
}

func TestInsert_ExplicitID(t *testing.T) {
	eng, _ := newEng(t)
	ids, err := eng.Insert("db", "col", []bson.D{
		{{Key: "_id", Value: "myid"}, {Key: "x", Value: int32(1)}},
	})
	if err != nil || len(ids) == 0 {
		t.Fatalf("insert: %v", err)
	}
	if ids[0] != "myid" {
		t.Fatalf("expected id=myid, got %v", ids[0])
	}
}

func TestInsert_Multiple(t *testing.T) {
	eng, _ := newEng(t)
	ids, err := eng.Insert("db", "col", []bson.D{
		{{Key: "n", Value: int32(1)}},
		{{Key: "n", Value: int32(2)}},
		{{Key: "n", Value: int32(3)}},
	})
	if err != nil || len(ids) != 3 {
		t.Fatalf("expected 3 ids, got %v err=%v", ids, err)
	}
}

func TestInsert_Persistence(t *testing.T) {
	eng, path := newEng(t)
	mustInsert(t, eng, "db", "col", bson.D{{Key: "n", Value: int32(1)}})

	eng2 := reloadEng(t, path)
	docs, err := eng2.Find("db", "col", bson.D{}, nil, 0, 0)
	if err != nil || len(docs) != 1 {
		t.Fatalf("expected 1 doc after reload, got %d err=%v", len(docs), err)
	}
}

func TestInsert_UniqueIndexViolation(t *testing.T) {
	eng, _ := newEng(t)
	eng.CreateIndexes("db", "col", []IndexSpec{
		{Name: "email_1", Keys: bson.D{{Key: "email", Value: int32(1)}}, Unique: true},
	})
	mustInsert(t, eng, "db", "col", bson.D{{Key: "email", Value: "a@b.com"}})

	_, err := eng.Insert("db", "col", []bson.D{{{Key: "email", Value: "a@b.com"}}})
	if err == nil {
		t.Fatal("expected unique index violation")
	}
	var dupErr *DuplicateKeyError
	if !errors.As(err, &dupErr) {
		t.Fatalf("expected *DuplicateKeyError, got %T: %v", err, err)
	}
}

// ---- Engine.Find ----

func TestFind_NonexistentDB(t *testing.T) {
	eng, _ := newEng(t)
	docs, err := eng.Find("nodb", "col", bson.D{}, nil, 0, 0)
	if err != nil || docs != nil {
		t.Fatalf("expected nil, nil; got %v, %v", docs, err)
	}
}

func TestFind_NonexistentCollection(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "other", bson.D{{Key: "x", Value: 1}})
	docs, err := eng.Find("db", "col", bson.D{}, nil, 0, 0)
	if err != nil || docs != nil {
		t.Fatalf("expected nil, nil; got %v, %v", docs, err)
	}
}

func TestFind_All(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "n", Value: int32(1)}},
		bson.D{{Key: "n", Value: int32(2)}},
	)
	docs, err := eng.Find("db", "col", bson.D{}, nil, 0, 0)
	if err != nil || len(docs) != 2 {
		t.Fatalf("expected 2, got %d, err=%v", len(docs), err)
	}
}

func TestFind_WithFilter(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "age", Value: int32(25)}},
		bson.D{{Key: "age", Value: int32(30)}},
		bson.D{{Key: "age", Value: int32(20)}},
	)
	docs, err := eng.Find("db", "col",
		bson.D{{Key: "age", Value: bson.D{{Key: "$gt", Value: int32(24)}}}},
		nil, 0, 0,
	)
	if err != nil || len(docs) != 2 {
		t.Fatalf("expected 2, got %d, err=%v", len(docs), err)
	}
}

func TestFind_SortAscending(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "n", Value: int32(3)}},
		bson.D{{Key: "n", Value: int32(1)}},
		bson.D{{Key: "n", Value: int32(2)}},
	)
	docs, _ := eng.Find("db", "col", bson.D{}, bson.D{{Key: "n", Value: int32(1)}}, 0, 0)
	if len(docs) != 3 {
		t.Fatalf("expected 3 docs")
	}
	n0, _ := GetField(docs[0], "n")
	n1, _ := GetField(docs[1], "n")
	n2, _ := GetField(docs[2], "n")
	if n0 != int32(1) || n1 != int32(2) || n2 != int32(3) {
		t.Fatalf("wrong asc order: %v %v %v", n0, n1, n2)
	}
}

func TestFind_SortDescending(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "n", Value: int32(1)}},
		bson.D{{Key: "n", Value: int32(3)}},
		bson.D{{Key: "n", Value: int32(2)}},
	)
	docs, _ := eng.Find("db", "col", bson.D{}, bson.D{{Key: "n", Value: int32(-1)}}, 0, 0)
	n0, _ := GetField(docs[0], "n")
	if n0 != int32(3) {
		t.Fatalf("expected desc first=3, got %v", n0)
	}
}

func TestFind_SkipLimit(t *testing.T) {
	eng, _ := newEng(t)
	for i := 0; i < 5; i++ {
		mustInsert(t, eng, "db", "col", bson.D{{Key: "i", Value: int32(i)}})
	}
	// sort asc by i, skip 1, limit 2 → [1,2]
	docs, _ := eng.Find("db", "col", bson.D{}, bson.D{{Key: "i", Value: int32(1)}}, 1, 2)
	if len(docs) != 2 {
		t.Fatalf("expected 2, got %d", len(docs))
	}
	i0, _ := GetField(docs[0], "i")
	if i0 != int32(1) {
		t.Fatalf("expected i=1, got %v", i0)
	}
}

func TestFind_SkipBeyondEnd(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col", bson.D{{Key: "x", Value: 1}})
	docs, err := eng.Find("db", "col", bson.D{}, nil, 100, 0)
	if err != nil || docs != nil {
		t.Fatalf("expected nil, nil; got %v, %v", docs, err)
	}
}

// ---- Engine.Update ----

func TestUpdate_SetSingle(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "name", Value: "Alice"}, {Key: "age", Value: int32(30)}},
	)
	matched, modified, _, err := eng.Update("db", "col",
		bson.D{{Key: "name", Value: "Alice"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: int32(31)}}}},
		false, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if matched != 1 || modified != 1 {
		t.Fatalf("expected matched=1 modified=1, got %d/%d", matched, modified)
	}
	docs, _ := eng.Find("db", "col", bson.D{{Key: "name", Value: "Alice"}}, nil, 0, 0)
	age, _ := GetField(docs[0], "age")
	if age != int32(31) {
		t.Fatalf("expected age=31, got %v", age)
	}
}

func TestUpdate_Multi(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "role", Value: "user"}},
		bson.D{{Key: "role", Value: "user"}},
		bson.D{{Key: "role", Value: "admin"}},
	)
	matched, modified, _, err := eng.Update("db", "col",
		bson.D{{Key: "role", Value: "user"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "active", Value: true}}}},
		true, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if matched != 2 || modified != 2 {
		t.Fatalf("expected 2/2, got %d/%d", matched, modified)
	}
}

func TestUpdate_SingleStopsAtFirst(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "x", Value: int32(1)}},
		bson.D{{Key: "x", Value: int32(1)}},
	)
	matched, _, _, _ := eng.Update("db", "col",
		bson.D{{Key: "x", Value: int32(1)}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "updated", Value: true}}}},
		false, false,
	)
	if matched != 1 {
		t.Fatalf("single update should match exactly 1, got %d", matched)
	}
}

func TestUpdate_Upsert(t *testing.T) {
	eng, _ := newEng(t)
	matched, modified, upsertedID, err := eng.Update("db", "col",
		bson.D{{Key: "name", Value: "Bob"}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: int32(25)}}}},
		false, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if matched != 0 || modified != 0 || upsertedID == nil {
		t.Fatalf("expected upsert: matched=%d modified=%d upsertedID=%v", matched, modified, upsertedID)
	}
	docs, _ := eng.Find("db", "col", bson.D{{Key: "name", Value: "Bob"}}, nil, 0, 0)
	if len(docs) != 1 {
		t.Fatal("upserted doc not found")
	}
}

func TestUpdate_NoMatch(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col", bson.D{{Key: "x", Value: int32(1)}})
	matched, modified, _, _ := eng.Update("db", "col",
		bson.D{{Key: "x", Value: int32(99)}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(0)}}}},
		false, false,
	)
	if matched != 0 || modified != 0 {
		t.Fatalf("expected 0/0, got %d/%d", matched, modified)
	}
}

// ---- Engine.Delete ----

func TestDelete_Single(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "n", Value: int32(1)}},
		bson.D{{Key: "n", Value: int32(2)}},
	)
	deleted, err := eng.Delete("db", "col", bson.D{{Key: "n", Value: int32(1)}}, false)
	if err != nil || deleted != 1 {
		t.Fatalf("expected 1, nil; got %d, %v", deleted, err)
	}
	docs, _ := eng.Find("db", "col", bson.D{}, nil, 0, 0)
	if len(docs) != 1 {
		t.Fatalf("expected 1 remaining, got %d", len(docs))
	}
}

func TestDelete_Multi(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "tag", Value: "x"}},
		bson.D{{Key: "tag", Value: "x"}},
		bson.D{{Key: "tag", Value: "y"}},
	)
	deleted, err := eng.Delete("db", "col", bson.D{{Key: "tag", Value: "x"}}, true)
	if err != nil || deleted != 2 {
		t.Fatalf("expected 2, nil; got %d, %v", deleted, err)
	}
	docs, _ := eng.Find("db", "col", bson.D{}, nil, 0, 0)
	if len(docs) != 1 {
		t.Fatalf("expected 1 remaining, got %d", len(docs))
	}
}

func TestDelete_NoMatch(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col", bson.D{{Key: "x", Value: int32(1)}})
	deleted, err := eng.Delete("db", "col", bson.D{{Key: "x", Value: int32(99)}}, false)
	if err != nil || deleted != 0 {
		t.Fatalf("expected 0, nil; got %d, %v", deleted, err)
	}
}

func TestDelete_NonexistentDB(t *testing.T) {
	eng, _ := newEng(t)
	deleted, err := eng.Delete("noDB", "col", bson.D{}, false)
	if err != nil || deleted != 0 {
		t.Fatalf("expected 0, nil; got %d, %v", deleted, err)
	}
}

func TestDelete_Persistence(t *testing.T) {
	eng, path := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "n", Value: int32(1)}},
		bson.D{{Key: "n", Value: int32(2)}},
	)
	eng.Delete("db", "col", bson.D{{Key: "n", Value: int32(1)}}, false)

	eng2 := reloadEng(t, path)
	docs, _ := eng2.Find("db", "col", bson.D{}, nil, 0, 0)
	if len(docs) != 1 {
		t.Fatalf("expected 1 after reload, got %d", len(docs))
	}
}

// ---- Engine.Count ----

func TestCount_All(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "x", Value: 2}},
		bson.D{{Key: "x", Value: 3}},
	)
	n, err := eng.Count("db", "col", bson.D{})
	if err != nil || n != 3 {
		t.Fatalf("expected 3, nil; got %d, %v", n, err)
	}
}

func TestCount_WithFilter(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "active", Value: true}},
		bson.D{{Key: "active", Value: false}},
		bson.D{{Key: "active", Value: true}},
	)
	n, _ := eng.Count("db", "col", bson.D{{Key: "active", Value: true}})
	if n != 2 {
		t.Fatalf("expected 2, got %d", n)
	}
}

func TestCount_NonexistentCollection(t *testing.T) {
	eng, _ := newEng(t)
	n, err := eng.Count("db", "col", bson.D{})
	if err != nil || n != 0 {
		t.Fatalf("expected 0, nil; got %d, %v", n, err)
	}
}

// ---- Engine.FindAndModify ----

func TestFindAndModify_ReturnPre(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col", bson.D{{Key: "x", Value: int32(1)}})
	doc, err := eng.FindAndModify("db", "col",
		bson.D{{Key: "x", Value: int32(1)}}, nil,
		bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(2)}}}},
		false, false, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	x, _ := GetField(doc, "x")
	if x != int32(1) {
		t.Fatalf("expected pre-doc x=1, got %v", x)
	}
}

func TestFindAndModify_ReturnPost(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col", bson.D{{Key: "x", Value: int32(1)}})
	doc, err := eng.FindAndModify("db", "col",
		bson.D{{Key: "x", Value: int32(1)}}, nil,
		bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: int32(2)}}}},
		false, true, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	x, _ := GetField(doc, "x")
	if x != int32(2) {
		t.Fatalf("expected post-doc x=2, got %v", x)
	}
}

func TestFindAndModify_Remove(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col", bson.D{{Key: "x", Value: int32(5)}})
	doc, err := eng.FindAndModify("db", "col",
		bson.D{{Key: "x", Value: int32(5)}}, nil,
		nil, true, false, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	x, _ := GetField(doc, "x")
	if x != int32(5) {
		t.Fatalf("expected removed doc x=5, got %v", x)
	}
	n, _ := eng.Count("db", "col", bson.D{})
	if n != 0 {
		t.Fatal("doc not removed")
	}
}

func TestFindAndModify_Upsert(t *testing.T) {
	eng, _ := newEng(t)
	doc, err := eng.FindAndModify("db", "col",
		bson.D{{Key: "name", Value: "Eve"}}, nil,
		bson.D{{Key: "$set", Value: bson.D{{Key: "age", Value: int32(28)}}}},
		false, true, true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if doc == nil {
		t.Fatal("expected upserted doc")
	}
	name, _ := GetField(doc, "name")
	if name != "Eve" {
		t.Fatalf("expected name=Eve, got %v", name)
	}
}

func TestFindAndModify_NoMatch(t *testing.T) {
	eng, _ := newEng(t)
	doc, err := eng.FindAndModify("db", "col",
		bson.D{{Key: "x", Value: int32(99)}}, nil,
		nil, false, false, false,
	)
	if err != nil || doc != nil {
		t.Fatalf("expected nil, nil; got %v, %v", doc, err)
	}
}

// ---- Engine.Aggregate ----

func TestAggregate_NonexistentCollection(t *testing.T) {
	eng, _ := newEng(t)
	docs, err := eng.Aggregate("db", "col", []bson.D{})
	if err != nil || docs != nil {
		t.Fatalf("expected nil, nil; got %v, %v", docs, err)
	}
}

func TestAggregate_Match(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "n", Value: int32(1)}},
		bson.D{{Key: "n", Value: int32(2)}},
		bson.D{{Key: "n", Value: int32(3)}},
	)
	pipeline := []bson.D{
		{{Key: "$match", Value: bson.D{{Key: "n", Value: bson.D{{Key: "$gte", Value: int32(2)}}}}}},
	}
	docs, err := eng.Aggregate("db", "col", pipeline)
	if err != nil || len(docs) != 2 {
		t.Fatalf("expected 2, got %d, err=%v", len(docs), err)
	}
}

func TestAggregate_Group_Sum(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "city", Value: "NY"}, {Key: "score", Value: int32(10)}},
		bson.D{{Key: "city", Value: "NY"}, {Key: "score", Value: int32(20)}},
		bson.D{{Key: "city", Value: "LA"}, {Key: "score", Value: int32(5)}},
	)
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$city"},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$score"}}},
		}}},
	}
	docs, err := eng.Aggregate("db", "col", pipeline)
	if err != nil || len(docs) != 2 {
		t.Fatalf("expected 2 groups, got %d, err=%v", len(docs), err)
	}
	byCity := make(map[interface{}]interface{})
	for _, d := range docs {
		id, _ := GetField(d, "_id")
		total, _ := GetField(d, "total")
		byCity[id] = total
	}
	if byCity["NY"] != int64(30) {
		t.Fatalf("expected NY total=30, got %v", byCity["NY"])
	}
	if byCity["LA"] != int64(5) {
		t.Fatalf("expected LA total=5, got %v", byCity["LA"])
	}
}

func TestAggregate_Count(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "x", Value: 1}},
		bson.D{{Key: "x", Value: 2}},
	)
	pipeline := []bson.D{
		{{Key: "$count", Value: "total"}},
	}
	docs, err := eng.Aggregate("db", "col", pipeline)
	if err != nil || len(docs) != 1 {
		t.Fatalf("expected 1 doc, got %d, err=%v", len(docs), err)
	}
	total, _ := GetField(docs[0], "total")
	if total != int64(2) {
		t.Fatalf("expected total=2, got %v", total)
	}
}

func TestAggregate_SortSkipLimit(t *testing.T) {
	eng, _ := newEng(t)
	for i := 0; i < 5; i++ {
		mustInsert(t, eng, "db", "col", bson.D{{Key: "i", Value: int32(i)}})
	}
	pipeline := []bson.D{
		{{Key: "$sort", Value: bson.D{{Key: "i", Value: int32(1)}}}},
		{{Key: "$skip", Value: int64(1)}},
		{{Key: "$limit", Value: int64(2)}},
	}
	docs, err := eng.Aggregate("db", "col", pipeline)
	if err != nil || len(docs) != 2 {
		t.Fatalf("expected 2, got %d, err=%v", len(docs), err)
	}
	i0, _ := GetField(docs[0], "i")
	if i0 != int32(1) {
		t.Fatalf("expected i=1, got %v", i0)
	}
}

func TestAggregate_Project(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "name", Value: "Alice"}, {Key: "age", Value: int32(30)}, {Key: "secret", Value: "x"}},
	)
	pipeline := []bson.D{
		{{Key: "$project", Value: bson.D{{Key: "name", Value: int32(1)}, {Key: "_id", Value: int32(0)}}}},
	}
	docs, err := eng.Aggregate("db", "col", pipeline)
	if err != nil || len(docs) != 1 {
		t.Fatalf("expected 1, got %d, err=%v", len(docs), err)
	}
	_, hasAge := GetField(docs[0], "age")
	_, hasName := GetField(docs[0], "name")
	_, hasID := GetField(docs[0], "_id")
	if hasAge || !hasName || hasID {
		t.Fatalf("project: hasAge=%v hasName=%v hasID=%v", hasAge, hasName, hasID)
	}
}

func TestAggregate_Unwind(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col",
		bson.D{{Key: "tags", Value: bson.A{"a", "b", "c"}}},
	)
	pipeline := []bson.D{
		{{Key: "$unwind", Value: "$tags"}},
	}
	docs, err := eng.Aggregate("db", "col", pipeline)
	if err != nil || len(docs) != 3 {
		t.Fatalf("expected 3, got %d, err=%v", len(docs), err)
	}
}

// ---- Database/Collection management ----

func TestListDatabases(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db1", "col", bson.D{{Key: "x", Value: 1}})
	mustInsert(t, eng, "db2", "col", bson.D{{Key: "x", Value: 2}})
	dbs := eng.ListDatabases()
	sort.Strings(dbs)
	if len(dbs) != 2 || dbs[0] != "db1" || dbs[1] != "db2" {
		t.Fatalf("expected [db1,db2], got %v", dbs)
	}
}

func TestDropDatabase(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col", bson.D{{Key: "x", Value: 1}})
	if err := eng.DropDatabase("db"); err != nil {
		t.Fatal(err)
	}
	if len(eng.ListDatabases()) != 0 {
		t.Fatal("expected empty db list after drop")
	}
}

func TestListCollections(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "alpha", bson.D{{Key: "x", Value: 1}})
	mustInsert(t, eng, "db", "beta", bson.D{{Key: "x", Value: 2}})
	colls := eng.ListCollections("db")
	sort.Strings(colls)
	if len(colls) != 2 || colls[0] != "alpha" || colls[1] != "beta" {
		t.Fatalf("expected [alpha,beta], got %v", colls)
	}
}

func TestListCollections_NonexistentDB(t *testing.T) {
	eng, _ := newEng(t)
	colls := eng.ListCollections("noDB")
	if colls != nil {
		t.Fatalf("expected nil, got %v", colls)
	}
}

func TestCreateCollection(t *testing.T) {
	eng, _ := newEng(t)
	if err := eng.CreateCollection("db", "col"); err != nil {
		t.Fatal(err)
	}
	colls := eng.ListCollections("db")
	if len(colls) != 1 || colls[0] != "col" {
		t.Fatalf("expected [col], got %v", colls)
	}
}

func TestDropCollection(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col", bson.D{{Key: "x", Value: 1}})
	if err := eng.DropCollection("db", "col"); err != nil {
		t.Fatal(err)
	}
	colls := eng.ListCollections("db")
	if len(colls) != 0 {
		t.Fatalf("expected empty, got %v", colls)
	}
}

func TestDropCollection_NonexistentDB(t *testing.T) {
	eng, _ := newEng(t)
	if err := eng.DropCollection("noDB", "col"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---- Indexes ----

func TestCreateListDropIndexes(t *testing.T) {
	eng, _ := newEng(t)
	err := eng.CreateIndexes("db", "col", []IndexSpec{
		{Name: "email_1", Keys: bson.D{{Key: "email", Value: int32(1)}}, Unique: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	idxs := eng.ListIndexes("db", "col")
	if len(idxs) != 2 { // _id_ + email_1
		t.Fatalf("expected 2 indexes, got %d", len(idxs))
	}

	if err := eng.DropIndexes("db", "col", "email_1"); err != nil {
		t.Fatal(err)
	}
	idxs = eng.ListIndexes("db", "col")
	if len(idxs) != 1 { // only _id_ remains
		t.Fatalf("expected 1 index after drop, got %d", len(idxs))
	}
}

func TestDropIndexes_All(t *testing.T) {
	eng, _ := newEng(t)
	eng.CreateIndexes("db", "col", []IndexSpec{
		{Name: "a_1", Keys: bson.D{{Key: "a", Value: int32(1)}}},
		{Name: "b_1", Keys: bson.D{{Key: "b", Value: int32(1)}}},
	})
	eng.DropIndexes("db", "col", "*")
	idxs := eng.ListIndexes("db", "col")
	if len(idxs) != 1 { // only _id_
		t.Fatalf("expected 1, got %d", len(idxs))
	}
}

func TestCreateIndexes_NoDuplicate(t *testing.T) {
	eng, _ := newEng(t)
	eng.CreateIndexes("db", "col", []IndexSpec{{Name: "x_1", Keys: bson.D{{Key: "x", Value: int32(1)}}}})
	eng.CreateIndexes("db", "col", []IndexSpec{{Name: "x_1", Keys: bson.D{{Key: "x", Value: int32(1)}}}})
	idxs := eng.ListIndexes("db", "col")
	if len(idxs) != 2 { // _id_ + x_1
		t.Fatalf("expected 2, got %d", len(idxs))
	}
}

func TestListIndexes_DefaultIndex(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "col", bson.D{{Key: "x", Value: 1}})
	idxs := eng.ListIndexes("db", "col")
	if len(idxs) == 0 || idxs[0].Name != "_id_" {
		t.Fatalf("expected _id_ as first index, got %v", idxs)
	}
}

func TestDropIndexes_NonexistentDB(t *testing.T) {
	eng, _ := newEng(t)
	if err := eng.DropIndexes("noDB", "col", "x_1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreateIndexes_AutoName(t *testing.T) {
	eng, _ := newEng(t)
	err := eng.CreateIndexes("db", "col", []IndexSpec{
		{Keys: bson.D{{Key: "email", Value: int32(1)}}}, // no Name → auto-generated
	})
	if err != nil {
		t.Fatal(err)
	}
	idxs := eng.ListIndexes("db", "col")
	found := false
	for _, idx := range idxs {
		if idx.Name == "email_1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("auto-generated name 'email_1' not found in %v", idxs)
	}
}
