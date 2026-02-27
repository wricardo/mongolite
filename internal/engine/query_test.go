package engine

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ---- MatchDoc ----

func TestMatchDoc_EmptyFilter(t *testing.T) {
	doc := bson.D{{Key: "x", Value: 1}}
	if !MatchDoc(doc, bson.D{}) {
		t.Fatal("empty filter should match everything")
	}
}

func TestMatchDoc_DirectEquality(t *testing.T) {
	doc := bson.D{{Key: "name", Value: "Alice"}}
	if !MatchDoc(doc, bson.D{{Key: "name", Value: "Alice"}}) {
		t.Fatal("direct equality should match")
	}
	if MatchDoc(doc, bson.D{{Key: "name", Value: "Bob"}}) {
		t.Fatal("different value should not match")
	}
}

func TestMatchDoc_FieldMissing(t *testing.T) {
	doc := bson.D{{Key: "x", Value: 1}}
	// Filtering on missing field with non-nil value should not match
	if MatchDoc(doc, bson.D{{Key: "y", Value: 1}}) {
		t.Fatal("missing field should not match non-nil value")
	}
}

func TestMatchDoc_DotNotation(t *testing.T) {
	doc := bson.D{{Key: "addr", Value: bson.D{{Key: "city", Value: "NY"}}}}
	if !MatchDoc(doc, bson.D{{Key: "addr.city", Value: "NY"}}) {
		t.Fatal("dot notation should match")
	}
	if MatchDoc(doc, bson.D{{Key: "addr.city", Value: "LA"}}) {
		t.Fatal("dot notation mismatch should not match")
	}
}

func TestMatchDoc_And(t *testing.T) {
	doc := bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}
	filterOk := bson.D{{Key: "$and", Value: bson.A{
		bson.D{{Key: "a", Value: int32(1)}},
		bson.D{{Key: "b", Value: int32(2)}},
	}}}
	if !MatchDoc(doc, filterOk) {
		t.Fatal("$and both conditions met should match")
	}
	filterFail := bson.D{{Key: "$and", Value: bson.A{
		bson.D{{Key: "a", Value: int32(1)}},
		bson.D{{Key: "b", Value: int32(99)}},
	}}}
	if MatchDoc(doc, filterFail) {
		t.Fatal("$and with failing condition should not match")
	}
}

func TestMatchDoc_Or(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(5)}}
	filterOk := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "x", Value: int32(3)}},
		bson.D{{Key: "x", Value: int32(5)}},
	}}}
	if !MatchDoc(doc, filterOk) {
		t.Fatal("$or with one matching condition should match")
	}
	filterFail := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "x", Value: int32(3)}},
		bson.D{{Key: "x", Value: int32(7)}},
	}}}
	if MatchDoc(doc, filterFail) {
		t.Fatal("$or with no matching conditions should not match")
	}
}

func TestMatchDoc_Nor(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(5)}}
	filterOk := bson.D{{Key: "$nor", Value: bson.A{
		bson.D{{Key: "x", Value: int32(3)}},
		bson.D{{Key: "x", Value: int32(7)}},
	}}}
	if !MatchDoc(doc, filterOk) {
		t.Fatal("$nor with none matching should match")
	}
	filterFail := bson.D{{Key: "$nor", Value: bson.A{
		bson.D{{Key: "x", Value: int32(5)}},
	}}}
	if MatchDoc(doc, filterFail) {
		t.Fatal("$nor with one matching should not match")
	}
}

func TestMatchDoc_Not_TopLevel(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(5)}}
	filterOk := bson.D{{Key: "$not", Value: bson.D{{Key: "x", Value: int32(3)}}}}
	if !MatchDoc(doc, filterOk) {
		t.Fatal("$not on non-matching sub-filter should match")
	}
	filterFail := bson.D{{Key: "$not", Value: bson.D{{Key: "x", Value: int32(5)}}}}
	if MatchDoc(doc, filterFail) {
		t.Fatal("$not on matching sub-filter should not match")
	}
}

func TestMatchDoc_Eq(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(5)}}
	if !MatchDoc(doc, bson.D{{Key: "x", Value: bson.D{{Key: "$eq", Value: int32(5)}}}}) {
		t.Fatal("$eq matching value should match")
	}
	if MatchDoc(doc, bson.D{{Key: "x", Value: bson.D{{Key: "$eq", Value: int32(9)}}}}) {
		t.Fatal("$eq different value should not match")
	}
}

func TestMatchDoc_Ne(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(5)}}
	if !MatchDoc(doc, bson.D{{Key: "x", Value: bson.D{{Key: "$ne", Value: int32(3)}}}}) {
		t.Fatal("$ne with different value should match")
	}
	if MatchDoc(doc, bson.D{{Key: "x", Value: bson.D{{Key: "$ne", Value: int32(5)}}}}) {
		t.Fatal("$ne with same value should not match")
	}
}

func TestMatchDoc_GtLtGteLte(t *testing.T) {
	doc := bson.D{{Key: "age", Value: int32(25)}}
	cases := []struct {
		op     string
		val    int32
		expect bool
	}{
		{"$gt", 20, true},
		{"$gt", 25, false},
		{"$gt", 30, false},
		{"$gte", 25, true},
		{"$gte", 26, false},
		{"$lt", 30, true},
		{"$lt", 25, false},
		{"$lte", 25, true},
		{"$lte", 24, false},
	}
	for _, c := range cases {
		filter := bson.D{{Key: "age", Value: bson.D{{Key: c.op, Value: c.val}}}}
		if MatchDoc(doc, filter) != c.expect {
			t.Errorf("%s %d: expected %v", c.op, c.val, c.expect)
		}
	}
}

func TestMatchDoc_In(t *testing.T) {
	doc := bson.D{{Key: "status", Value: "active"}}
	if !MatchDoc(doc, bson.D{{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{"active", "pending"}}}}}) {
		t.Fatal("$in should match")
	}
	if MatchDoc(doc, bson.D{{Key: "status", Value: bson.D{{Key: "$in", Value: bson.A{"inactive", "banned"}}}}}) {
		t.Fatal("$in should not match")
	}
}

func TestMatchDoc_Nin(t *testing.T) {
	doc := bson.D{{Key: "status", Value: "active"}}
	if !MatchDoc(doc, bson.D{{Key: "status", Value: bson.D{{Key: "$nin", Value: bson.A{"inactive", "banned"}}}}}) {
		t.Fatal("$nin should match when value not in list")
	}
	if MatchDoc(doc, bson.D{{Key: "status", Value: bson.D{{Key: "$nin", Value: bson.A{"active", "pending"}}}}}) {
		t.Fatal("$nin should not match when value in list")
	}
}

func TestMatchDoc_Exists_True(t *testing.T) {
	doc := bson.D{{Key: "a", Value: 1}}
	if !MatchDoc(doc, bson.D{{Key: "a", Value: bson.D{{Key: "$exists", Value: true}}}}) {
		t.Fatal("$exists:true on existing field should match")
	}
	if MatchDoc(doc, bson.D{{Key: "b", Value: bson.D{{Key: "$exists", Value: true}}}}) {
		t.Fatal("$exists:true on missing field should not match")
	}
}

func TestMatchDoc_Exists_False(t *testing.T) {
	doc := bson.D{{Key: "a", Value: 1}}
	if !MatchDoc(doc, bson.D{{Key: "b", Value: bson.D{{Key: "$exists", Value: false}}}}) {
		t.Fatal("$exists:false on missing field should match")
	}
	if MatchDoc(doc, bson.D{{Key: "a", Value: bson.D{{Key: "$exists", Value: false}}}}) {
		t.Fatal("$exists:false on existing field should not match")
	}
}

func TestMatchDoc_Type(t *testing.T) {
	doc := bson.D{
		{Key: "name", Value: "Alice"},
		{Key: "age", Value: int32(30)},
		{Key: "score", Value: float64(9.5)},
		{Key: "active", Value: true},
		{Key: "tags", Value: bson.A{"a", "b"}},
		{Key: "meta", Value: bson.D{{Key: "x", Value: 1}}},
	}
	cases := []struct {
		field    string
		typeName string
		expect   bool
	}{
		{"name", "string", true},
		{"age", "int", true},
		{"score", "double", true},
		{"active", "bool", true},
		{"tags", "array", true},
		{"meta", "object", true},
		{"age", "string", false},
		{"name", "int", false},
	}
	for _, c := range cases {
		filter := bson.D{{Key: c.field, Value: bson.D{{Key: "$type", Value: c.typeName}}}}
		if MatchDoc(doc, filter) != c.expect {
			t.Errorf("field=%s $type=%s: expected %v", c.field, c.typeName, c.expect)
		}
	}
}

func TestMatchDoc_All(t *testing.T) {
	doc := bson.D{{Key: "tags", Value: bson.A{"go", "python", "rust"}}}
	if !MatchDoc(doc, bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: bson.A{"go", "rust"}}}}}) {
		t.Fatal("$all should match subset")
	}
	if MatchDoc(doc, bson.D{{Key: "tags", Value: bson.D{{Key: "$all", Value: bson.A{"go", "java"}}}}}) {
		t.Fatal("$all should not match when element missing")
	}
}

func TestMatchDoc_Size(t *testing.T) {
	doc := bson.D{{Key: "items", Value: bson.A{1, 2, 3}}}
	if !MatchDoc(doc, bson.D{{Key: "items", Value: bson.D{{Key: "$size", Value: int32(3)}}}}) {
		t.Fatal("$size should match")
	}
	if MatchDoc(doc, bson.D{{Key: "items", Value: bson.D{{Key: "$size", Value: int32(2)}}}}) {
		t.Fatal("$size should not match wrong count")
	}
}

func TestMatchDoc_ElemMatch(t *testing.T) {
	doc := bson.D{{Key: "scores", Value: bson.A{
		bson.D{{Key: "subject", Value: "math"}, {Key: "score", Value: int32(90)}},
		bson.D{{Key: "subject", Value: "english"}, {Key: "score", Value: int32(75)}},
	}}}
	filterOk := bson.D{{Key: "scores", Value: bson.D{{Key: "$elemMatch", Value: bson.D{
		{Key: "subject", Value: "math"},
		{Key: "score", Value: bson.D{{Key: "$gte", Value: int32(80)}}},
	}}}}}
	if !MatchDoc(doc, filterOk) {
		t.Fatal("$elemMatch should match")
	}
	filterFail := bson.D{{Key: "scores", Value: bson.D{{Key: "$elemMatch", Value: bson.D{
		{Key: "subject", Value: "math"},
		{Key: "score", Value: bson.D{{Key: "$gte", Value: int32(95)}}},
	}}}}}
	if MatchDoc(doc, filterFail) {
		t.Fatal("$elemMatch should not match")
	}
}

func TestMatchDoc_FieldNot(t *testing.T) {
	doc := bson.D{{Key: "age", Value: int32(25)}}
	// age is NOT > 30 → should match
	filterOk := bson.D{{Key: "age", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$gt", Value: int32(30)}}}}}}
	if !MatchDoc(doc, filterOk) {
		t.Fatal("field $not should match (25 not > 30)")
	}
	// age is NOT > 20 → false (25 > 20, so $not fails)
	filterFail := bson.D{{Key: "age", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$gt", Value: int32(20)}}}}}}
	if MatchDoc(doc, filterFail) {
		t.Fatal("field $not should not match (25 > 20)")
	}
}

// ---- SetField / UnsetField / GetField ----

func TestSetField_ExistingKey(t *testing.T) {
	doc := bson.D{{Key: "a", Value: 1}}
	doc = SetField(doc, "a", 2)
	v, _ := GetField(doc, "a")
	if v != 2 {
		t.Fatalf("expected 2, got %v", v)
	}
}

func TestSetField_NewKey(t *testing.T) {
	doc := bson.D{{Key: "a", Value: 1}}
	doc = SetField(doc, "b", 99)
	v, ok := GetField(doc, "b")
	if !ok || v != 99 {
		t.Fatalf("expected 99, got %v (ok=%v)", v, ok)
	}
}

func TestSetField_Nested(t *testing.T) {
	doc := bson.D{}
	doc = SetField(doc, "a.b.c", 42)
	v, ok := GetField(doc, "a.b.c")
	if !ok || v != 42 {
		t.Fatalf("expected 42, got %v (ok=%v)", v, ok)
	}
}

func TestSetField_UpdateNested(t *testing.T) {
	doc := bson.D{{Key: "a", Value: bson.D{{Key: "b", Value: 1}}}}
	doc = SetField(doc, "a.b", 2)
	v, _ := GetField(doc, "a.b")
	if v != 2 {
		t.Fatalf("expected 2, got %v", v)
	}
}

func TestUnsetField_Simple(t *testing.T) {
	doc := bson.D{{Key: "x", Value: 1}, {Key: "y", Value: 2}}
	doc = UnsetField(doc, "x")
	_, okX := GetField(doc, "x")
	_, okY := GetField(doc, "y")
	if okX {
		t.Fatal("x should be removed")
	}
	if !okY {
		t.Fatal("y should remain")
	}
}

func TestUnsetField_Nested(t *testing.T) {
	doc := bson.D{{Key: "a", Value: bson.D{{Key: "b", Value: 1}, {Key: "c", Value: 2}}}}
	doc = UnsetField(doc, "a.b")
	_, okAB := GetField(doc, "a.b")
	_, okAC := GetField(doc, "a.c")
	if okAB {
		t.Fatal("a.b should be removed")
	}
	if !okAC {
		t.Fatal("a.c should remain")
	}
}

func TestUnsetField_Missing(t *testing.T) {
	doc := bson.D{{Key: "x", Value: 1}}
	doc = UnsetField(doc, "y") // no-op, should not panic
	_, ok := GetField(doc, "x")
	if !ok {
		t.Fatal("x should still be present")
	}
}

func TestGetField_Exists(t *testing.T) {
	doc := bson.D{{Key: "k", Value: "v"}}
	val, ok := GetField(doc, "k")
	if !ok || val != "v" {
		t.Fatalf("expected v, got %v ok=%v", val, ok)
	}
}

func TestGetField_Missing(t *testing.T) {
	doc := bson.D{{Key: "x", Value: 1}}
	_, ok := GetField(doc, "y")
	if ok {
		t.Fatal("y should not exist")
	}
}

// ---- FilterDocs ----

func TestFilterDocs_EmptyFilter(t *testing.T) {
	docs := []bson.D{
		{{Key: "x", Value: 1}},
		{{Key: "x", Value: 2}},
	}
	result := FilterDocs(docs, bson.D{})
	if len(result) != 2 {
		t.Fatalf("empty filter should return all docs, got %d", len(result))
	}
}

func TestFilterDocs_WithCondition(t *testing.T) {
	docs := []bson.D{
		{{Key: "x", Value: int32(1)}},
		{{Key: "x", Value: int32(5)}},
		{{Key: "x", Value: int32(10)}},
	}
	result := FilterDocs(docs, bson.D{{Key: "x", Value: bson.D{{Key: "$gt", Value: int32(3)}}}})
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

// ---- SortDocs ----

func TestSortDocs_Ascending(t *testing.T) {
	docs := []bson.D{
		{{Key: "n", Value: int32(3)}},
		{{Key: "n", Value: int32(1)}},
		{{Key: "n", Value: int32(2)}},
	}
	SortDocs(docs, bson.D{{Key: "n", Value: int32(1)}})
	n0, _ := GetField(docs[0], "n")
	n2, _ := GetField(docs[2], "n")
	if n0 != int32(1) || n2 != int32(3) {
		t.Fatalf("wrong asc order: first=%v last=%v", n0, n2)
	}
}

func TestSortDocs_Descending(t *testing.T) {
	docs := []bson.D{
		{{Key: "n", Value: int32(1)}},
		{{Key: "n", Value: int32(3)}},
		{{Key: "n", Value: int32(2)}},
	}
	SortDocs(docs, bson.D{{Key: "n", Value: int32(-1)}})
	n0, _ := GetField(docs[0], "n")
	if n0 != int32(3) {
		t.Fatalf("expected desc first=3, got %v", n0)
	}
}

func TestSortDocs_StringField(t *testing.T) {
	docs := []bson.D{
		{{Key: "name", Value: "Charlie"}},
		{{Key: "name", Value: "Alice"}},
		{{Key: "name", Value: "Bob"}},
	}
	SortDocs(docs, bson.D{{Key: "name", Value: int32(1)}})
	n0, _ := GetField(docs[0], "name")
	if n0 != "Alice" {
		t.Fatalf("expected Alice first, got %v", n0)
	}
}

func TestSortDocs_Empty(t *testing.T) {
	var docs []bson.D
	SortDocs(docs, bson.D{{Key: "x", Value: int32(1)}}) // should not panic
}

// ---- CopyDoc ----

func TestCopyDoc(t *testing.T) {
	orig := bson.D{{Key: "x", Value: int32(1)}, {Key: "y", Value: "hello"}}
	copy_, err := CopyDoc(orig)
	if err != nil {
		t.Fatal(err)
	}
	// Modify copy, original should be unchanged
	copy_ = SetField(copy_, "x", int32(99))
	v, _ := GetField(orig, "x")
	if v != int32(1) {
		t.Fatalf("original should be unchanged, got %v", v)
	}
}
