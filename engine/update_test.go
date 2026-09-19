package engine

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestApplyUpdate_Set(t *testing.T) {
	doc := bson.D{{Key: "a", Value: 1}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$set", Value: bson.D{
		{Key: "a", Value: 2},
		{Key: "b", Value: 3},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := GetField(out, "a")
	b, _ := GetField(out, "b")
	if a != 2 || b != 3 {
		t.Fatalf("expected a=2 b=3, got a=%v b=%v", a, b)
	}
}

func TestApplyUpdate_Set_Nested(t *testing.T) {
	doc := bson.D{}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$set", Value: bson.D{
		{Key: "user.name", Value: "Alice"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	v, ok := GetField(out, "user.name")
	if !ok || v != "Alice" {
		t.Fatalf("expected user.name=Alice, got %v ok=%v", v, ok)
	}
}

func TestApplyUpdate_Unset(t *testing.T) {
	doc := bson.D{{Key: "a", Value: 1}, {Key: "b", Value: 2}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$unset", Value: bson.D{{Key: "a", Value: ""}}}})
	if err != nil {
		t.Fatal(err)
	}
	_, okA := GetField(out, "a")
	_, okB := GetField(out, "b")
	if okA {
		t.Fatal("a should be unset")
	}
	if !okB {
		t.Fatal("b should remain")
	}
}

func TestApplyUpdate_Inc_Existing(t *testing.T) {
	doc := bson.D{{Key: "count", Value: int32(5)}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$inc", Value: bson.D{{Key: "count", Value: int32(3)}}}})
	if err != nil {
		t.Fatal(err)
	}
	v, _ := GetField(out, "count")
	if v != int64(8) {
		t.Fatalf("expected int64(8), got %v (%T)", v, v)
	}
}

func TestApplyUpdate_Inc_New(t *testing.T) {
	doc := bson.D{}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$inc", Value: bson.D{{Key: "views", Value: int32(1)}}}})
	if err != nil {
		t.Fatal(err)
	}
	v, ok := GetField(out, "views")
	if !ok || v == nil {
		t.Fatal("views should be set")
	}
}

func TestApplyUpdate_Inc_NonNumeric(t *testing.T) {
	doc := bson.D{{Key: "x", Value: "hello"}}
	_, err := ApplyUpdate(doc, bson.D{{Key: "$inc", Value: bson.D{{Key: "x", Value: int32(1)}}}})
	if err == nil {
		t.Fatal("expected error for $inc on non-numeric field")
	}
}

func TestApplyUpdate_Mul_Existing(t *testing.T) {
	doc := bson.D{{Key: "price", Value: int32(10)}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$mul", Value: bson.D{{Key: "price", Value: int32(3)}}}})
	if err != nil {
		t.Fatal(err)
	}
	v, _ := GetField(out, "price")
	if v != int64(30) {
		t.Fatalf("expected int64(30), got %v (%T)", v, v)
	}
}

func TestApplyUpdate_Mul_New(t *testing.T) {
	doc := bson.D{}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$mul", Value: bson.D{{Key: "x", Value: int32(5)}}}})
	if err != nil {
		t.Fatal(err)
	}
	v, _ := GetField(out, "x")
	if v != int64(0) {
		t.Fatalf("$mul on missing field should set 0, got %v", v)
	}
}

func TestApplyUpdate_Min(t *testing.T) {
	doc := bson.D{{Key: "score", Value: int32(50)}}
	// 30 < 50 → set to 30
	out, err := ApplyUpdate(doc, bson.D{{Key: "$min", Value: bson.D{{Key: "score", Value: int32(30)}}}})
	if err != nil {
		t.Fatal(err)
	}
	v, _ := GetField(out, "score")
	if v != int32(30) {
		t.Fatalf("expected 30, got %v", v)
	}
	// 60 > 30 → no change
	out2, _ := ApplyUpdate(out, bson.D{{Key: "$min", Value: bson.D{{Key: "score", Value: int32(60)}}}})
	v2, _ := GetField(out2, "score")
	if v2 != int32(30) {
		t.Fatalf("$min should not increase value, got %v", v2)
	}
}

func TestApplyUpdate_Max(t *testing.T) {
	doc := bson.D{{Key: "score", Value: int32(30)}}
	// 60 > 30 → set to 60
	out, err := ApplyUpdate(doc, bson.D{{Key: "$max", Value: bson.D{{Key: "score", Value: int32(60)}}}})
	if err != nil {
		t.Fatal(err)
	}
	v, _ := GetField(out, "score")
	if v != int32(60) {
		t.Fatalf("expected 60, got %v", v)
	}
	// 10 < 60 → no change
	out2, _ := ApplyUpdate(out, bson.D{{Key: "$max", Value: bson.D{{Key: "score", Value: int32(10)}}}})
	v2, _ := GetField(out2, "score")
	if v2 != int32(60) {
		t.Fatalf("$max should not decrease value, got %v", v2)
	}
}

func TestApplyUpdate_Rename(t *testing.T) {
	doc := bson.D{{Key: "oldName", Value: "val"}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$rename", Value: bson.D{{Key: "oldName", Value: "newName"}}}})
	if err != nil {
		t.Fatal(err)
	}
	_, oldOk := GetField(out, "oldName")
	newVal, newOk := GetField(out, "newName")
	if oldOk {
		t.Fatal("oldName should be removed")
	}
	if !newOk || newVal != "val" {
		t.Fatalf("newName should be val, got %v ok=%v", newVal, newOk)
	}
}

func TestApplyUpdate_Rename_Missing(t *testing.T) {
	doc := bson.D{{Key: "x", Value: 1}}
	// Renaming a missing field is a no-op
	out, err := ApplyUpdate(doc, bson.D{{Key: "$rename", Value: bson.D{{Key: "missing", Value: "newField"}}}})
	if err != nil {
		t.Fatal(err)
	}
	_, ok := GetField(out, "newField")
	if ok {
		t.Fatal("newField should not exist when source was missing")
	}
}

func TestApplyUpdate_Push_Existing(t *testing.T) {
	doc := bson.D{{Key: "tags", Value: bson.A{"a", "b"}}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$push", Value: bson.D{{Key: "tags", Value: "c"}}}})
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := GetField(out, "tags")
	a := arr.(bson.A)
	if len(a) != 3 || a[2] != "c" {
		t.Fatalf("expected [a,b,c], got %v", a)
	}
}

func TestApplyUpdate_Push_New(t *testing.T) {
	doc := bson.D{}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$push", Value: bson.D{{Key: "items", Value: "x"}}}})
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := GetField(out, "items")
	a := arr.(bson.A)
	if len(a) != 1 || a[0] != "x" {
		t.Fatalf("expected [x], got %v", a)
	}
}

func TestApplyUpdate_Push_NonArray(t *testing.T) {
	doc := bson.D{{Key: "x", Value: "not_array"}}
	_, err := ApplyUpdate(doc, bson.D{{Key: "$push", Value: bson.D{{Key: "x", Value: "v"}}}})
	if err == nil {
		t.Fatal("expected error for $push on non-array")
	}
}

func TestApplyUpdate_Pull_ByValue(t *testing.T) {
	doc := bson.D{{Key: "nums", Value: bson.A{int32(1), int32(2), int32(3), int32(2)}}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$pull", Value: bson.D{{Key: "nums", Value: int32(2)}}}})
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := GetField(out, "nums")
	a := arr.(bson.A)
	if len(a) != 2 {
		t.Fatalf("expected 2 elements remaining, got %d: %v", len(a), a)
	}
}

func TestApplyUpdate_Pull_ByCondition(t *testing.T) {
	doc := bson.D{{Key: "scores", Value: bson.A{
		bson.D{{Key: "v", Value: int32(5)}},
		bson.D{{Key: "v", Value: int32(15)}},
		bson.D{{Key: "v", Value: int32(3)}},
	}}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$pull", Value: bson.D{
		{Key: "scores", Value: bson.D{{Key: "v", Value: bson.D{{Key: "$lt", Value: int32(10)}}}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := GetField(out, "scores")
	a := arr.(bson.A)
	if len(a) != 1 {
		t.Fatalf("expected 1 remaining (score>=10), got %d: %v", len(a), a)
	}
}

func TestApplyUpdate_Pull_Missing(t *testing.T) {
	doc := bson.D{{Key: "x", Value: 1}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$pull", Value: bson.D{{Key: "missing", Value: "v"}}}})
	if err != nil {
		t.Fatal(err)
	}
	// doc should be unchanged
	v, _ := GetField(out, "x")
	if v != 1 {
		t.Fatalf("x should remain unchanged, got %v", v)
	}
}

func TestApplyUpdate_AddToSet_NoDuplicate(t *testing.T) {
	doc := bson.D{{Key: "tags", Value: bson.A{"a", "b"}}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: "a"}}}})
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := GetField(out, "tags")
	a := arr.(bson.A)
	if len(a) != 2 {
		t.Fatalf("expected 2 (no dup), got %d", len(a))
	}
}

func TestApplyUpdate_AddToSet_NewElement(t *testing.T) {
	doc := bson.D{{Key: "tags", Value: bson.A{"a", "b"}}}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: "c"}}}})
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := GetField(out, "tags")
	a := arr.(bson.A)
	if len(a) != 3 {
		t.Fatalf("expected 3, got %d", len(a))
	}
}

func TestApplyUpdate_AddToSet_New(t *testing.T) {
	doc := bson.D{}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$addToSet", Value: bson.D{{Key: "tags", Value: "x"}}}})
	if err != nil {
		t.Fatal(err)
	}
	arr, _ := GetField(out, "tags")
	a := arr.(bson.A)
	if len(a) != 1 || a[0] != "x" {
		t.Fatalf("expected [x], got %v", a)
	}
}

func TestApplyUpdate_CurrentDate(t *testing.T) {
	doc := bson.D{}
	out, err := ApplyUpdate(doc, bson.D{{Key: "$currentDate", Value: bson.D{{Key: "updatedAt", Value: true}}}})
	if err != nil {
		t.Fatal(err)
	}
	v, ok := GetField(out, "updatedAt")
	if !ok || v == nil {
		t.Fatal("updatedAt should be set")
	}
	_, isDateTime := v.(bson.DateTime)
	if !isDateTime {
		t.Fatalf("expected bson.DateTime, got %T", v)
	}
}

func TestApplyUpdate_Replacement(t *testing.T) {
	doc := bson.D{{Key: "_id", Value: "abc"}, {Key: "x", Value: 1}, {Key: "y", Value: 2}}
	update := bson.D{{Key: "z", Value: 3}} // no $ prefix → replacement
	out, err := ApplyUpdate(doc, update)
	if err != nil {
		t.Fatal(err)
	}
	// _id must be preserved
	id, ok := GetField(out, "_id")
	if !ok || id != "abc" {
		t.Fatalf("_id should be preserved, got %v ok=%v", id, ok)
	}
	// z should be present
	z, ok := GetField(out, "z")
	if !ok || z != 3 {
		t.Fatalf("expected z=3, got %v ok=%v", z, ok)
	}
	// x and y should be gone
	_, okX := GetField(out, "x")
	_, okY := GetField(out, "y")
	if okX || okY {
		t.Fatal("x and y should not exist after replacement")
	}
}

func TestApplyUpdate_UnsupportedOperator(t *testing.T) {
	doc := bson.D{{Key: "x", Value: 1}}
	_, err := ApplyUpdate(doc, bson.D{{Key: "$bogus", Value: bson.D{{Key: "x", Value: 1}}}})
	if err == nil {
		t.Fatal("expected error for unsupported operator")
	}
}

func TestApplyUpdate_OperatorValueNotDoc(t *testing.T) {
	doc := bson.D{{Key: "x", Value: 1}}
	_, err := ApplyUpdate(doc, bson.D{{Key: "$set", Value: "not_a_doc"}})
	if err == nil {
		t.Fatal("expected error when operator value is not a document")
	}
}
