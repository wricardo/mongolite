package engine

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ---- RunPipeline ----

func TestRunPipeline_Empty(t *testing.T) {
	docs := []bson.D{{{Key: "x", Value: 1}}}
	out, err := RunPipeline(docs, nil, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("empty pipeline should return all docs: got %d, err=%v", len(out), err)
	}
}

func TestRunPipeline_InvalidStage(t *testing.T) {
	docs := []bson.D{{{Key: "x", Value: 1}}}
	_, err := RunPipeline(docs, []bson.D{{{Key: "$bogus", Value: nil}}}, nil)
	if err == nil {
		t.Fatal("expected error for unsupported stage")
	}
}

func TestRunPipeline_StageMustHaveOneField(t *testing.T) {
	docs := []bson.D{{{Key: "x", Value: 1}}}
	// stage with 2 fields should fail
	_, err := RunPipeline(docs, []bson.D{
		{{Key: "$match", Value: bson.D{}}, {Key: "$sort", Value: bson.D{}}},
	}, nil)
	if err == nil {
		t.Fatal("expected error for stage with multiple fields")
	}
}

// ---- $group accumulators ----

func TestGroupAccumulator_Avg(t *testing.T) {
	docs := []bson.D{
		{{Key: "cat", Value: "a"}, {Key: "v", Value: int32(10)}},
		{{Key: "cat", Value: "a"}, {Key: "v", Value: int32(20)}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$cat"},
			{Key: "avg", Value: bson.D{{Key: "$avg", Value: "$v"}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1, got %d err=%v", len(out), err)
	}
	avg, _ := GetField(out[0], "avg")
	if avg != float64(15) {
		t.Fatalf("expected avg=15.0, got %v (%T)", avg, avg)
	}
}

func TestGroupAccumulator_Min(t *testing.T) {
	docs := []bson.D{
		{{Key: "g", Value: "x"}, {Key: "v", Value: int32(5)}},
		{{Key: "g", Value: "x"}, {Key: "v", Value: int32(3)}},
		{{Key: "g", Value: "x"}, {Key: "v", Value: int32(8)}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$g"},
			{Key: "minV", Value: bson.D{{Key: "$min", Value: "$v"}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1, got %d err=%v", len(out), err)
	}
	v, _ := GetField(out[0], "minV")
	if v != int32(3) {
		t.Fatalf("expected min=3, got %v", v)
	}
}

func TestGroupAccumulator_Max(t *testing.T) {
	docs := []bson.D{
		{{Key: "g", Value: "x"}, {Key: "v", Value: int32(5)}},
		{{Key: "g", Value: "x"}, {Key: "v", Value: int32(3)}},
		{{Key: "g", Value: "x"}, {Key: "v", Value: int32(8)}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$g"},
			{Key: "maxV", Value: bson.D{{Key: "$max", Value: "$v"}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1, got %d err=%v", len(out), err)
	}
	v, _ := GetField(out[0], "maxV")
	if v != int32(8) {
		t.Fatalf("expected max=8, got %v", v)
	}
}

func TestGroupAccumulator_First_Last(t *testing.T) {
	docs := []bson.D{
		{{Key: "g", Value: "x"}, {Key: "v", Value: "alpha"}},
		{{Key: "g", Value: "x"}, {Key: "v", Value: "beta"}},
		{{Key: "g", Value: "x"}, {Key: "v", Value: "gamma"}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$g"},
			{Key: "first", Value: bson.D{{Key: "$first", Value: "$v"}}},
			{Key: "last", Value: bson.D{{Key: "$last", Value: "$v"}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1, got %d err=%v", len(out), err)
	}
	first, _ := GetField(out[0], "first")
	last, _ := GetField(out[0], "last")
	if first != "alpha" {
		t.Fatalf("expected first=alpha, got %v", first)
	}
	if last != "gamma" {
		t.Fatalf("expected last=gamma, got %v", last)
	}
}

func TestGroupAccumulator_Push(t *testing.T) {
	docs := []bson.D{
		{{Key: "g", Value: "x"}, {Key: "v", Value: int32(1)}},
		{{Key: "g", Value: "x"}, {Key: "v", Value: int32(2)}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$g"},
			{Key: "all", Value: bson.D{{Key: "$push", Value: "$v"}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1, got %d err=%v", len(out), err)
	}
	all, _ := GetField(out[0], "all")
	arr := all.(bson.A)
	if len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(arr))
	}
}

func TestGroupAccumulator_AddToSet(t *testing.T) {
	docs := []bson.D{
		{{Key: "g", Value: "x"}, {Key: "tag", Value: "go"}},
		{{Key: "g", Value: "x"}, {Key: "tag", Value: "python"}},
		{{Key: "g", Value: "x"}, {Key: "tag", Value: "go"}}, // duplicate
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$g"},
			{Key: "tags", Value: bson.D{{Key: "$addToSet", Value: "$tag"}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1, got %d err=%v", len(out), err)
	}
	tags, _ := GetField(out[0], "tags")
	arr := tags.(bson.A)
	if len(arr) != 2 {
		t.Fatalf("expected 2 unique tags, got %d: %v", len(arr), arr)
	}
}

func TestGroupAccumulator_SumConstant(t *testing.T) {
	// $sum: 1 (count documents per group)
	docs := []bson.D{
		{{Key: "cat", Value: "a"}},
		{{Key: "cat", Value: "a"}},
		{{Key: "cat", Value: "b"}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$cat"},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 2 {
		t.Fatalf("expected 2 groups, got %d err=%v", len(out), err)
	}
	byID := make(map[interface{}]interface{})
	for _, d := range out {
		id, _ := GetField(d, "_id")
		cnt, _ := GetField(d, "count")
		byID[id] = cnt
	}
	if byID["a"] != int64(2) {
		t.Fatalf("expected a count=2, got %v", byID["a"])
	}
	if byID["b"] != int64(1) {
		t.Fatalf("expected b count=1, got %v", byID["b"])
	}
}

// ---- $group compound _id ----

func TestGroupCompoundID(t *testing.T) {
	docs := []bson.D{
		{{Key: "dept", Value: "eng"}, {Key: "level", Value: "senior"}, {Key: "salary", Value: int32(100)}},
		{{Key: "dept", Value: "eng"}, {Key: "level", Value: "junior"}, {Key: "salary", Value: int32(70)}},
		{{Key: "dept", Value: "eng"}, {Key: "level", Value: "senior"}, {Key: "salary", Value: int32(120)}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{{Key: "dept", Value: "$dept"}, {Key: "level", Value: "$level"}}},
			{Key: "total", Value: bson.D{{Key: "$sum", Value: "$salary"}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil {
		t.Fatal(err)
	}
	// eng/senior and eng/junior = 2 groups
	if len(out) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(out))
	}
}

// ---- $lookup ----

func TestLookup(t *testing.T) {
	orders := []bson.D{
		{{Key: "orderId", Value: int32(1)}, {Key: "userId", Value: int32(10)}},
		{{Key: "orderId", Value: int32(2)}, {Key: "userId", Value: int32(20)}},
	}
	users := []bson.D{
		{{Key: "_id", Value: int32(10)}, {Key: "name", Value: "Alice"}},
		{{Key: "_id", Value: int32(20)}, {Key: "name", Value: "Bob"}},
	}

	lookupFn := func(_, coll string, filter bson.D) ([]bson.D, error) {
		if coll == "users" {
			return FilterDocs(users, filter), nil
		}
		return nil, nil
	}

	pipeline := []bson.D{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "users"},
			{Key: "localField", Value: "userId"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "user"},
		}}},
	}
	out, err := RunPipeline(orders, pipeline, lookupFn)
	if err != nil || len(out) != 2 {
		t.Fatalf("expected 2, got %d err=%v", len(out), err)
	}
	userArr, _ := GetField(out[0], "user")
	arr := userArr.(bson.A)
	if len(arr) != 1 {
		t.Fatalf("expected 1 matched user, got %d", len(arr))
	}
}

func TestLookup_NilFn(t *testing.T) {
	docs := []bson.D{{{Key: "x", Value: 1}}}
	pipeline := []bson.D{
		{{Key: "$lookup", Value: bson.D{
			{Key: "from", Value: "users"},
			{Key: "localField", Value: "id"},
			{Key: "foreignField", Value: "_id"},
			{Key: "as", Value: "user"},
		}}},
	}
	_, err := RunPipeline(docs, pipeline, nil)
	if err == nil {
		t.Fatal("expected error when lookupFn is nil")
	}
}

// ---- compareValues edge cases ----

func TestCompareValues_Strings(t *testing.T) {
	if compareValues("apple", "banana") >= 0 {
		t.Fatal("apple should be < banana")
	}
	if compareValues("z", "a") <= 0 {
		t.Fatal("z should be > a")
	}
	if compareValues("same", "same") != 0 {
		t.Fatal("same should be == same")
	}
}

func TestCompareValues_Bools(t *testing.T) {
	if compareValues(false, true) >= 0 {
		t.Fatal("false should be < true")
	}
	if compareValues(true, false) <= 0 {
		t.Fatal("true should be > false")
	}
	if compareValues(true, true) != 0 {
		t.Fatal("true should == true")
	}
}

func TestCompareValues_ObjectIDs(t *testing.T) {
	id1 := bson.ObjectID{0x00}
	id2 := bson.ObjectID{0xFF}
	if compareValues(id1, id2) >= 0 {
		t.Fatal("id1 should be < id2")
	}
	if compareValues(id2, id1) <= 0 {
		t.Fatal("id2 should be > id1")
	}
	if compareValues(id1, id1) != 0 {
		t.Fatal("same ObjectID should be equal")
	}
}

func TestCompareValues_MixedTypes(t *testing.T) {
	// incomparable types return 0
	if compareValues("str", int32(1)) != 0 {
		t.Fatal("mixed types should return 0")
	}
}

// ---- valuesEqual edge cases ----

func TestValuesEqual_BothNil(t *testing.T) {
	if !valuesEqual(nil, nil) {
		t.Fatal("nil == nil")
	}
}

func TestValuesEqual_OneNil(t *testing.T) {
	if valuesEqual(nil, "x") {
		t.Fatal("nil != string")
	}
	if valuesEqual("x", nil) {
		t.Fatal("string != nil")
	}
}

// ---- matchType null ----

func TestMatchType_Null(t *testing.T) {
	doc := bson.D{{Key: "x", Value: nil}}
	if !MatchDoc(doc, bson.D{{Key: "x", Value: bson.D{{Key: "$type", Value: "null"}}}}) {
		t.Fatal("$type null should match nil")
	}
}
