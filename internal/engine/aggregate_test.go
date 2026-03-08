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

// ---- Phase 1: Quick wins ----

func TestProjectDocs_Computed(t *testing.T) {
	docs := []bson.D{
		{{Key: "a", Value: int32(3)}, {Key: "b", Value: int32(4)}},
	}
	spec := bson.D{
		{Key: "_id", Value: int32(0)},
		{Key: "sum", Value: bson.D{{Key: "$add", Value: bson.A{"$a", "$b"}}}},
	}
	out, err := ProjectDocs(docs, spec)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1 doc, err=%v", err)
	}
	sum, _ := GetField(out[0], "sum")
	if toFloat64(sum) != 7 {
		t.Fatalf("expected sum=7, got %v", sum)
	}
}

func TestAddFields(t *testing.T) {
	docs := []bson.D{
		{{Key: "price", Value: float64(10)}, {Key: "qty", Value: int32(3)}},
	}
	pipeline := []bson.D{
		{{Key: "$addFields", Value: bson.D{
			{Key: "total", Value: bson.D{{Key: "$multiply", Value: bson.A{"$price", "$qty"}}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1 doc, err=%v", err)
	}
	total, _ := GetField(out[0], "total")
	if toFloat64(total) != 30 {
		t.Fatalf("expected total=30, got %v", total)
	}
	// Original fields preserved
	price, _ := GetField(out[0], "price")
	if toFloat64(price) != 10 {
		t.Fatalf("original field price should be preserved")
	}
}

func TestSetStage(t *testing.T) {
	// $set is alias for $addFields
	docs := []bson.D{{{Key: "x", Value: int32(5)}}}
	pipeline := []bson.D{
		{{Key: "$set", Value: bson.D{{Key: "doubled", Value: bson.D{{Key: "$multiply", Value: bson.A{"$x", int32(2)}}}}}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1 doc, err=%v", err)
	}
	v, _ := GetField(out[0], "doubled")
	if toFloat64(v) != 10 {
		t.Fatalf("expected doubled=10, got %v", v)
	}
}

func TestUnsetStage(t *testing.T) {
	docs := []bson.D{{{Key: "a", Value: 1}, {Key: "b", Value: 2}, {Key: "c", Value: 3}}}
	pipeline := []bson.D{
		{{Key: "$unset", Value: bson.A{"b", "c"}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1 doc, err=%v", err)
	}
	if _, ok := GetField(out[0], "b"); ok {
		t.Fatal("field b should be removed")
	}
	if _, ok := GetField(out[0], "c"); ok {
		t.Fatal("field c should be removed")
	}
	if _, ok := GetField(out[0], "a"); !ok {
		t.Fatal("field a should remain")
	}
}

func TestReplaceRoot(t *testing.T) {
	docs := []bson.D{
		{{Key: "nested", Value: bson.D{{Key: "x", Value: int32(1)}, {Key: "y", Value: int32(2)}}}},
	}
	pipeline := []bson.D{
		{{Key: "$replaceRoot", Value: bson.D{{Key: "newRoot", Value: "$nested"}}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1 doc, err=%v", err)
	}
	x, _ := GetField(out[0], "x")
	if toFloat64(x) != 1 {
		t.Fatalf("expected x=1, got %v", x)
	}
}

func TestReplaceWith(t *testing.T) {
	docs := []bson.D{
		{{Key: "sub", Value: bson.D{{Key: "val", Value: "hello"}}}},
	}
	pipeline := []bson.D{
		{{Key: "$replaceWith", Value: "$sub"}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1 doc, err=%v", err)
	}
	v, _ := GetField(out[0], "val")
	if v != "hello" {
		t.Fatalf("expected val=hello, got %v", v)
	}
}

func TestSortByCount(t *testing.T) {
	docs := []bson.D{
		{{Key: "tag", Value: "go"}},
		{{Key: "tag", Value: "go"}},
		{{Key: "tag", Value: "python"}},
	}
	pipeline := []bson.D{
		{{Key: "$sortByCount", Value: "$tag"}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 2 {
		t.Fatalf("expected 2, got %d err=%v", len(out), err)
	}
	cnt, _ := GetField(out[0], "count")
	if toInt64(cnt) != 2 {
		t.Fatalf("first entry should have count=2 (go), got %v", cnt)
	}
}

// ---- Phase 2: Expression Evaluator ----

func TestEvalExpr_FieldPath(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(42)}}
	v := evalExpr(doc, "$x")
	if toFloat64(v) != 42 {
		t.Fatalf("expected 42, got %v", v)
	}
}

func TestEvalExpr_Constant(t *testing.T) {
	doc := bson.D{}
	v := evalExpr(doc, "hello")
	if v != "hello" {
		t.Fatalf("expected 'hello', got %v", v)
	}
}

func TestEvalExpr_Add(t *testing.T) {
	doc := bson.D{{Key: "a", Value: int32(3)}, {Key: "b", Value: int32(4)}}
	result := evalExpr(doc, bson.D{{Key: "$add", Value: bson.A{"$a", "$b"}}})
	if toFloat64(result) != 7 {
		t.Fatalf("expected 7, got %v", result)
	}
}

func TestEvalExpr_Subtract(t *testing.T) {
	doc := bson.D{{Key: "a", Value: int32(10)}, {Key: "b", Value: int32(3)}}
	result := evalExpr(doc, bson.D{{Key: "$subtract", Value: bson.A{"$a", "$b"}}})
	if toFloat64(result) != 7 {
		t.Fatalf("expected 7, got %v", result)
	}
}

func TestEvalExpr_Multiply(t *testing.T) {
	doc := bson.D{{Key: "qty", Value: int32(5)}, {Key: "price", Value: float64(2.5)}}
	result := evalExpr(doc, bson.D{{Key: "$multiply", Value: bson.A{"$qty", "$price"}}})
	if toFloat64(result) != 12.5 {
		t.Fatalf("expected 12.5, got %v", result)
	}
}

func TestEvalExpr_Divide(t *testing.T) {
	doc := bson.D{{Key: "a", Value: float64(10)}, {Key: "b", Value: float64(4)}}
	result := evalExpr(doc, bson.D{{Key: "$divide", Value: bson.A{"$a", "$b"}}})
	if toFloat64(result) != 2.5 {
		t.Fatalf("expected 2.5, got %v", result)
	}
}

func TestEvalExpr_Mod(t *testing.T) {
	doc := bson.D{{Key: "a", Value: int32(10)}, {Key: "b", Value: int32(3)}}
	result := evalExpr(doc, bson.D{{Key: "$mod", Value: bson.A{"$a", "$b"}}})
	if toFloat64(result) != 1 {
		t.Fatalf("expected 1, got %v", result)
	}
}

func TestEvalExpr_Abs(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(-5)}}
	result := evalExpr(doc, bson.D{{Key: "$abs", Value: "$x"}})
	if toFloat64(result) != 5 {
		t.Fatalf("expected 5, got %v", result)
	}
}

func TestEvalExpr_Ceil(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$ceil", Value: float64(4.2)}})
	if toFloat64(result) != 5 {
		t.Fatalf("expected 5, got %v", result)
	}
}

func TestEvalExpr_Floor(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$floor", Value: float64(4.9)}})
	if toFloat64(result) != 4 {
		t.Fatalf("expected 4, got %v", result)
	}
}

func TestEvalExpr_Sqrt(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$sqrt", Value: float64(9)}})
	if toFloat64(result) != 3 {
		t.Fatalf("expected 3, got %v", result)
	}
}

func TestEvalExpr_Pow(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$pow", Value: bson.A{float64(2), float64(10)}}})
	if toFloat64(result) != 1024 {
		t.Fatalf("expected 1024, got %v", result)
	}
}

func TestEvalExpr_Comparison(t *testing.T) {
	doc := bson.D{{Key: "a", Value: int32(5)}, {Key: "b", Value: int32(3)}}
	tests := []struct {
		op   string
		want bool
	}{
		{"$gt", true}, {"$gte", true}, {"$lt", false}, {"$lte", false}, {"$eq", false}, {"$ne", true},
	}
	for _, tc := range tests {
		result := evalExpr(doc, bson.D{{Key: tc.op, Value: bson.A{"$a", "$b"}}})
		if result != tc.want {
			t.Fatalf("%s: expected %v, got %v", tc.op, tc.want, result)
		}
	}
}

func TestEvalExpr_Cmp(t *testing.T) {
	doc := bson.D{}
	r1 := evalExpr(doc, bson.D{{Key: "$cmp", Value: bson.A{int32(5), int32(3)}}})
	if toInt64(r1) != 1 {
		t.Fatalf("5 cmp 3 expected 1, got %v", r1)
	}
	r2 := evalExpr(doc, bson.D{{Key: "$cmp", Value: bson.A{int32(3), int32(5)}}})
	if toInt64(r2) != -1 {
		t.Fatalf("3 cmp 5 expected -1, got %v", r2)
	}
	r3 := evalExpr(doc, bson.D{{Key: "$cmp", Value: bson.A{int32(5), int32(5)}}})
	if toInt64(r3) != 0 {
		t.Fatalf("5 cmp 5 expected 0, got %v", r3)
	}
}

func TestEvalExpr_And(t *testing.T) {
	doc := bson.D{}
	r1 := evalExpr(doc, bson.D{{Key: "$and", Value: bson.A{true, true}}})
	if r1 != true {
		t.Fatal("true && true should be true")
	}
	r2 := evalExpr(doc, bson.D{{Key: "$and", Value: bson.A{true, false}}})
	if r2 != false {
		t.Fatal("true && false should be false")
	}
}

func TestEvalExpr_Or(t *testing.T) {
	doc := bson.D{}
	r1 := evalExpr(doc, bson.D{{Key: "$or", Value: bson.A{false, true}}})
	if r1 != true {
		t.Fatal("false || true should be true")
	}
	r2 := evalExpr(doc, bson.D{{Key: "$or", Value: bson.A{false, false}}})
	if r2 != false {
		t.Fatal("false || false should be false")
	}
}

func TestEvalExpr_Not(t *testing.T) {
	doc := bson.D{}
	r := evalExpr(doc, bson.D{{Key: "$not", Value: bson.A{true}}})
	if r != false {
		t.Fatal("!true should be false")
	}
}

func TestEvalExpr_Cond_Array(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(5)}}
	result := evalExpr(doc, bson.D{{Key: "$cond", Value: bson.A{
		bson.D{{Key: "$gt", Value: bson.A{"$x", int32(3)}}},
		"big",
		"small",
	}}})
	if result != "big" {
		t.Fatalf("expected 'big', got %v", result)
	}
}

func TestEvalExpr_Cond_Doc(t *testing.T) {
	doc := bson.D{{Key: "x", Value: int32(1)}}
	result := evalExpr(doc, bson.D{{Key: "$cond", Value: bson.D{
		{Key: "if", Value: bson.D{{Key: "$eq", Value: bson.A{"$x", int32(0)}}}},
		{Key: "then", Value: "zero"},
		{Key: "else", Value: "nonzero"},
	}}})
	if result != "nonzero" {
		t.Fatalf("expected 'nonzero', got %v", result)
	}
}

func TestEvalExpr_IfNull(t *testing.T) {
	doc := bson.D{{Key: "x", Value: nil}}
	result := evalExpr(doc, bson.D{{Key: "$ifNull", Value: bson.A{"$x", "default"}}})
	if result != "default" {
		t.Fatalf("expected 'default', got %v", result)
	}
}

func TestEvalExpr_Switch(t *testing.T) {
	doc := bson.D{{Key: "score", Value: int32(85)}}
	result := evalExpr(doc, bson.D{{Key: "$switch", Value: bson.D{
		{Key: "branches", Value: bson.A{
			bson.D{{Key: "case", Value: bson.D{{Key: "$gte", Value: bson.A{"$score", int32(90)}}}}, {Key: "then", Value: "A"}},
			bson.D{{Key: "case", Value: bson.D{{Key: "$gte", Value: bson.A{"$score", int32(80)}}}}, {Key: "then", Value: "B"}},
		}},
		{Key: "default", Value: "C"},
	}}})
	if result != "B" {
		t.Fatalf("expected 'B', got %v", result)
	}
}

func TestEvalExpr_Concat(t *testing.T) {
	doc := bson.D{{Key: "first", Value: "Hello"}, {Key: "last", Value: "World"}}
	result := evalExpr(doc, bson.D{{Key: "$concat", Value: bson.A{"$first", " ", "$last"}}})
	if result != "Hello World" {
		t.Fatalf("expected 'Hello World', got %v", result)
	}
}

func TestEvalExpr_ToLower(t *testing.T) {
	doc := bson.D{{Key: "s", Value: "HELLO"}}
	result := evalExpr(doc, bson.D{{Key: "$toLower", Value: "$s"}})
	if result != "hello" {
		t.Fatalf("expected 'hello', got %v", result)
	}
}

func TestEvalExpr_ToUpper(t *testing.T) {
	doc := bson.D{{Key: "s", Value: "hello"}}
	result := evalExpr(doc, bson.D{{Key: "$toUpper", Value: "$s"}})
	if result != "HELLO" {
		t.Fatalf("expected 'HELLO', got %v", result)
	}
}

func TestEvalExpr_Trim(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$trim", Value: bson.D{{Key: "input", Value: "  hello  "}}}})
	if result != "hello" {
		t.Fatalf("expected 'hello', got %v", result)
	}
}

func TestEvalExpr_Split(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$split", Value: bson.A{"a,b,c", ","}}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 3 {
		t.Fatalf("expected 3 parts, got %v", result)
	}
}

func TestEvalExpr_StrLenBytes(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$strLenBytes", Value: "hello"}})
	if toInt64(result) != 5 {
		t.Fatalf("expected 5, got %v", result)
	}
}

func TestEvalExpr_Substr(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$substr", Value: bson.A{"hello", int32(1), int32(3)}}})
	if result != "ell" {
		t.Fatalf("expected 'ell', got %v", result)
	}
}

func TestEvalExpr_ReplaceOne(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$replaceOne", Value: bson.D{
		{Key: "input", Value: "aababc"},
		{Key: "find", Value: "ab"},
		{Key: "replacement", Value: "X"},
	}}})
	if result != "aXabc" {
		t.Fatalf("expected 'aXabc', got %v", result)
	}
}

func TestEvalExpr_ReplaceAll(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$replaceAll", Value: bson.D{
		{Key: "input", Value: "ababab"},
		{Key: "find", Value: "ab"},
		{Key: "replacement", Value: "X"},
	}}})
	if result != "XXX" {
		t.Fatalf("expected 'XXX', got %v", result)
	}
}

func TestEvalExpr_Strcasecmp(t *testing.T) {
	doc := bson.D{}
	r1 := evalExpr(doc, bson.D{{Key: "$strcasecmp", Value: bson.A{"ABC", "abc"}}})
	if toInt64(r1) != 0 {
		t.Fatalf("case-insensitive equal should be 0, got %v", r1)
	}
}

func TestEvalExpr_Literal(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$literal", Value: "$notAField"}})
	if result != "$notAField" {
		t.Fatalf("$literal should return raw string, got %v", result)
	}
}

// ---- Phase 3: Array Expression Operators ----

func TestEvalExpr_Size(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{1, 2, 3}}}
	result := evalExpr(doc, bson.D{{Key: "$size", Value: "$arr"}})
	if toInt64(result) != 3 {
		t.Fatalf("expected 3, got %v", result)
	}
}

func TestEvalExpr_ArrayElemAt(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{"a", "b", "c"}}}
	result := evalExpr(doc, bson.D{{Key: "$arrayElemAt", Value: bson.A{"$arr", int32(1)}}})
	if result != "b" {
		t.Fatalf("expected 'b', got %v", result)
	}
}

func TestEvalExpr_ArrayElemAt_Negative(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{"a", "b", "c"}}}
	result := evalExpr(doc, bson.D{{Key: "$arrayElemAt", Value: bson.A{"$arr", int32(-1)}}})
	if result != "c" {
		t.Fatalf("expected 'c', got %v", result)
	}
}

func TestEvalExpr_IsArray(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{1, 2}}, {Key: "x", Value: 1}}
	r1 := evalExpr(doc, bson.D{{Key: "$isArray", Value: "$arr"}})
	r2 := evalExpr(doc, bson.D{{Key: "$isArray", Value: "$x"}})
	if r1 != true || r2 != false {
		t.Fatalf("$isArray: got %v, %v", r1, r2)
	}
}

func TestEvalExpr_ConcatArrays(t *testing.T) {
	doc := bson.D{{Key: "a", Value: bson.A{1, 2}}, {Key: "b", Value: bson.A{3, 4}}}
	result := evalExpr(doc, bson.D{{Key: "$concatArrays", Value: bson.A{"$a", "$b"}}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 4 {
		t.Fatalf("expected 4 elements, got %v", result)
	}
}

func TestEvalExpr_Slice(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{1, 2, 3, 4, 5}}}
	result := evalExpr(doc, bson.D{{Key: "$slice", Value: bson.A{"$arr", int32(2)}}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %v", result)
	}
}

func TestEvalExpr_ReverseArray(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{1, 2, 3}}}
	result := evalExpr(doc, bson.D{{Key: "$reverseArray", Value: "$arr"}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 3 || toFloat64(arr[0]) != 3 {
		t.Fatalf("expected reversed [3,2,1], got %v", result)
	}
}

func TestEvalExpr_In_Array(t *testing.T) {
	doc := bson.D{{Key: "x", Value: "b"}}
	result := evalExpr(doc, bson.D{{Key: "$in", Value: bson.A{"$x", bson.A{"a", "b", "c"}}}})
	if result != true {
		t.Fatalf("expected true, got %v", result)
	}
}

func TestEvalExpr_IndexOfArray(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$indexOfArray", Value: bson.A{bson.A{"a", "b", "c"}, "b"}}})
	if toInt64(result) != 1 {
		t.Fatalf("expected index 1, got %v", result)
	}
}

func TestEvalExpr_Range(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$range", Value: bson.A{int32(0), int32(5)}}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 5 {
		t.Fatalf("expected [0..4], got %v", result)
	}
}

func TestEvalExpr_FirstN(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{1, 2, 3, 4, 5}}}
	result := evalExpr(doc, bson.D{{Key: "$firstN", Value: bson.D{
		{Key: "input", Value: "$arr"},
		{Key: "n", Value: int32(3)},
	}}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 3 {
		t.Fatalf("expected 3 elements, got %v", result)
	}
}

func TestEvalExpr_LastN(t *testing.T) {
	doc := bson.D{{Key: "arr", Value: bson.A{1, 2, 3, 4, 5}}}
	result := evalExpr(doc, bson.D{{Key: "$lastN", Value: bson.D{
		{Key: "input", Value: "$arr"},
		{Key: "n", Value: int32(3)},
	}}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 3 || toFloat64(arr[0]) != 3 {
		t.Fatalf("expected [3,4,5], got %v", result)
	}
}

func TestEvalExpr_Filter(t *testing.T) {
	doc := bson.D{{Key: "nums", Value: bson.A{int32(1), int32(2), int32(3), int32(4), int32(5)}}}
	result := evalExpr(doc, bson.D{{Key: "$filter", Value: bson.D{
		{Key: "input", Value: "$nums"},
		{Key: "as", Value: "n"},
		{Key: "cond", Value: bson.D{{Key: "$gt", Value: bson.A{"$$n", int32(2)}}}},
	}}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 3 {
		t.Fatalf("expected [3,4,5], got %v", result)
	}
}

func TestEvalExpr_Map(t *testing.T) {
	doc := bson.D{{Key: "nums", Value: bson.A{int32(1), int32(2), int32(3)}}}
	result := evalExpr(doc, bson.D{{Key: "$map", Value: bson.D{
		{Key: "input", Value: "$nums"},
		{Key: "as", Value: "n"},
		{Key: "in", Value: bson.D{{Key: "$multiply", Value: bson.A{"$$n", int32(2)}}}},
	}}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 3 || toFloat64(arr[2]) != 6 {
		t.Fatalf("expected [2,4,6], got %v", result)
	}
}

func TestEvalExpr_Reduce(t *testing.T) {
	doc := bson.D{{Key: "nums", Value: bson.A{int32(1), int32(2), int32(3), int32(4)}}}
	result := evalExpr(doc, bson.D{{Key: "$reduce", Value: bson.D{
		{Key: "input", Value: "$nums"},
		{Key: "initialValue", Value: int32(0)},
		{Key: "in", Value: bson.D{{Key: "$add", Value: bson.A{"$$value", "$$this"}}}},
	}}})
	if toFloat64(result) != 10 {
		t.Fatalf("expected sum=10, got %v", result)
	}
}

func TestEvalExpr_ArrayToObject(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$arrayToObject", Value: bson.A{
		bson.D{{Key: "k", Value: "name"}, {Key: "v", Value: "Alice"}},
		bson.D{{Key: "k", Value: "age"}, {Key: "v", Value: int32(30)}},
	}}})
	d, ok := result.(bson.D)
	if !ok {
		t.Fatalf("expected bson.D, got %T", result)
	}
	name, _ := GetField(d, "name")
	if name != "Alice" {
		t.Fatalf("expected name=Alice, got %v", name)
	}
}

func TestEvalExpr_ObjectToArray(t *testing.T) {
	doc := bson.D{{Key: "obj", Value: bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}}}
	result := evalExpr(doc, bson.D{{Key: "$objectToArray", Value: "$obj"}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 2 {
		t.Fatalf("expected 2 entries, got %v", result)
	}
}

// ---- Phase 4: Type Operators ----

func TestEvalExpr_ToInt(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$toInt", Value: float64(3.7)}})
	if toInt64(result) != 3 {
		t.Fatalf("expected 3, got %v", result)
	}
}

func TestEvalExpr_ToDouble(t *testing.T) {
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$toDouble", Value: int32(5)}})
	if toFloat64(result) != 5.0 {
		t.Fatalf("expected 5.0, got %v", result)
	}
}

func TestEvalExpr_IsNumber(t *testing.T) {
	doc := bson.D{}
	r1 := evalExpr(doc, bson.D{{Key: "$isNumber", Value: int32(5)}})
	r2 := evalExpr(doc, bson.D{{Key: "$isNumber", Value: "text"}})
	if r1 != true || r2 != false {
		t.Fatalf("$isNumber: got %v, %v", r1, r2)
	}
}

func TestEvalExpr_Type(t *testing.T) {
	doc := bson.D{}
	r1 := evalExpr(doc, bson.D{{Key: "$type", Value: "hello"}})
	if r1 != "string" {
		t.Fatalf("expected 'string', got %v", r1)
	}
	r2 := evalExpr(doc, bson.D{{Key: "$type", Value: int32(1)}})
	if r2 != "int" {
		t.Fatalf("expected 'int', got %v", r2)
	}
}

func TestEvalExpr_ToBool(t *testing.T) {
	doc := bson.D{}
	r1 := evalExpr(doc, bson.D{{Key: "$toBool", Value: int32(1)}})
	r2 := evalExpr(doc, bson.D{{Key: "$toBool", Value: int32(0)}})
	if r1 != true || r2 != false {
		t.Fatalf("$toBool: got %v, %v", r1, r2)
	}
}

// ---- Phase 5: $expr in query matching ----

func TestMatchDoc_Expr(t *testing.T) {
	doc := bson.D{{Key: "qty", Value: int32(10)}, {Key: "price", Value: float64(5)}}
	filter := bson.D{{Key: "$expr", Value: bson.D{{Key: "$gt", Value: bson.A{
		bson.D{{Key: "$multiply", Value: bson.A{"$qty", "$price"}}},
		float64(40),
	}}}}}
	if !MatchDoc(doc, filter) {
		t.Fatal("qty*price=50 should be > 40")
	}
	doc2 := bson.D{{Key: "qty", Value: int32(2)}, {Key: "price", Value: float64(5)}}
	if MatchDoc(doc2, filter) {
		t.Fatal("qty*price=10 should not be > 40")
	}
}

// ---- Phase 7: Additional accumulators ----

func TestGroupAccumulator_Count(t *testing.T) {
	docs := []bson.D{
		{{Key: "g", Value: "x"}},
		{{Key: "g", Value: "x"}},
		{{Key: "g", Value: "x"}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$g"},
			{Key: "n", Value: bson.D{{Key: "$count", Value: bson.D{}}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1 group, err=%v", err)
	}
	n, _ := GetField(out[0], "n")
	if toInt64(n) != 3 {
		t.Fatalf("expected count=3, got %v", n)
	}
}

func TestGroupAccumulator_MergeObjects(t *testing.T) {
	docs := []bson.D{
		{{Key: "g", Value: "x"}, {Key: "data", Value: bson.D{{Key: "a", Value: int32(1)}}}},
		{{Key: "g", Value: "x"}, {Key: "data", Value: bson.D{{Key: "b", Value: int32(2)}}}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$g"},
			{Key: "merged", Value: bson.D{{Key: "$mergeObjects", Value: "$data"}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1, err=%v", err)
	}
	merged, _ := GetField(out[0], "merged")
	d, ok := merged.(bson.D)
	if !ok {
		t.Fatalf("expected bson.D, got %T", merged)
	}
	a, _ := GetField(d, "a")
	b, _ := GetField(d, "b")
	if toInt64(a) != 1 || toInt64(b) != 2 {
		t.Fatalf("expected a=1,b=2 in merged, got %v", d)
	}
}

func TestGroupAccumulator_StdDevPop(t *testing.T) {
	docs := []bson.D{
		{{Key: "g", Value: "x"}, {Key: "v", Value: float64(2)}},
		{{Key: "g", Value: "x"}, {Key: "v", Value: float64(4)}},
		{{Key: "g", Value: "x"}, {Key: "v", Value: float64(6)}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$g"},
			{Key: "std", Value: bson.D{{Key: "$stdDevPop", Value: "$v"}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1, err=%v", err)
	}
	std, _ := GetField(out[0], "std")
	// stddev of [2,4,6] = sqrt(8/3) ≈ 1.633
	if toFloat64(std) < 1.6 || toFloat64(std) > 1.7 {
		t.Fatalf("expected stddev ~1.633, got %v", std)
	}
}

// ---- Distinct engine method ----

func TestDistinct_Basic(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "coll",
		bson.D{{Key: "tag", Value: "go"}},
		bson.D{{Key: "tag", Value: "python"}},
		bson.D{{Key: "tag", Value: "go"}},
	)
	values, err := eng.Distinct("db", "coll", "tag", nil)
	if err != nil {
		t.Fatalf("Distinct error: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 distinct values, got %d: %v", len(values), values)
	}
}

func TestDistinct_WithFilter(t *testing.T) {
	eng, _ := newEng(t)
	mustInsert(t, eng, "db", "coll",
		bson.D{{Key: "tag", Value: "go"}, {Key: "active", Value: true}},
		bson.D{{Key: "tag", Value: "python"}, {Key: "active", Value: false}},
		bson.D{{Key: "tag", Value: "go"}, {Key: "active", Value: false}},
	)
	values, err := eng.Distinct("db", "coll", "tag", bson.D{{Key: "active", Value: true}})
	if err != nil {
		t.Fatalf("Distinct error: %v", err)
	}
	if len(values) != 1 {
		t.Fatalf("expected 1 distinct active tag, got %d: %v", len(values), values)
	}
}

func TestDistinct_NonexistentColl(t *testing.T) {
	eng, _ := newEng(t)
	values, err := eng.Distinct("db", "nosuch", "field", nil)
	if err != nil || len(values) != 0 {
		t.Fatalf("expected empty result for nonexistent coll, got %v err=%v", values, err)
	}
}

// ---- $group with expression in accumulator field ----

func TestGroupAccumulator_SumExpression(t *testing.T) {
	docs := []bson.D{
		{{Key: "g", Value: "a"}, {Key: "qty", Value: int32(2)}, {Key: "price", Value: float64(5)}},
		{{Key: "g", Value: "a"}, {Key: "qty", Value: int32(3)}, {Key: "price", Value: float64(4)}},
	}
	pipeline := []bson.D{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$g"},
			{Key: "revenue", Value: bson.D{{Key: "$sum", Value: bson.D{{Key: "$multiply", Value: bson.A{"$qty", "$price"}}}}}},
		}}},
	}
	out, err := RunPipeline(docs, pipeline, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("expected 1, err=%v", err)
	}
	rev, _ := GetField(out[0], "revenue")
	// 2*5 + 3*4 = 10 + 12 = 22
	if toFloat64(rev) != 22 {
		t.Fatalf("expected revenue=22, got %v", rev)
	}
}

func TestEvalExpr_Filter_EmptyResult(t *testing.T) {
	// Verify $filter returns empty array (not nil) when no elements match
	doc := bson.D{{Key: "nums", Value: bson.A{int32(1), int32(2)}}}
	result := evalExpr(doc, bson.D{{Key: "$filter", Value: bson.D{
		{Key: "input", Value: "$nums"},
		{Key: "cond", Value: bson.D{{Key: "$gt", Value: bson.A{"$$this", int32(10)}}}},
	}}})
	arr, ok := result.(bson.A)
	if !ok {
		t.Fatalf("expected bson.A, got %T", result)
	}
	if arr == nil {
		t.Fatal("expected empty bson.A, not nil")
	}
	if len(arr) != 0 {
		t.Fatalf("expected 0 elements, got %d", len(arr))
	}
}

func TestEvalExpr_IndexOfBytes_StartAfterEnd(t *testing.T) {
	// start > end should not panic, return -1
	doc := bson.D{}
	result := evalExpr(doc, bson.D{{Key: "$indexOfBytes", Value: bson.A{"hello", "l", int32(4), int32(2)}}})
	if toInt64(result) != -1 {
		t.Fatalf("expected -1 for start>end, got %v", result)
	}
}

func TestEvalExpr_FirstN_Negative(t *testing.T) {
	// Negative n should return empty array (not panic)
	doc := bson.D{{Key: "arr", Value: bson.A{1, 2, 3}}}
	result := evalExpr(doc, bson.D{{Key: "$firstN", Value: bson.D{
		{Key: "input", Value: "$arr"},
		{Key: "n", Value: int32(-1)},
	}}})
	arr, ok := result.(bson.A)
	if !ok || len(arr) != 0 {
		t.Fatalf("expected empty array for negative n, got %v", result)
	}
}
