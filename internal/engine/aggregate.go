package engine

import (
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// LookupFunc fetches documents from another collection for $lookup.
type LookupFunc func(db, coll string, filter bson.D) ([]bson.D, error)

// RunPipeline executes an aggregation pipeline on the given documents.
func RunPipeline(docs []bson.D, pipeline []bson.D, lookupFn LookupFunc) ([]bson.D, error) {
	current := docs
	for _, stage := range pipeline {
		if len(stage) != 1 {
			return nil, fmt.Errorf("pipeline stage must have exactly one field")
		}
		stageOp := stage[0].Key
		stageVal := stage[0].Value

		var err error
		switch stageOp {
		case "$match":
			filter, ok := stageVal.(bson.D)
			if !ok {
				return nil, fmt.Errorf("$match requires a document")
			}
			current = FilterDocs(current, filter)

		case "$limit":
			limit := int(toInt64(stageVal))
			if limit < len(current) {
				current = current[:limit]
			}

		case "$skip":
			skip := int(toInt64(stageVal))
			if skip >= len(current) {
				current = nil
			} else {
				current = current[skip:]
			}

		case "$sort":
			sortSpec, ok := stageVal.(bson.D)
			if !ok {
				return nil, fmt.Errorf("$sort requires a document")
			}
			sorted := make([]bson.D, len(current))
			copy(sorted, current)
			SortDocs(sorted, sortSpec)
			current = sorted

		case "$project":
			spec, ok := stageVal.(bson.D)
			if !ok {
				return nil, fmt.Errorf("$project requires a document")
			}
			current, err = ProjectDocs(current, spec)
			if err != nil {
				return nil, err
			}

		case "$addFields", "$set":
			spec, ok := stageVal.(bson.D)
			if !ok {
				return nil, fmt.Errorf("%s requires a document", stageOp)
			}
			var next []bson.D
			for _, doc := range current {
				d := doc
				for _, s := range spec {
					computed := evalExpr(d, s.Value)
					d = SetField(d, s.Key, computed)
				}
				next = append(next, d)
			}
			current = next

		case "$unset":
			var fields []string
			switch v := stageVal.(type) {
			case string:
				fields = []string{v}
			case bson.A:
				for _, item := range v {
					if s, ok := item.(string); ok {
						fields = append(fields, s)
					}
				}
			default:
				return nil, fmt.Errorf("$unset requires a string or array of strings")
			}
			var next []bson.D
			for _, doc := range current {
				d := doc
				for _, field := range fields {
					d = UnsetField(d, field)
				}
				next = append(next, d)
			}
			current = next

		case "$replaceRoot", "$replaceWith":
			var newRootExpr interface{}
			if stageOp == "$replaceWith" {
				newRootExpr = stageVal
			} else {
				spec, ok := stageVal.(bson.D)
				if !ok {
					return nil, fmt.Errorf("$replaceRoot requires a document")
				}
				for _, s := range spec {
					if s.Key == "newRoot" {
						newRootExpr = s.Value
						break
					}
				}
			}
			var next []bson.D
			for _, doc := range current {
				newRoot := evalExpr(doc, newRootExpr)
				nd, ok := newRoot.(bson.D)
				if !ok {
					return nil, fmt.Errorf("$replaceRoot: newRoot expression must evaluate to a document")
				}
				next = append(next, nd)
			}
			current = next

		case "$sortByCount":
			// Shorthand for {$group: {_id: expr, count: {$sum: 1}}}, {$sort: {count: -1}}
			groupSpec := bson.D{
				{Key: "_id", Value: stageVal},
				{Key: "count", Value: bson.D{{Key: "$sum", Value: int32(1)}}},
			}
			current, err = groupDocs(current, groupSpec)
			if err != nil {
				return nil, err
			}
			SortDocs(current, bson.D{{Key: "count", Value: int32(-1)}})

		case "$unwind":
			path, ok := stageVal.(string)
			if !ok {
				return nil, fmt.Errorf("$unwind requires a string field path")
			}
			current, err = unwindDocs(current, path)
			if err != nil {
				return nil, err
			}

		case "$group":
			groupSpec, ok := stageVal.(bson.D)
			if !ok {
				return nil, fmt.Errorf("$group requires a document")
			}
			current, err = groupDocs(current, groupSpec)
			if err != nil {
				return nil, err
			}

		case "$count":
			fieldName, ok := stageVal.(string)
			if !ok {
				return nil, fmt.Errorf("$count requires a string")
			}
			current = []bson.D{{bson.E{Key: fieldName, Value: int64(len(current))}}}

		case "$lookup":
			spec, ok := stageVal.(bson.D)
			if !ok {
				return nil, fmt.Errorf("$lookup requires a document")
			}
			current, err = lookupDocs(current, spec, lookupFn)
			if err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("unsupported pipeline stage: %s", stageOp)
		}
	}
	return current, nil
}

// ProjectDocs applies a projection spec to a slice of documents.
func ProjectDocs(docs []bson.D, spec bson.D) ([]bson.D, error) {
	// Determine inclusion or exclusion mode.
	// A field is "exclusion" only if its value is explicitly 0/false.
	// Computed expressions and 1/true values indicate inclusion mode.
	isInclusion := false
	isExclusion := false
	for _, s := range spec {
		if s.Key == "_id" {
			continue
		}
		if isExplicitZero(s.Value) {
			isExclusion = true
		} else {
			isInclusion = true
		}
	}

	var result []bson.D
	for _, doc := range docs {
		var projected bson.D
		if isInclusion {
			// Include only specified fields (+ _id by default unless excluded)
			includeID := true
			for _, s := range spec {
				if s.Key == "_id" && isExplicitZero(s.Value) {
					includeID = false
				}
			}
			if includeID {
				if v, ok := GetField(doc, "_id"); ok {
					projected = append(projected, bson.E{Key: "_id", Value: v})
				}
			}
			for _, s := range spec {
				if s.Key == "_id" {
					continue
				}
				if isExplicitZero(s.Value) {
					continue
				}
				if isExplicitOne(s.Value) {
					// Include field as-is
					if v, ok := GetField(doc, s.Key); ok {
						projected = append(projected, bson.E{Key: s.Key, Value: v})
					}
				} else {
					// Computed expression
					computed := evalExpr(doc, s.Value)
					projected = append(projected, bson.E{Key: s.Key, Value: computed})
				}
			}
		} else if isExclusion {
			excludeSet := make(map[string]bool)
			for _, s := range spec {
				if isExplicitZero(s.Value) {
					excludeSet[s.Key] = true
				}
			}
			for _, e := range doc {
				if !excludeSet[e.Key] {
					projected = append(projected, e)
				}
			}
		} else {
			projected = doc
		}
		result = append(result, projected)
	}
	return result, nil
}

func unwindDocs(docs []bson.D, path string) ([]bson.D, error) {
	// Strip leading $ from path
	if len(path) > 0 && path[0] == '$' {
		path = path[1:]
	}
	var result []bson.D
	for _, doc := range docs {
		val, exists := GetField(doc, path)
		if !exists {
			continue // skip documents without the field
		}
		arr, ok := val.(bson.A)
		if !ok {
			// Not an array, pass through as-is
			result = append(result, doc)
			continue
		}
		for _, elem := range arr {
			newDoc, err := CopyDoc(doc)
			if err != nil {
				return nil, err
			}
			newDoc = SetField(newDoc, path, elem)
			result = append(result, newDoc)
		}
	}
	return result, nil
}

// groupKeyString converts a group key into a canonical string that is unique
// per distinct (type, value) pair, preventing collisions like int32(1) vs int64(1).
func groupKeyString(key interface{}) string {
	switch v := key.(type) {
	case nil:
		return "nil:"
	case string:
		return "string:" + v
	case int32:
		return fmt.Sprintf("int32:%d", v)
	case int64:
		return fmt.Sprintf("int64:%d", v)
	case float64:
		return fmt.Sprintf("float64:%v", v)
	case bool:
		return fmt.Sprintf("bool:%v", v)
	case bson.ObjectID:
		return "objectid:" + v.Hex()
	case bson.D:
		// Compound _id: marshal to BSON bytes and hex-encode for a stable canonical key.
		b, err := bson.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%T:%v", v, v)
		}
		return "bsond:" + hex.EncodeToString(b)
	default:
		return fmt.Sprintf("%T:%v", v, v)
	}
}

func groupDocs(docs []bson.D, spec bson.D) ([]bson.D, error) {
	// Find _id expression
	var idExpr interface{}
	for _, s := range spec {
		if s.Key == "_id" {
			idExpr = s.Value
			break
		}
	}

	type group struct {
		id   interface{}
		docs []bson.D
	}
	var groups []group
	groupIndex := make(map[string]int) // key -> index in groups

	for _, doc := range docs {
		key := evalExpr(doc, idExpr)
		keyStr := groupKeyString(key)
		idx, exists := groupIndex[keyStr]
		if !exists {
			idx = len(groups)
			groupIndex[keyStr] = idx
			groups = append(groups, group{id: key})
		}
		groups[idx].docs = append(groups[idx].docs, doc)
	}

	var result []bson.D
	for _, g := range groups {
		outDoc := bson.D{bson.E{Key: "_id", Value: g.id}}
		for _, s := range spec {
			if s.Key == "_id" {
				continue
			}
			accSpec, ok := s.Value.(bson.D)
			if !ok || len(accSpec) == 0 {
				continue
			}
			accOp := accSpec[0].Key
			accField := accSpec[0].Value
			val := computeAccumulator(g.docs, accOp, accField)
			outDoc = append(outDoc, bson.E{Key: s.Key, Value: val})
		}
		result = append(result, outDoc)
	}
	return result, nil
}

func computeAccumulator(docs []bson.D, op string, field interface{}) interface{} {
	switch op {
	case "$sum":
		var sum float64
		allInt := true
		for _, doc := range docs {
			v := evalExpr(doc, field)
			if v == nil {
				continue
			}
			if !isInt(v) {
				allInt = false
			}
			sum += toFloat64(v)
		}
		if allInt {
			return int64(sum)
		}
		return sum

	case "$avg":
		var sum float64
		count := 0
		for _, doc := range docs {
			v := evalExpr(doc, field)
			if v == nil {
				continue
			}
			sum += toFloat64(v)
			count++
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)

	case "$min":
		var minVal interface{}
		for _, doc := range docs {
			v := evalExpr(doc, field)
			if v == nil {
				continue
			}
			if minVal == nil || compareValues(v, minVal) < 0 {
				minVal = v
			}
		}
		return minVal

	case "$max":
		var maxVal interface{}
		for _, doc := range docs {
			v := evalExpr(doc, field)
			if v == nil {
				continue
			}
			if maxVal == nil || compareValues(v, maxVal) > 0 {
				maxVal = v
			}
		}
		return maxVal

	case "$first":
		if len(docs) == 0 {
			return nil
		}
		return evalExpr(docs[0], field)

	case "$last":
		if len(docs) == 0 {
			return nil
		}
		return evalExpr(docs[len(docs)-1], field)

	case "$push":
		var arr bson.A
		for _, doc := range docs {
			v := evalExpr(doc, field)
			arr = append(arr, v)
		}
		return arr

	case "$addToSet":
		var arr bson.A
		for _, doc := range docs {
			v := evalExpr(doc, field)
			found := false
			for _, existing := range arr {
				if valuesEqual(existing, v) {
					found = true
					break
				}
			}
			if !found {
				arr = append(arr, v)
			}
		}
		return arr

	case "$count":
		// {$count: {}} — count documents in group
		return int64(len(docs))

	case "$stdDevPop":
		var vals []float64
		for _, doc := range docs {
			v := evalExpr(doc, field)
			if v == nil {
				continue
			}
			vals = append(vals, toFloat64(v))
		}
		if len(vals) == 0 {
			return nil
		}
		var sum float64
		for _, v := range vals {
			sum += v
		}
		mean := sum / float64(len(vals))
		var variance float64
		for _, v := range vals {
			diff := v - mean
			variance += diff * diff
		}
		variance /= float64(len(vals))
		return math.Sqrt(variance)

	case "$stdDevSamp":
		var vals []float64
		for _, doc := range docs {
			v := evalExpr(doc, field)
			if v == nil {
				continue
			}
			vals = append(vals, toFloat64(v))
		}
		if len(vals) < 2 {
			return nil
		}
		var sum float64
		for _, v := range vals {
			sum += v
		}
		mean := sum / float64(len(vals))
		var variance float64
		for _, v := range vals {
			diff := v - mean
			variance += diff * diff
		}
		variance /= float64(len(vals) - 1)
		return math.Sqrt(variance)

	case "$mergeObjects":
		merged := bson.D{}
		for _, doc := range docs {
			v := evalExpr(doc, field)
			if sub, ok := v.(bson.D); ok {
				for _, e := range sub {
					merged = SetField(merged, e.Key, e.Value)
				}
			}
		}
		return merged
	}
	return nil
}

func lookupDocs(docs []bson.D, spec bson.D, lookupFn LookupFunc) ([]bson.D, error) {
	if lookupFn == nil {
		return nil, fmt.Errorf("$lookup not supported without lookup function")
	}

	var from, localField, foreignField, as string
	for _, s := range spec {
		switch s.Key {
		case "from":
			from, _ = s.Value.(string)
		case "localField":
			localField, _ = s.Value.(string)
		case "foreignField":
			foreignField, _ = s.Value.(string)
		case "as":
			as, _ = s.Value.(string)
		}
	}

	var result []bson.D
	for _, doc := range docs {
		localVal, _ := GetField(doc, localField)
		filter := bson.D{{Key: foreignField, Value: localVal}}
		matched, err := lookupFn("", from, filter)
		if err != nil {
			return nil, err
		}
		// Convert to bson.A
		var matchedArr bson.A
		for _, m := range matched {
			matchedArr = append(matchedArr, m)
		}
		newDoc, err := CopyDoc(doc)
		if err != nil {
			return nil, err
		}
		newDoc = SetField(newDoc, as, matchedArr)
		result = append(result, newDoc)
	}
	return result, nil
}

// ---- Expression Evaluator ----

// evalExpr evaluates a MongoDB aggregation expression against a document.
// It supports field paths ($field), operator documents ({$op: args}),
// object expressions ({key: expr}), user-defined variables ($$varName),
// and constants.
func evalExpr(doc bson.D, expr interface{}) interface{} {
	switch e := expr.(type) {
	case string:
		if strings.HasPrefix(e, "$$") {
			// Variable reference ($$this, $$value, user vars)
			varName := e[2:]
			// Look for "$$varName" key injected into doc by $filter/$map/$reduce
			for _, elem := range doc {
				if elem.Key == "$$"+varName {
					return elem.Value
				}
			}
			return nil
		}
		if strings.HasPrefix(e, "$") {
			v, _ := GetField(doc, e[1:])
			return v
		}
		return e
	case bson.D:
		if len(e) == 0 {
			return bson.D{}
		}
		if len(e[0].Key) > 0 && e[0].Key[0] == '$' {
			return evalOperator(doc, e[0].Key, e[0].Value)
		}
		// Object expression: evaluate each field
		result := bson.D{}
		for _, elem := range e {
			result = append(result, bson.E{Key: elem.Key, Value: evalExpr(doc, elem.Value)})
		}
		return result
	default:
		return expr
	}
}

// evalOperator dispatches to the appropriate operator implementation.
func evalOperator(doc bson.D, op string, args interface{}) interface{} {
	switch op {
	// ---- Arithmetic ----
	case "$add":
		arr := evalExprArray(doc, args)
		var sum float64
		allInt := true
		for _, v := range arr {
			if !isInt(v) {
				allInt = false
			}
			sum += toFloat64(v)
		}
		if allInt {
			return int64(sum)
		}
		return sum

	case "$subtract":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return nil
		}
		a, b := toFloat64(arr[0]), toFloat64(arr[1])
		if isInt(arr[0]) && isInt(arr[1]) {
			return int64(a - b)
		}
		return a - b

	case "$multiply":
		arr := evalExprArray(doc, args)
		prod := 1.0
		allInt := true
		for _, v := range arr {
			if !isInt(v) {
				allInt = false
			}
			prod *= toFloat64(v)
		}
		if allInt {
			return int64(prod)
		}
		return prod

	case "$divide":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return nil
		}
		b := toFloat64(arr[1])
		if b == 0 {
			return nil
		}
		return toFloat64(arr[0]) / b

	case "$mod":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return nil
		}
		a, b := toFloat64(arr[0]), toFloat64(arr[1])
		if b == 0 {
			return nil
		}
		if isInt(arr[0]) && isInt(arr[1]) {
			return int64(a) % int64(b)
		}
		return math.Mod(a, b)

	case "$abs":
		v := evalExpr(doc, args)
		f := toFloat64(v)
		if isInt(v) {
			return int64(math.Abs(f))
		}
		return math.Abs(f)

	case "$ceil":
		v := evalExpr(doc, args)
		return math.Ceil(toFloat64(v))

	case "$floor":
		v := evalExpr(doc, args)
		return math.Floor(toFloat64(v))

	case "$round":
		arr := evalExprArray(doc, args)
		if len(arr) == 0 {
			return nil
		}
		f := toFloat64(arr[0])
		places := 0
		if len(arr) >= 2 {
			places = int(toInt64(arr[1]))
		}
		factor := math.Pow(10, float64(places))
		return math.Round(f*factor) / factor

	case "$trunc":
		arr := evalExprArray(doc, args)
		if len(arr) == 0 {
			return nil
		}
		f := toFloat64(arr[0])
		places := 0
		if len(arr) >= 2 {
			places = int(toInt64(arr[1]))
		}
		factor := math.Pow(10, float64(places))
		return math.Trunc(f*factor) / factor

	case "$sqrt":
		v := evalExpr(doc, args)
		return math.Sqrt(toFloat64(v))

	case "$pow":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return nil
		}
		return math.Pow(toFloat64(arr[0]), toFloat64(arr[1]))

	case "$log":
		arr := evalExprArray(doc, args)
		if len(arr) == 0 {
			return nil
		}
		if len(arr) == 1 {
			return math.Log(toFloat64(arr[0]))
		}
		return math.Log(toFloat64(arr[0])) / math.Log(toFloat64(arr[1]))

	case "$log10":
		v := evalExpr(doc, args)
		return math.Log10(toFloat64(v))

	case "$exp":
		v := evalExpr(doc, args)
		return math.Exp(toFloat64(v))

	// ---- Comparison ----
	case "$eq":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return false
		}
		return valuesEqual(arr[0], arr[1])

	case "$ne":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return false
		}
		return !valuesEqual(arr[0], arr[1])

	case "$gt":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return false
		}
		return compareValues(arr[0], arr[1]) > 0

	case "$gte":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return false
		}
		return compareValues(arr[0], arr[1]) >= 0

	case "$lt":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return false
		}
		return compareValues(arr[0], arr[1]) < 0

	case "$lte":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return false
		}
		return compareValues(arr[0], arr[1]) <= 0

	case "$cmp":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return int32(0)
		}
		return int32(compareValues(arr[0], arr[1]))

	// ---- Boolean ----
	case "$and":
		arr := evalExprArray(doc, args)
		for _, v := range arr {
			if !isTruthy(v) {
				return false
			}
		}
		return true

	case "$or":
		arr := evalExprArray(doc, args)
		for _, v := range arr {
			if isTruthy(v) {
				return true
			}
		}
		return false

	case "$not":
		arr := evalExprArray(doc, args)
		if len(arr) != 1 {
			return false
		}
		return !isTruthy(arr[0])

	// ---- Conditional ----
	case "$cond":
		switch v := args.(type) {
		case bson.A:
			if len(v) != 3 {
				return nil
			}
			cond := evalExpr(doc, v[0])
			if isTruthy(cond) {
				return evalExpr(doc, v[1])
			}
			return evalExpr(doc, v[2])
		case bson.D:
			var ifExpr, thenExpr, elseExpr interface{}
			for _, e := range v {
				switch e.Key {
				case "if":
					ifExpr = e.Value
				case "then":
					thenExpr = e.Value
				case "else":
					elseExpr = e.Value
				}
			}
			if isTruthy(evalExpr(doc, ifExpr)) {
				return evalExpr(doc, thenExpr)
			}
			return evalExpr(doc, elseExpr)
		}
		return nil

	case "$ifNull":
		arr, ok := args.(bson.A)
		if !ok {
			return nil
		}
		for _, item := range arr {
			v := evalExpr(doc, item)
			if v != nil {
				return v
			}
		}
		return nil

	case "$switch":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var branches bson.A
		var defaultExpr interface{}
		for _, e := range spec {
			switch e.Key {
			case "branches":
				branches, _ = e.Value.(bson.A)
			case "default":
				defaultExpr = e.Value
			}
		}
		for _, branch := range branches {
			b, ok := branch.(bson.D)
			if !ok {
				continue
			}
			var caseExpr, thenExpr interface{}
			for _, e := range b {
				switch e.Key {
				case "case":
					caseExpr = e.Value
				case "then":
					thenExpr = e.Value
				}
			}
			if isTruthy(evalExpr(doc, caseExpr)) {
				return evalExpr(doc, thenExpr)
			}
		}
		if defaultExpr != nil {
			return evalExpr(doc, defaultExpr)
		}
		return nil

	// ---- String ----
	case "$concat":
		arr, ok := args.(bson.A)
		if !ok {
			return nil
		}
		var sb strings.Builder
		for _, item := range arr {
			v := evalExpr(doc, item)
			s, ok := v.(string)
			if !ok {
				return nil
			}
			sb.WriteString(s)
		}
		return sb.String()

	case "$toLower":
		v := evalExpr(doc, args)
		s, ok := v.(string)
		if !ok {
			return ""
		}
		return strings.ToLower(s)

	case "$toUpper":
		v := evalExpr(doc, args)
		s, ok := v.(string)
		if !ok {
			return ""
		}
		return strings.ToUpper(s)

	case "$trim":
		return evalTrim(doc, args, true, true)
	case "$ltrim":
		return evalTrim(doc, args, true, false)
	case "$rtrim":
		return evalTrim(doc, args, false, true)

	case "$split":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return nil
		}
		s, ok := arr[0].(string)
		if !ok {
			return nil
		}
		delim, ok := arr[1].(string)
		if !ok {
			return nil
		}
		parts := strings.Split(s, delim)
		result := make(bson.A, len(parts))
		for i, p := range parts {
			result[i] = p
		}
		return result

	case "$strLenBytes":
		v := evalExpr(doc, args)
		s, ok := v.(string)
		if !ok {
			return nil
		}
		return int32(len([]byte(s)))

	case "$strLenCP":
		v := evalExpr(doc, args)
		s, ok := v.(string)
		if !ok {
			return nil
		}
		return int32(len([]rune(s)))

	case "$substr", "$substrBytes":
		arr := evalExprArray(doc, args)
		if len(arr) != 3 {
			return ""
		}
		s, ok := arr[0].(string)
		if !ok {
			return ""
		}
		b := []byte(s)
		start := int(toInt64(arr[1]))
		length := int(toInt64(arr[2]))
		if start < 0 {
			start = 0
		}
		if start > len(b) {
			return ""
		}
		end := start + length
		if length < 0 || end > len(b) {
			end = len(b)
		}
		return string(b[start:end])

	case "$substrCP":
		arr := evalExprArray(doc, args)
		if len(arr) != 3 {
			return ""
		}
		s, ok := arr[0].(string)
		if !ok {
			return ""
		}
		runes := []rune(s)
		start := int(toInt64(arr[1]))
		length := int(toInt64(arr[2]))
		if start < 0 {
			start = 0
		}
		if start > len(runes) {
			return ""
		}
		end := start + length
		if length < 0 || end > len(runes) {
			end = len(runes)
		}
		return string(runes[start:end])

	case "$replaceOne":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var input, find, replacement string
		for _, e := range spec {
			v := evalExpr(doc, e.Value)
			s, _ := v.(string)
			switch e.Key {
			case "input":
				input = s
			case "find":
				find = s
			case "replacement":
				replacement = s
			}
		}
		return strings.Replace(input, find, replacement, 1)

	case "$replaceAll":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var input, find, replacement string
		for _, e := range spec {
			v := evalExpr(doc, e.Value)
			s, _ := v.(string)
			switch e.Key {
			case "input":
				input = s
			case "find":
				find = s
			case "replacement":
				replacement = s
			}
		}
		return strings.ReplaceAll(input, find, replacement)

	case "$strcasecmp":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return int32(0)
		}
		a, _ := arr[0].(string)
		b, _ := arr[1].(string)
		a = strings.ToLower(a)
		b = strings.ToLower(b)
		if a < b {
			return int32(-1)
		}
		if a > b {
			return int32(1)
		}
		return int32(0)

	case "$indexOfBytes":
		arr := evalExprArray(doc, args)
		if len(arr) < 2 {
			return int32(-1)
		}
		s, ok := arr[0].(string)
		if !ok {
			return int32(-1)
		}
		sub, ok := arr[1].(string)
		if !ok {
			return int32(-1)
		}
		start := 0
		end := len(s)
		if len(arr) >= 3 {
			start = int(toInt64(arr[2]))
		}
		if len(arr) >= 4 {
			end = int(toInt64(arr[3]))
		}
		sb := []byte(s)
		if start < 0 || start > len(sb) {
			return int32(-1)
		}
		if end > len(sb) {
			end = len(sb)
		}
		if end < start {
			end = start
		}
		idx := strings.Index(string(sb[start:end]), sub)
		if idx < 0 {
			return int32(-1)
		}
		return int32(start + idx)

	case "$toString":
		v := evalExpr(doc, args)
		return valueToString(v)

	// ---- Literal ----
	case "$literal":
		return args

	// ---- Array ----
	case "$size":
		v := evalExpr(doc, args)
		arr, ok := v.(bson.A)
		if !ok {
			return nil
		}
		return int32(len(arr))

	case "$arrayElemAt":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return nil
		}
		a, ok := arr[0].(bson.A)
		if !ok {
			return nil
		}
		idx := int(toInt64(arr[1]))
		if idx < 0 {
			idx = len(a) + idx
		}
		if idx < 0 || idx >= len(a) {
			return nil
		}
		return a[idx]

	case "$isArray":
		v := evalExpr(doc, args)
		_, ok := v.(bson.A)
		return ok

	case "$concatArrays":
		arr, ok := args.(bson.A)
		if !ok {
			return nil
		}
		var result bson.A
		for _, item := range arr {
			v := evalExpr(doc, item)
			a, ok := v.(bson.A)
			if !ok {
				return nil
			}
			result = append(result, a...)
		}
		return result

	case "$slice":
		arr := evalExprArray(doc, args)
		if len(arr) < 2 {
			return nil
		}
		a, ok := arr[0].(bson.A)
		if !ok {
			return nil
		}
		if len(arr) == 2 {
			n := int(toInt64(arr[1]))
			if n < 0 {
				// last n elements
				start := len(a) + n
				if start < 0 {
					start = 0
				}
				return a[start:]
			}
			if n > len(a) {
				n = len(a)
			}
			return a[:n]
		}
		// [arr, start, count]
		start := int(toInt64(arr[1]))
		count := int(toInt64(arr[2]))
		if start < 0 {
			start = len(a) + start
		}
		if start < 0 {
			start = 0
		}
		if start >= len(a) {
			return bson.A{}
		}
		end := start + count
		if end > len(a) {
			end = len(a)
		}
		return a[start:end]

	case "$reverseArray":
		v := evalExpr(doc, args)
		a, ok := v.(bson.A)
		if !ok {
			return nil
		}
		result := make(bson.A, len(a))
		for i, elem := range a {
			result[len(a)-1-i] = elem
		}
		return result

	case "$in":
		arr := evalExprArray(doc, args)
		if len(arr) != 2 {
			return false
		}
		needle := arr[0]
		haystack, ok := arr[1].(bson.A)
		if !ok {
			return false
		}
		for _, item := range haystack {
			if valuesEqual(item, needle) {
				return true
			}
		}
		return false

	case "$indexOfArray":
		arr := evalExprArray(doc, args)
		if len(arr) < 2 {
			return int32(-1)
		}
		a, ok := arr[0].(bson.A)
		if !ok {
			return int32(-1)
		}
		needle := arr[1]
		start := 0
		end := len(a)
		if len(arr) >= 3 {
			start = int(toInt64(arr[2]))
		}
		if len(arr) >= 4 {
			end = int(toInt64(arr[3]))
		}
		if start < 0 {
			start = 0
		}
		if end > len(a) {
			end = len(a)
		}
		for i := start; i < end; i++ {
			if valuesEqual(a[i], needle) {
				return int32(i)
			}
		}
		return int32(-1)

	case "$range":
		arr := evalExprArray(doc, args)
		if len(arr) < 2 {
			return nil
		}
		start := int(toInt64(arr[0]))
		end := int(toInt64(arr[1]))
		step := 1
		if len(arr) >= 3 {
			step = int(toInt64(arr[2]))
		}
		if step == 0 {
			return nil
		}
		var result bson.A
		for i := start; (step > 0 && i < end) || (step < 0 && i > end); i += step {
			result = append(result, int32(i))
		}
		return result

	case "$firstN":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var inputExpr, nExpr interface{}
		for _, e := range spec {
			switch e.Key {
			case "input":
				inputExpr = e.Value
			case "n":
				nExpr = e.Value
			}
		}
		v := evalExpr(doc, inputExpr)
		a, ok := v.(bson.A)
		if !ok {
			return nil
		}
		n := int(toInt64(evalExpr(doc, nExpr)))
		if n < 0 {
			n = 0
		}
		if n > len(a) {
			n = len(a)
		}
		return a[:n]

	case "$lastN":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var inputExpr, nExpr interface{}
		for _, e := range spec {
			switch e.Key {
			case "input":
				inputExpr = e.Value
			case "n":
				nExpr = e.Value
			}
		}
		v := evalExpr(doc, inputExpr)
		a, ok := v.(bson.A)
		if !ok {
			return nil
		}
		n := int(toInt64(evalExpr(doc, nExpr)))
		if n < 0 {
			n = 0
		}
		if n > len(a) {
			n = len(a)
		}
		return a[len(a)-n:]

	case "$filter":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var inputExpr, condExpr interface{}
		varName := "this"
		for _, e := range spec {
			switch e.Key {
			case "input":
				inputExpr = e.Value
			case "as":
				if s, ok := e.Value.(string); ok {
					varName = s
				}
			case "cond":
				condExpr = e.Value
			}
		}
		v := evalExpr(doc, inputExpr)
		a, ok := v.(bson.A)
		if !ok {
			return nil
		}
		result := bson.A{}
		for _, elem := range a {
			augDoc := append(bson.D{{Key: "$$" + varName, Value: elem}}, doc...)
			if isTruthy(evalExpr(augDoc, condExpr)) {
				result = append(result, elem)
			}
		}
		return result

	case "$map":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var inputExpr, inExpr interface{}
		varName := "this"
		for _, e := range spec {
			switch e.Key {
			case "input":
				inputExpr = e.Value
			case "as":
				if s, ok := e.Value.(string); ok {
					varName = s
				}
			case "in":
				inExpr = e.Value
			}
		}
		v := evalExpr(doc, inputExpr)
		a, ok := v.(bson.A)
		if !ok {
			return nil
		}
		result := make(bson.A, len(a))
		for i, elem := range a {
			augDoc := append(bson.D{{Key: "$$" + varName, Value: elem}}, doc...)
			result[i] = evalExpr(augDoc, inExpr)
		}
		return result

	case "$reduce":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var inputExpr, initialValueExpr, inExpr interface{}
		for _, e := range spec {
			switch e.Key {
			case "input":
				inputExpr = e.Value
			case "initialValue":
				initialValueExpr = e.Value
			case "in":
				inExpr = e.Value
			}
		}
		v := evalExpr(doc, inputExpr)
		a, ok := v.(bson.A)
		if !ok {
			return nil
		}
		accumulator := evalExpr(doc, initialValueExpr)
		for _, elem := range a {
			augDoc := append(bson.D{
				{Key: "$$value", Value: accumulator},
				{Key: "$$this", Value: elem},
			}, doc...)
			accumulator = evalExpr(augDoc, inExpr)
		}
		return accumulator

	case "$sortArray":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var inputExpr interface{}
		var sortBy bson.D
		for _, e := range spec {
			switch e.Key {
			case "input":
				inputExpr = e.Value
			case "sortBy":
				sortBy, _ = e.Value.(bson.D)
			}
		}
		v := evalExpr(doc, inputExpr)
		a, ok := v.(bson.A)
		if !ok {
			return nil
		}
		// Convert to []bson.D for sorting
		sorted := make(bson.A, len(a))
		copy(sorted, a)
		sort.SliceStable(sorted, func(i, j int) bool {
			di, oki := sorted[i].(bson.D)
			dj, okj := sorted[j].(bson.D)
			if oki && okj && len(sortBy) > 0 {
				return compareDocs(di, dj, sortBy) < 0
			}
			return compareValues(sorted[i], sorted[j]) < 0
		})
		return sorted

	case "$arrayToObject":
		v := evalExpr(doc, args)
		a, ok := v.(bson.A)
		if !ok {
			return nil
		}
		result := bson.D{}
		for _, item := range a {
			switch elem := item.(type) {
			case bson.D:
				var k string
				var val interface{}
				for _, e := range elem {
					switch e.Key {
					case "k":
						k, _ = e.Value.(string)
					case "v":
						val = e.Value
					}
				}
				if k != "" {
					result = SetField(result, k, val)
				}
			case bson.A:
				if len(elem) == 2 {
					k, _ := elem[0].(string)
					if k != "" {
						result = SetField(result, k, elem[1])
					}
				}
			}
		}
		return result

	case "$objectToArray":
		v := evalExpr(doc, args)
		d, ok := v.(bson.D)
		if !ok {
			return nil
		}
		result := make(bson.A, len(d))
		for i, e := range d {
			result[i] = bson.D{{Key: "k", Value: e.Key}, {Key: "v", Value: e.Value}}
		}
		return result

	case "$zip":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var inputs bson.A
		useLongest := false
		var defaults bson.A
		for _, e := range spec {
			switch e.Key {
			case "inputs":
				inputs, _ = e.Value.(bson.A)
			case "useLongestLength":
				useLongest, _ = e.Value.(bool)
			case "defaults":
				defaults, _ = e.Value.(bson.A)
			}
		}
		arrays := make([]bson.A, 0, len(inputs))
		for _, inp := range inputs {
			v := evalExpr(doc, inp)
			a, ok := v.(bson.A)
			if !ok {
				return nil
			}
			arrays = append(arrays, a)
		}
		if len(arrays) == 0 {
			return bson.A{}
		}
		length := len(arrays[0])
		if useLongest {
			for _, a := range arrays {
				if len(a) > length {
					length = len(a)
				}
			}
		} else {
			for _, a := range arrays {
				if len(a) < length {
					length = len(a)
				}
			}
		}
		result := make(bson.A, length)
		for i := 0; i < length; i++ {
			row := make(bson.A, len(arrays))
			for j, a := range arrays {
				if i < len(a) {
					row[j] = a[i]
				} else if j < len(defaults) {
					row[j] = defaults[j]
				} else {
					row[j] = nil
				}
			}
			result[i] = row
		}
		return result

	// ---- Type ----
	case "$toInt":
		v := evalExpr(doc, args)
		return int32(toInt64(v))

	case "$toLong":
		v := evalExpr(doc, args)
		return toInt64(v)

	case "$toDouble":
		v := evalExpr(doc, args)
		return toFloat64(v)

	case "$toDecimal":
		v := evalExpr(doc, args)
		return toFloat64(v)

	case "$toBool":
		v := evalExpr(doc, args)
		return isTruthy(v)

	case "$toObjectId":
		v := evalExpr(doc, args)
		s, ok := v.(string)
		if !ok {
			return nil
		}
		id, err := bson.ObjectIDFromHex(s)
		if err != nil {
			return nil
		}
		return id

	case "$isNumber":
		v := evalExpr(doc, args)
		return isNumeric(v)

	case "$type":
		v := evalExpr(doc, args)
		return bsonTypeName(v)

	case "$convert":
		spec, ok := args.(bson.D)
		if !ok {
			return nil
		}
		var inputExpr interface{}
		var toType string
		var onErrorExpr, onNullExpr interface{}
		for _, e := range spec {
			switch e.Key {
			case "input":
				inputExpr = e.Value
			case "to":
				toType, _ = e.Value.(string)
			case "onError":
				onErrorExpr = e.Value
			case "onNull":
				onNullExpr = e.Value
			}
		}
		v := evalExpr(doc, inputExpr)
		if v == nil {
			if onNullExpr != nil {
				return evalExpr(doc, onNullExpr)
			}
			return nil
		}
		_ = onErrorExpr
		switch toType {
		case "int", "16":
			return int32(toInt64(v))
		case "long", "18":
			return toInt64(v)
		case "double", "1":
			return toFloat64(v)
		case "bool", "8":
			return isTruthy(v)
		case "string", "2":
			return valueToString(v)
		case "objectId", "7":
			s, ok := v.(string)
			if !ok {
				return nil
			}
			id, err := bson.ObjectIDFromHex(s)
			if err != nil {
				if onErrorExpr != nil {
					return evalExpr(doc, onErrorExpr)
				}
				return nil
			}
			return id
		}
		return nil

	case "$mergeObjects":
		arr := evalExprArray(doc, args)
		result := bson.D{}
		for _, item := range arr {
			if d, ok := item.(bson.D); ok {
				for _, e := range d {
					result = SetField(result, e.Key, e.Value)
				}
			}
		}
		return result
	}

	return nil
}

// evalExprArray evaluates args as an array expression and returns a slice of evaluated values.
func evalExprArray(doc bson.D, args interface{}) []interface{} {
	switch v := args.(type) {
	case bson.A:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = evalExpr(doc, item)
		}
		return result
	default:
		// Single argument
		return []interface{}{evalExpr(doc, args)}
	}
}

// evalTrim handles $trim, $ltrim, $rtrim.
func evalTrim(doc bson.D, args interface{}, left, right bool) interface{} {
	var inputExpr interface{}
	var chars string
	hasChars := false

	switch v := args.(type) {
	case bson.D:
		for _, e := range v {
			switch e.Key {
			case "input":
				inputExpr = e.Value
			case "chars":
				cv := evalExpr(doc, e.Value)
				if s, ok := cv.(string); ok {
					chars = s
					hasChars = true
				}
			}
		}
	default:
		inputExpr = args
	}

	iv := evalExpr(doc, inputExpr)
	s, ok := iv.(string)
	if !ok {
		return ""
	}

	if !hasChars {
		if left && right {
			return strings.TrimSpace(s)
		} else if left {
			return strings.TrimLeftFunc(s, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' })
		} else {
			return strings.TrimRightFunc(s, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' })
		}
	}

	cutset := chars
	if left && right {
		return strings.Trim(s, cutset)
	} else if left {
		return strings.TrimLeft(s, cutset)
	}
	return strings.TrimRight(s, cutset)
}

// isTruthy returns false for nil, false bool, and zero numbers; true otherwise.
func isTruthy(v interface{}) bool {
	if v == nil {
		return false
	}
	switch n := v.(type) {
	case bool:
		return n
	case int:
		return n != 0
	case int32:
		return n != 0
	case int64:
		return n != 0
	case float32:
		return n != 0
	case float64:
		return n != 0
	}
	return true
}

// isExplicitZero returns true if v is a numeric 0 or false bool.
func isExplicitZero(v interface{}) bool {
	switch n := v.(type) {
	case int:
		return n == 0
	case int32:
		return n == 0
	case int64:
		return n == 0
	case float64:
		return n == 0
	case bool:
		return !n
	}
	return false
}

// isExplicitOne returns true if v is a numeric 1 or true bool.
func isExplicitOne(v interface{}) bool {
	switch n := v.(type) {
	case int:
		return n == 1
	case int32:
		return n == 1
	case int64:
		return n == 1
	case float64:
		return n == 1
	case bool:
		return n
	}
	return false
}

// bsonTypeName returns the BSON type name string for a value.
func bsonTypeName(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch v.(type) {
	case float64, float32:
		return "double"
	case string:
		return "string"
	case bson.D:
		return "object"
	case bson.A:
		return "array"
	case bson.ObjectID:
		return "objectId"
	case bool:
		return "bool"
	case int32:
		return "int"
	case int64:
		return "long"
	case int:
		return "int"
	case bson.Decimal128:
		return "decimal"
	default:
		return "undefined"
	}
}

// valueToString converts a value to its string representation.
func valueToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch n := v.(type) {
	case string:
		return n
	case bool:
		if n {
			return "true"
		}
		return "false"
	case int:
		return strconv.Itoa(n)
	case int32:
		return strconv.FormatInt(int64(n), 10)
	case int64:
		return strconv.FormatInt(n, 10)
	case float32:
		return strconv.FormatFloat(float64(n), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(n, 'g', -1, 64)
	case bson.ObjectID:
		return n.Hex()
	default:
		return fmt.Sprintf("%v", v)
	}
}
