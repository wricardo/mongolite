package engine

import (
	"fmt"

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
			current, err = projectDocs(current, spec)
			if err != nil {
				return nil, err
			}

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

func projectDocs(docs []bson.D, spec bson.D) ([]bson.D, error) {
	// Determine inclusion or exclusion mode
	isInclusion := false
	isExclusion := false
	for _, s := range spec {
		if s.Key == "_id" {
			continue
		}
		v := toInt64(s.Value)
		if v == 1 {
			isInclusion = true
		} else if v == 0 {
			isExclusion = true
		}
	}

	var result []bson.D
	for _, doc := range docs {
		var projected bson.D
		if isInclusion {
			// Include only specified fields (+ _id by default)
			includeID := true
			for _, s := range spec {
				if s.Key == "_id" && toInt64(s.Value) == 0 {
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
				if toInt64(s.Value) == 1 {
					if v, ok := GetField(doc, s.Key); ok {
						projected = append(projected, bson.E{Key: s.Key, Value: v})
					}
				}
			}
		} else if isExclusion {
			// Copy all fields except excluded ones
			excludeSet := make(map[string]bool)
			for _, s := range spec {
				if toInt64(s.Value) == 0 {
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
		key := resolveGroupID(doc, idExpr)
		keyStr := fmt.Sprintf("%v", key)
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

func resolveGroupID(doc bson.D, expr interface{}) interface{} {
	switch v := expr.(type) {
	case string:
		if len(v) > 0 && v[0] == '$' {
			val, _ := GetField(doc, v[1:])
			return val
		}
		return v
	case bson.D:
		// Compound _id: resolve each field
		result := bson.D{}
		for _, e := range v {
			result = append(result, bson.E{Key: e.Key, Value: resolveGroupID(doc, e.Value)})
		}
		return result
	default:
		return v
	}
}

func computeAccumulator(docs []bson.D, op string, field interface{}) interface{} {
	fieldPath := ""
	if s, ok := field.(string); ok && len(s) > 0 && s[0] == '$' {
		fieldPath = s[1:]
	}

	switch op {
	case "$sum":
		if fieldPath == "" {
			// $sum with a constant (e.g., {$sum: 1})
			return int64(len(docs)) * toInt64(field)
		}
		var sum float64
		allInt := true
		for _, doc := range docs {
			v, ok := GetField(doc, fieldPath)
			if !ok {
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
			v, ok := GetField(doc, fieldPath)
			if !ok {
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
			v, ok := GetField(doc, fieldPath)
			if !ok {
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
			v, ok := GetField(doc, fieldPath)
			if !ok {
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
		v, _ := GetField(docs[0], fieldPath)
		return v

	case "$last":
		if len(docs) == 0 {
			return nil
		}
		v, _ := GetField(docs[len(docs)-1], fieldPath)
		return v

	case "$push":
		var arr bson.A
		for _, doc := range docs {
			v, _ := GetField(doc, fieldPath)
			arr = append(arr, v)
		}
		return arr

	case "$addToSet":
		var arr bson.A
		for _, doc := range docs {
			v, _ := GetField(doc, fieldPath)
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
