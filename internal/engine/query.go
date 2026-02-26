package engine

import (
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// MatchDoc checks if a document matches the given filter.
func MatchDoc(doc bson.D, filter bson.D) bool {
	if len(filter) == 0 {
		return true
	}
	for _, fe := range filter {
		key := fe.Key
		val := fe.Value

		switch key {
		case "$and":
			arr, ok := val.(bson.A)
			if !ok {
				return false
			}
			for _, sub := range arr {
				subDoc, ok := sub.(bson.D)
				if !ok {
					return false
				}
				if !MatchDoc(doc, subDoc) {
					return false
				}
			}
		case "$or":
			arr, ok := val.(bson.A)
			if !ok {
				return false
			}
			matched := false
			for _, sub := range arr {
				subDoc, ok := sub.(bson.D)
				if !ok {
					continue
				}
				if MatchDoc(doc, subDoc) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		case "$nor":
			arr, ok := val.(bson.A)
			if !ok {
				return false
			}
			for _, sub := range arr {
				subDoc, ok := sub.(bson.D)
				if !ok {
					continue
				}
				if MatchDoc(doc, subDoc) {
					return false
				}
			}
		case "$not":
			subDoc, ok := val.(bson.D)
			if !ok {
				return false
			}
			if MatchDoc(doc, subDoc) {
				return false
			}
		default:
			docVal, exists := lookupField(doc, key)
			if !matchFieldValue(docVal, exists, val) {
				return false
			}
		}
	}
	return true
}

// lookupField resolves a dotted field path in a document.
func lookupField(doc bson.D, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	var current interface{} = doc

	for _, part := range parts {
		switch v := current.(type) {
		case bson.D:
			found := false
			for _, elem := range v {
				if elem.Key == part {
					current = elem.Value
					found = true
					break
				}
			}
			if !found {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return current, true
}

// matchFieldValue matches a document field value against a filter value.
// If the filter value is a bson.D with operator keys ($gt, $eq, etc.), apply operators.
// Otherwise, do equality comparison.
func matchFieldValue(docVal interface{}, exists bool, filterVal interface{}) bool {
	// Check if filterVal is an operator document
	if opDoc, ok := filterVal.(bson.D); ok && len(opDoc) > 0 && strings.HasPrefix(opDoc[0].Key, "$") {
		return matchOperators(docVal, exists, opDoc)
	}
	// Direct equality
	if !exists {
		return filterVal == nil
	}
	return valuesEqual(docVal, filterVal)
}

func matchOperators(docVal interface{}, exists bool, ops bson.D) bool {
	for _, op := range ops {
		if !applyOperator(docVal, exists, op.Key, op.Value) {
			return false
		}
	}
	return true
}

func applyOperator(docVal interface{}, exists bool, op string, opVal interface{}) bool {
	switch op {
	case "$eq":
		return exists && valuesEqual(docVal, opVal)
	case "$ne":
		return !exists || !valuesEqual(docVal, opVal)
	case "$gt":
		return exists && compareValues(docVal, opVal) > 0
	case "$gte":
		return exists && compareValues(docVal, opVal) >= 0
	case "$lt":
		return exists && compareValues(docVal, opVal) < 0
	case "$lte":
		return exists && compareValues(docVal, opVal) <= 0
	case "$in":
		arr, ok := opVal.(bson.A)
		if !ok {
			return false
		}
		if !exists {
			return false
		}
		for _, v := range arr {
			if valuesEqual(docVal, v) {
				return true
			}
		}
		return false
	case "$nin":
		arr, ok := opVal.(bson.A)
		if !ok {
			return true
		}
		if !exists {
			return true
		}
		for _, v := range arr {
			if valuesEqual(docVal, v) {
				return false
			}
		}
		return true
	case "$exists":
		want, ok := opVal.(bool)
		if !ok {
			return false
		}
		return exists == want
	case "$type":
		if !exists {
			return false
		}
		return matchType(docVal, opVal)
	case "$all":
		arr, ok := opVal.(bson.A)
		if !ok {
			return false
		}
		docArr, ok := docVal.(bson.A)
		if !ok {
			return false
		}
		for _, needed := range arr {
			found := false
			for _, have := range docArr {
				if valuesEqual(have, needed) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	case "$size":
		docArr, ok := docVal.(bson.A)
		if !ok {
			return false
		}
		size := toInt64(opVal)
		return int64(len(docArr)) == size
	case "$elemMatch":
		subFilter, ok := opVal.(bson.D)
		if !ok {
			return false
		}
		docArr, ok := docVal.(bson.A)
		if !ok {
			return false
		}
		for _, elem := range docArr {
			elemDoc, ok := elem.(bson.D)
			if !ok {
				continue
			}
			if MatchDoc(elemDoc, subFilter) {
				return true
			}
		}
		return false
	case "$not":
		subOps, ok := opVal.(bson.D)
		if !ok {
			return false
		}
		return !matchOperators(docVal, exists, subOps)
	default:
		return false
	}
}

func matchType(val interface{}, typeVal interface{}) bool {
	typeName, ok := typeVal.(string)
	if !ok {
		return false
	}
	switch typeName {
	case "string":
		_, ok := val.(string)
		return ok
	case "int":
		return isNumeric(val)
	case "double":
		_, ok := val.(float64)
		return ok
	case "bool":
		_, ok := val.(bool)
		return ok
	case "object":
		_, ok := val.(bson.D)
		return ok
	case "array":
		_, ok := val.(bson.A)
		return ok
	case "null":
		return val == nil
	default:
		return false
	}
}

func valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Numeric comparison: normalize to float64 or int64
	if isNumeric(a) && isNumeric(b) {
		return compareValues(a, b) == 0
	}
	return reflect.DeepEqual(a, b)
}

// compareValues returns -1, 0, or 1. For non-comparable types, returns 0.
func compareValues(a, b interface{}) int {
	// Handle numeric types
	if isNumeric(a) && isNumeric(b) {
		af := toFloat64(a)
		bf := toFloat64(b)
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}
	// String comparison
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			if as < bs {
				return -1
			}
			if as > bs {
				return 1
			}
			return 0
		}
	}
	// Bool comparison
	if ab, ok := a.(bool); ok {
		if bb, ok := b.(bool); ok {
			if ab == bb {
				return 0
			}
			if !ab {
				return -1
			}
			return 1
		}
	}
	// ObjectID comparison
	if ao, ok := a.(bson.ObjectID); ok {
		if bo, ok := b.(bson.ObjectID); ok {
			for i := 0; i < 12; i++ {
				if ao[i] < bo[i] {
					return -1
				}
				if ao[i] > bo[i] {
					return 1
				}
			}
			return 0
		}
	}
	return 0
}

func isNumeric(v interface{}) bool {
	switch v.(type) {
	case int, int32, int64, float32, float64:
		return true
	}
	return false
}

func toFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	}
	return 0
}

func toInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return 0
}

// SortDocs sorts documents by the given sort specification.
func SortDocs(docs []bson.D, sortSpec bson.D) {
	if len(sortSpec) == 0 {
		return
	}
	// Use stable sort to preserve insertion order for equal elements
	n := len(docs)
	for i := 1; i < n; i++ {
		for j := i; j > 0; j-- {
			if compareDocs(docs[j-1], docs[j], sortSpec) > 0 {
				docs[j-1], docs[j] = docs[j], docs[j-1]
			} else {
				break
			}
		}
	}
}

func compareDocs(a, b bson.D, sortSpec bson.D) int {
	for _, s := range sortSpec {
		aVal, _ := lookupField(a, s.Key)
		bVal, _ := lookupField(b, s.Key)
		cmp := compareValues(aVal, bVal)
		if cmp == 0 {
			continue
		}
		dir := toInt64(s.Value)
		if dir < 0 {
			return -cmp
		}
		return cmp
	}
	return 0
}

// FilterDocs returns documents matching the filter.
func FilterDocs(docs []bson.D, filter bson.D) []bson.D {
	if len(filter) == 0 {
		result := make([]bson.D, len(docs))
		copy(result, docs)
		return result
	}
	var result []bson.D
	for _, doc := range docs {
		if MatchDoc(doc, filter) {
			result = append(result, doc)
		}
	}
	return result
}

// SetField sets a field in a document, creating intermediate docs for dotted paths.
func SetField(doc bson.D, path string, val interface{}) bson.D {
	parts := strings.Split(path, ".")
	if len(parts) == 1 {
		for i, e := range doc {
			if e.Key == path {
				doc[i].Value = val
				return doc
			}
		}
		return append(doc, bson.E{Key: path, Value: val})
	}
	// Nested path: find or create the intermediate document
	first := parts[0]
	rest := strings.Join(parts[1:], ".")
	for i, e := range doc {
		if e.Key == first {
			sub, ok := e.Value.(bson.D)
			if !ok {
				sub = bson.D{}
			}
			doc[i].Value = SetField(sub, rest, val)
			return doc
		}
	}
	sub := SetField(bson.D{}, rest, val)
	return append(doc, bson.E{Key: first, Value: sub})
}

// UnsetField removes a field from a document.
func UnsetField(doc bson.D, path string) bson.D {
	parts := strings.Split(path, ".")
	if len(parts) == 1 {
		for i, e := range doc {
			if e.Key == path {
				return append(doc[:i], doc[i+1:]...)
			}
		}
		return doc
	}
	first := parts[0]
	rest := strings.Join(parts[1:], ".")
	for i, e := range doc {
		if e.Key == first {
			sub, ok := e.Value.(bson.D)
			if !ok {
				return doc
			}
			doc[i].Value = UnsetField(sub, rest)
			return doc
		}
	}
	return doc
}

// GetField retrieves a field value from a bson.D, same as lookupField but exported.
func GetField(doc bson.D, path string) (interface{}, bool) {
	return lookupField(doc, path)
}

// CopyDoc creates a deep copy of a bson.D by marshaling and unmarshaling.
func CopyDoc(doc bson.D) (bson.D, error) {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal for copy: %w", err)
	}
	var out bson.D
	if err := bson.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("unmarshal for copy: %w", err)
	}
	return out, nil
}
