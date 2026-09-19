package engine

import "go.mongodb.org/mongo-driver/v2/bson"

// CheckUniqueIndex checks if inserting the given document would violate a unique index.
func CheckUniqueIndex(docs []bson.D, indexes []IndexSpec, newDoc bson.D) error {
	for _, idx := range indexes {
		if !idx.Unique {
			continue
		}
		for _, existing := range docs {
			if indexKeysMatch(existing, newDoc, idx.Keys) {
				return &DuplicateKeyError{Index: idx.Name}
			}
		}
	}
	return nil
}

func indexKeysMatch(a, b bson.D, keys bson.D) bool {
	for _, k := range keys {
		va, _ := GetField(a, k.Key)
		vb, _ := GetField(b, k.Key)
		if !valuesEqual(va, vb) {
			return false
		}
	}
	return true
}

// DefaultIndexName generates an index name from keys.
func DefaultIndexName(keys bson.D) string {
	name := ""
	for i, k := range keys {
		if i > 0 {
			name += "_"
		}
		dir := toInt64(k.Value)
		name += k.Key + "_"
		if dir >= 0 {
			name += "1"
		} else {
			name += "-1"
		}
	}
	return name
}

type DuplicateKeyError struct {
	Index string
}

func (e *DuplicateKeyError) Error() string {
	return "E11000 duplicate key error collection, index: " + e.Index
}
