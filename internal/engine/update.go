package engine

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// ApplyUpdate applies update operators to a document.
func ApplyUpdate(doc bson.D, update bson.D) (bson.D, error) {
	// Check if this is a replacement document (no $ operators)
	if len(update) > 0 && update[0].Key != "" && update[0].Key[0] != '$' {
		// Replacement: keep _id from original, replace everything else
		id, hasID := GetField(doc, "_id")
		result := make(bson.D, len(update))
		copy(result, update)
		if hasID {
			// Ensure _id is preserved
			result = SetField(result, "_id", id)
		}
		return result, nil
	}

	var err error
	for _, op := range update {
		fields, ok := op.Value.(bson.D)
		if !ok {
			return nil, fmt.Errorf("update operator %s requires a document", op.Key)
		}
		switch op.Key {
		case "$set":
			for _, f := range fields {
				doc = SetField(doc, f.Key, f.Value)
			}
		case "$unset":
			for _, f := range fields {
				doc = UnsetField(doc, f.Key)
				_ = f.Value // value is ignored for $unset
			}
		case "$inc":
			for _, f := range fields {
				doc, err = incField(doc, f.Key, f.Value)
				if err != nil {
					return nil, err
				}
			}
		case "$mul":
			for _, f := range fields {
				doc, err = mulField(doc, f.Key, f.Value)
				if err != nil {
					return nil, err
				}
			}
		case "$min":
			for _, f := range fields {
				current, exists := GetField(doc, f.Key)
				if !exists || compareValues(f.Value, current) < 0 {
					doc = SetField(doc, f.Key, f.Value)
				}
			}
		case "$max":
			for _, f := range fields {
				current, exists := GetField(doc, f.Key)
				if !exists || compareValues(f.Value, current) > 0 {
					doc = SetField(doc, f.Key, f.Value)
				}
			}
		case "$rename":
			for _, f := range fields {
				newName, ok := f.Value.(string)
				if !ok {
					return nil, fmt.Errorf("$rename target must be a string")
				}
				val, exists := GetField(doc, f.Key)
				if exists {
					doc = UnsetField(doc, f.Key)
					doc = SetField(doc, newName, val)
				}
			}
		case "$push":
			for _, f := range fields {
				doc, err = pushField(doc, f.Key, f.Value)
				if err != nil {
					return nil, err
				}
			}
		case "$pull":
			for _, f := range fields {
				doc, err = pullField(doc, f.Key, f.Value)
				if err != nil {
					return nil, err
				}
			}
		case "$addToSet":
			for _, f := range fields {
				doc, err = addToSetField(doc, f.Key, f.Value)
				if err != nil {
					return nil, err
				}
			}
		case "$currentDate":
			for _, f := range fields {
				doc = SetField(doc, f.Key, bson.DateTime(time.Now().UnixMilli()))
			}
		default:
			return nil, fmt.Errorf("unsupported update operator: %s", op.Key)
		}
	}
	return doc, nil
}

func incField(doc bson.D, path string, val interface{}) (bson.D, error) {
	incVal := toFloat64(val)
	current, exists := GetField(doc, path)
	if !exists {
		return SetField(doc, path, val), nil
	}
	if !isNumeric(current) {
		return nil, fmt.Errorf("$inc: field %q is not numeric", path)
	}
	// Preserve int type if both are int
	if isInt(current) && isInt(val) {
		return SetField(doc, path, toInt64(current)+toInt64(val)), nil
	}
	return SetField(doc, path, toFloat64(current)+incVal), nil
}

func mulField(doc bson.D, path string, val interface{}) (bson.D, error) {
	mulVal := toFloat64(val)
	current, exists := GetField(doc, path)
	if !exists {
		// $mul on nonexistent field sets it to 0
		return SetField(doc, path, int64(0)), nil
	}
	if !isNumeric(current) {
		return nil, fmt.Errorf("$mul: field %q is not numeric", path)
	}
	if isInt(current) && isInt(val) {
		return SetField(doc, path, toInt64(current)*toInt64(val)), nil
	}
	return SetField(doc, path, toFloat64(current)*mulVal), nil
}

func pushField(doc bson.D, path string, val interface{}) (bson.D, error) {
	current, exists := GetField(doc, path)
	if !exists {
		return SetField(doc, path, bson.A{val}), nil
	}
	arr, ok := current.(bson.A)
	if !ok {
		return nil, fmt.Errorf("$push: field %q is not an array", path)
	}
	return SetField(doc, path, append(arr, val)), nil
}

func pullField(doc bson.D, path string, val interface{}) (bson.D, error) {
	current, exists := GetField(doc, path)
	if !exists {
		return doc, nil
	}
	arr, ok := current.(bson.A)
	if !ok {
		return nil, fmt.Errorf("$pull: field %q is not an array", path)
	}
	var newArr bson.A
	// If val is a document, it's a condition
	if cond, ok := val.(bson.D); ok {
		for _, elem := range arr {
			elemDoc, ok := elem.(bson.D)
			if ok && MatchDoc(elemDoc, cond) {
				continue
			}
			newArr = append(newArr, elem)
		}
	} else {
		for _, elem := range arr {
			if !valuesEqual(elem, val) {
				newArr = append(newArr, elem)
			}
		}
	}
	return SetField(doc, path, newArr), nil
}

func addToSetField(doc bson.D, path string, val interface{}) (bson.D, error) {
	current, exists := GetField(doc, path)
	if !exists {
		return SetField(doc, path, bson.A{val}), nil
	}
	arr, ok := current.(bson.A)
	if !ok {
		return nil, fmt.Errorf("$addToSet: field %q is not an array", path)
	}
	for _, elem := range arr {
		if valuesEqual(elem, val) {
			return doc, nil // already exists
		}
	}
	return SetField(doc, path, append(arr, val)), nil
}

func isInt(v interface{}) bool {
	switch v.(type) {
	case int, int32, int64:
		return true
	}
	return false
}
