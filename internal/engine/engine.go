package engine

import (
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type Engine struct {
	mu       sync.RWMutex
	data     *Store
	filePath string
}

func New(filePath string) (*Engine, error) {
	store, err := LoadStore(filePath)
	if err != nil {
		return nil, fmt.Errorf("load store: %w", err)
	}
	return &Engine{data: store, filePath: filePath}, nil
}

func (e *Engine) save() error {
	return SaveStore(e.filePath, e.data)
}

// Insert adds documents to a collection. Returns the generated _id values.
func (e *Engine) Insert(db, coll string, docs []bson.D) ([]interface{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	c := e.data.GetOrCreateDB(db).GetOrCreateColl(coll)
	var ids []interface{}

	for _, doc := range docs {
		doc = ensureID(doc)
		id, _ := GetField(doc, "_id")
		ids = append(ids, id)

		if err := CheckUniqueIndex(c.Documents, c.Indexes, doc); err != nil {
			return nil, err
		}
		c.Documents = append(c.Documents, doc)
	}

	if err := e.save(); err != nil {
		return nil, err
	}
	return ids, nil
}

// Find queries documents in a collection.
func (e *Engine) Find(db, coll string, filter bson.D, sort bson.D, skip, limit int64) ([]bson.D, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	d := e.data.Databases[db]
	if d == nil {
		return nil, nil
	}
	c := d.Collections[coll]
	if c == nil {
		return nil, nil
	}

	results := FilterDocs(c.Documents, filter)

	if len(sort) > 0 {
		SortDocs(results, sort)
	}

	if skip > 0 {
		if int(skip) >= len(results) {
			return nil, nil
		}
		results = results[skip:]
	}

	if limit > 0 && int(limit) < len(results) {
		results = results[:limit]
	}

	return results, nil
}

// Update modifies documents. Returns (matchedCount, modifiedCount, upsertedID, error).
func (e *Engine) Update(db, coll string, filter, update bson.D, multi, upsert bool) (int64, int64, interface{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	c := e.data.GetOrCreateDB(db).GetOrCreateColl(coll)
	var matched, modified int64

	for i, doc := range c.Documents {
		if !MatchDoc(doc, filter) {
			continue
		}
		matched++
		updated, err := ApplyUpdate(doc, update)
		if err != nil {
			return matched, modified, nil, err
		}
		c.Documents[i] = updated
		modified++
		if !multi {
			break
		}
	}

	// Upsert: insert if nothing matched
	var upsertedID interface{}
	if matched == 0 && upsert {
		newDoc := bson.D{}
		// Apply filter fields as initial values
		for _, f := range filter {
			if f.Key[0] != '$' {
				newDoc = SetField(newDoc, f.Key, f.Value)
			}
		}
		var err error
		newDoc, err = ApplyUpdate(newDoc, update)
		if err != nil {
			return 0, 0, nil, err
		}
		newDoc = ensureID(newDoc)
		upsertedID, _ = GetField(newDoc, "_id")
		c.Documents = append(c.Documents, newDoc)
	}

	if matched > 0 || upsertedID != nil {
		if err := e.save(); err != nil {
			return matched, modified, upsertedID, err
		}
	}
	return matched, modified, upsertedID, nil
}

// Delete removes documents. Returns the number deleted.
func (e *Engine) Delete(db, coll string, filter bson.D, multi bool) (int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	d := e.data.Databases[db]
	if d == nil {
		return 0, nil
	}
	c := d.Collections[coll]
	if c == nil {
		return 0, nil
	}

	var kept []bson.D
	var deleted int64
	for i, doc := range c.Documents {
		if MatchDoc(doc, filter) {
			deleted++
			if !multi {
				// Keep remaining docs after this one
				kept = append(kept, c.Documents[i+1:]...)
				break
			}
		} else {
			kept = append(kept, doc)
		}
	}

	if deleted > 0 {
		c.Documents = kept
		if err := e.save(); err != nil {
			return deleted, err
		}
	}
	return deleted, nil
}

// Count returns the number of matching documents.
func (e *Engine) Count(db, coll string, filter bson.D) (int64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	d := e.data.Databases[db]
	if d == nil {
		return 0, nil
	}
	c := d.Collections[coll]
	if c == nil {
		return 0, nil
	}

	if len(filter) == 0 {
		return int64(len(c.Documents)), nil
	}
	var count int64
	for _, doc := range c.Documents {
		if MatchDoc(doc, filter) {
			count++
		}
	}
	return count, nil
}

// FindAndModify finds a single document and modifies or removes it.
func (e *Engine) FindAndModify(db, coll string, filter bson.D, sort bson.D, update bson.D, remove bool, returnNew bool, upsert bool) (bson.D, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	c := e.data.GetOrCreateDB(db).GetOrCreateColl(coll)

	// Find matching documents
	matches := FilterDocs(c.Documents, filter)
	if len(sort) > 0 {
		SortDocs(matches, sort)
	}

	if len(matches) == 0 {
		if !upsert || remove {
			return nil, nil
		}
		// Upsert
		newDoc := bson.D{}
		for _, f := range filter {
			if f.Key[0] != '$' {
				newDoc = SetField(newDoc, f.Key, f.Value)
			}
		}
		var err error
		newDoc, err = ApplyUpdate(newDoc, update)
		if err != nil {
			return nil, err
		}
		newDoc = ensureID(newDoc)
		c.Documents = append(c.Documents, newDoc)
		if err := e.save(); err != nil {
			return nil, err
		}
		return newDoc, nil
	}

	target := matches[0]
	// Find the actual index in the collection
	for i, doc := range c.Documents {
		targetID, _ := GetField(target, "_id")
		docID, _ := GetField(doc, "_id")
		if valuesEqual(targetID, docID) {
			if remove {
				preDoc := c.Documents[i]
				c.Documents = append(c.Documents[:i], c.Documents[i+1:]...)
				if err := e.save(); err != nil {
					return nil, err
				}
				return preDoc, nil
			}
			preDoc, err := CopyDoc(c.Documents[i])
			if err != nil {
				return nil, err
			}
			updated, err := ApplyUpdate(c.Documents[i], update)
			if err != nil {
				return nil, err
			}
			c.Documents[i] = updated
			if err := e.save(); err != nil {
				return nil, err
			}
			if returnNew {
				return updated, nil
			}
			return preDoc, nil
		}
	}
	return nil, nil
}

// Aggregate runs an aggregation pipeline.
func (e *Engine) Aggregate(db, coll string, pipeline []bson.D) ([]bson.D, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	d := e.data.Databases[db]
	if d == nil {
		return nil, nil
	}
	c := d.Collections[coll]
	if c == nil {
		return nil, nil
	}

	// Copy docs to avoid mutations
	docs := make([]bson.D, len(c.Documents))
	copy(docs, c.Documents)

	lookupFn := func(_, lookupColl string, filter bson.D) ([]bson.D, error) {
		lc := d.Collections[lookupColl]
		if lc == nil {
			return nil, nil
		}
		return FilterDocs(lc.Documents, filter), nil
	}

	return RunPipeline(docs, pipeline, lookupFn)
}

// ListDatabases returns all database names.
func (e *Engine) ListDatabases() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var names []string
	for name := range e.data.Databases {
		names = append(names, name)
	}
	return names
}

// DropDatabase removes a database.
func (e *Engine) DropDatabase(db string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.data.Databases, db)
	return e.save()
}

// ListCollections returns collection names for a database.
func (e *Engine) ListCollections(db string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	d := e.data.Databases[db]
	if d == nil {
		return nil
	}
	var names []string
	for name := range d.Collections {
		names = append(names, name)
	}
	return names
}

// CreateCollection creates an empty collection.
func (e *Engine) CreateCollection(db, coll string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.data.GetOrCreateDB(db).GetOrCreateColl(coll)
	return e.save()
}

// DropCollection removes a collection.
func (e *Engine) DropCollection(db, coll string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	d := e.data.Databases[db]
	if d == nil {
		return nil
	}
	delete(d.Collections, coll)
	return e.save()
}

// CreateIndexes adds index specifications to a collection.
func (e *Engine) CreateIndexes(db, coll string, specs []IndexSpec) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	c := e.data.GetOrCreateDB(db).GetOrCreateColl(coll)
	for _, spec := range specs {
		if spec.Name == "" {
			spec.Name = DefaultIndexName(spec.Keys)
		}
		// Check for duplicate index name
		found := false
		for _, existing := range c.Indexes {
			if existing.Name == spec.Name {
				found = true
				break
			}
		}
		if !found {
			c.Indexes = append(c.Indexes, spec)
		}
	}
	return e.save()
}

// ListIndexes returns indexes for a collection.
func (e *Engine) ListIndexes(db, coll string) []IndexSpec {
	e.mu.RLock()
	defer e.mu.RUnlock()

	d := e.data.Databases[db]
	if d == nil {
		return nil
	}
	c := d.Collections[coll]
	if c == nil {
		return nil
	}
	// Always include the default _id index
	result := []IndexSpec{{Name: "_id_", Keys: bson.D{{Key: "_id", Value: int32(1)}}}}
	result = append(result, c.Indexes...)
	return result
}

// DropIndexes removes an index by name. Use "*" to drop all non-_id indexes.
func (e *Engine) DropIndexes(db, coll string, name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	d := e.data.Databases[db]
	if d == nil {
		return nil
	}
	c := d.Collections[coll]
	if c == nil {
		return nil
	}

	if name == "*" {
		c.Indexes = nil
		return e.save()
	}

	for i, idx := range c.Indexes {
		if idx.Name == name {
			c.Indexes = append(c.Indexes[:i], c.Indexes[i+1:]...)
			return e.save()
		}
	}
	return nil
}

// ensureID adds an _id field if missing.
func ensureID(doc bson.D) bson.D {
	for _, e := range doc {
		if e.Key == "_id" {
			return doc
		}
	}
	id := bson.NewObjectID()
	return append(bson.D{bson.E{Key: "_id", Value: id}}, doc...)
}
