package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type Store struct {
	Databases map[string]*Database `bson:"databases" json:"databases"`
}

type Database struct {
	Collections map[string]*Collection `bson:"collections" json:"collections"`
}

type Collection struct {
	Documents []bson.D    `bson:"documents" json:"documents"`
	Indexes   []IndexSpec `bson:"indexes" json:"indexes"`
}

type IndexSpec struct {
	Name   string `bson:"name" json:"name"`
	Keys   bson.D `bson:"key" json:"key"`
	Unique bool   `bson:"unique" json:"unique"`
}

func NewStore() *Store {
	return &Store{Databases: make(map[string]*Database)}
}

func (s *Store) GetOrCreateDB(name string) *Database {
	db, ok := s.Databases[name]
	if !ok {
		db = &Database{Collections: make(map[string]*Collection)}
		s.Databases[name] = db
	}
	return db
}

func (db *Database) GetOrCreateColl(name string) *Collection {
	coll, ok := db.Collections[name]
	if !ok {
		coll = &Collection{}
		db.Collections[name] = coll
	}
	return coll
}

func LoadStore(path string) (*Store, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewStore(), nil
		}
		return nil, fmt.Errorf("read store file: %w", err)
	}
	if len(data) == 0 {
		return NewStore(), nil
	}
	var s Store
	if err := bson.UnmarshalExtJSON(data, false, &s); err != nil {
		return nil, fmt.Errorf("unmarshal store: %w", err)
	}
	if s.Databases == nil {
		s.Databases = make(map[string]*Database)
	}
	for _, db := range s.Databases {
		if db.Collections == nil {
			db.Collections = make(map[string]*Collection)
		}
	}
	return &s, nil
}

func SaveStore(path string, s *Store) error {
	// Sort documents by _id within each collection before saving,
	// and sort map keys (databases, collections) for deterministic output.
	sortStore(s)

	data, err := marshalStoreJSON(s)
	if err != nil {
		return fmt.Errorf("marshal store: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write tmp file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename tmp to store: %w", err)
	}
	return nil
}

// sortStore sorts documents by _id in each collection (in-place).
func sortStore(s *Store) {
	for _, db := range s.Databases {
		for _, coll := range db.Collections {
			sort.SliceStable(coll.Documents, func(i, j int) bool {
				idI := docIDSortKey(coll.Documents[i])
				idJ := docIDSortKey(coll.Documents[j])
				return idI < idJ
			})
		}
	}
}

// docIDSortKey returns a string that sorts correctly for the _id value.
// ObjectIDs sort by their hex representation (which is chronological since
// the first 4 bytes are a timestamp). Other types fall back to fmt.Sprintf.
func docIDSortKey(doc bson.D) string {
	for _, e := range doc {
		if e.Key == "_id" {
			switch v := e.Value.(type) {
			case bson.ObjectID:
				return v.Hex()
			case string:
				return v
			default:
				return fmt.Sprintf("%v", v)
			}
		}
	}
	return ""
}

// marshalStoreJSON produces deterministic JSON with sorted map keys and
// pretty-printed with 2-space indentation.
func marshalStoreJSON(s *Store) ([]byte, error) {
	// Build an ordered structure: databases sorted by name, collections sorted by name.
	dbNames := sortedKeys(s.Databases)

	ordered := make(map[string]interface{})
	dbs := make(map[string]interface{})
	for _, dbName := range dbNames {
		db := s.Databases[dbName]
		collNames := sortedKeys(db.Collections)

		colls := make(map[string]interface{})
		for _, collName := range collNames {
			coll := db.Collections[collName]
			// Marshal each document via bson.MarshalExtJSON for correct ObjectID/type handling
			docs := make([]json.RawMessage, len(coll.Documents))
			for i, doc := range coll.Documents {
				raw, err := bson.MarshalExtJSON(doc, false, false)
				if err != nil {
					return nil, fmt.Errorf("marshal doc: %w", err)
				}
				docs[i] = raw
			}
			var indexes interface{}
			if len(coll.Indexes) > 0 {
				idxRaw, err := bson.MarshalExtJSON(coll.Indexes, false, false)
				if err != nil {
					return nil, fmt.Errorf("marshal indexes: %w", err)
				}
				indexes = json.RawMessage(idxRaw)
			} else {
				indexes = []interface{}{}
			}
			colls[collName] = map[string]interface{}{
				"documents": docs,
				"indexes":   indexes,
			}
		}
		dbs[dbName] = map[string]interface{}{
			"collections": colls,
		}
	}
	ordered["databases"] = dbs

	data, err := json.MarshalIndent(ordered, "", "  ")
	if err != nil {
		return nil, err
	}
	// Ensure file ends with newline
	if !bytes.HasSuffix(data, []byte("\n")) {
		data = append(data, '\n')
	}
	return data, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
