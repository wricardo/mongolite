package engine

import (
	"fmt"
	"os"

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
	data, err := bson.MarshalExtJSON(s, false, false)
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
