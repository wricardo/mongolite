package engine

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCheckUniqueIndex_NoViolation(t *testing.T) {
	existing := []bson.D{
		{{Key: "email", Value: "a@b.com"}},
	}
	indexes := []IndexSpec{
		{Name: "email_1", Keys: bson.D{{Key: "email", Value: int32(1)}}, Unique: true},
	}
	newDoc := bson.D{{Key: "email", Value: "c@d.com"}}
	if err := CheckUniqueIndex(existing, indexes, newDoc); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCheckUniqueIndex_Violation(t *testing.T) {
	existing := []bson.D{
		{{Key: "email", Value: "a@b.com"}},
	}
	indexes := []IndexSpec{
		{Name: "email_1", Keys: bson.D{{Key: "email", Value: int32(1)}}, Unique: true},
	}
	newDoc := bson.D{{Key: "email", Value: "a@b.com"}}
	err := CheckUniqueIndex(existing, indexes, newDoc)
	if err == nil {
		t.Fatal("expected duplicate key error")
	}
	dupErr, ok := err.(*DuplicateKeyError)
	if !ok {
		t.Fatalf("expected *DuplicateKeyError, got %T: %v", err, err)
	}
	if dupErr.Index != "email_1" {
		t.Fatalf("expected index name email_1, got %s", dupErr.Index)
	}
}

func TestCheckUniqueIndex_NonUniqueIgnored(t *testing.T) {
	existing := []bson.D{
		{{Key: "tag", Value: "go"}},
	}
	indexes := []IndexSpec{
		{Name: "tag_1", Keys: bson.D{{Key: "tag", Value: int32(1)}}, Unique: false},
	}
	newDoc := bson.D{{Key: "tag", Value: "go"}}
	if err := CheckUniqueIndex(existing, indexes, newDoc); err != nil {
		t.Fatalf("non-unique index should not raise error: %v", err)
	}
}

func TestCheckUniqueIndex_NoIndexes(t *testing.T) {
	existing := []bson.D{{{Key: "x", Value: 1}}}
	if err := CheckUniqueIndex(existing, nil, bson.D{{Key: "x", Value: 1}}); err != nil {
		t.Fatalf("no indexes: unexpected error: %v", err)
	}
}

func TestDefaultIndexName_Single(t *testing.T) {
	got := DefaultIndexName(bson.D{{Key: "email", Value: int32(1)}})
	if got != "email_1" {
		t.Fatalf("expected email_1, got %s", got)
	}
}

func TestDefaultIndexName_SingleDesc(t *testing.T) {
	got := DefaultIndexName(bson.D{{Key: "name", Value: int32(-1)}})
	if got != "name_-1" {
		t.Fatalf("expected name_-1, got %s", got)
	}
}

func TestDefaultIndexName_Compound(t *testing.T) {
	got := DefaultIndexName(bson.D{
		{Key: "a", Value: int32(1)},
		{Key: "b", Value: int32(-1)},
	})
	if got != "a_1_b_-1" {
		t.Fatalf("expected a_1_b_-1, got %s", got)
	}
}

func TestDuplicateKeyError_Error(t *testing.T) {
	err := &DuplicateKeyError{Index: "foo_1"}
	msg := err.Error()
	if msg == "" {
		t.Fatal("error message should not be empty")
	}
	if !strings.Contains(msg, "foo_1") {
		t.Fatalf("error message should contain index name, got: %s", msg)
	}
}
