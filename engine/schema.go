package engine

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const schemaInternalDB = "_mongolite"
const schemaInternalColl = "schemas"

// ValidateDocAgainstSchema validates a bson.D against a JSON Schema (as raw JSON bytes).
// Converts the document to relaxed extended JSON, then validates.
func ValidateDocAgainstSchema(schemaJSON json.RawMessage, doc bson.D) error {
	docBytes, err := bson.MarshalExtJSON(doc, false, false)
	if err != nil {
		return fmt.Errorf("marshal doc for validation: %w", err)
	}

	var docVal interface{}
	if err := json.Unmarshal(docBytes, &docVal); err != nil {
		return fmt.Errorf("unmarshal doc for validation: %w", err)
	}

	const schemaURL = "http://mongolite/schema"
	c := jsonschema.NewCompiler()
	if err := c.AddResource(schemaURL, strings.NewReader(string(schemaJSON))); err != nil {
		return fmt.Errorf("add schema resource: %w", err)
	}
	sch, err := c.Compile(schemaURL)
	if err != nil {
		return fmt.Errorf("compile schema: %w", err)
	}

	if err := sch.Validate(docVal); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}
	return nil
}
