package handler

import (
	"fmt"
	"strings"

	"github.com/wricardo/mongolite/internal/engine"
	"github.com/wricardo/mongolite/internal/proto"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type Handler struct {
	Engine *engine.Engine
}

type CommandFunc func(h *Handler, db string, cmd bson.D, sections []proto.Section) (bson.D, error)

var commands = map[string]CommandFunc{}

func Register(name string, fn CommandFunc) {
	commands[strings.ToLower(name)] = fn
}

func New(e *engine.Engine) *Handler {
	return &Handler{Engine: e}
}

// Handle dispatches a command from an OP_MSG body.
func (h *Handler) Handle(body bson.Raw, extraSections []proto.Section) (bson.D, error) {
	var cmd bson.D
	if err := bson.Unmarshal(body, &cmd); err != nil {
		return errorResp(2, "BadValue", "failed to parse command: "+err.Error()), nil
	}

	if len(cmd) == 0 {
		return errorResp(2, "BadValue", "empty command"), nil
	}

	// First key is the command name
	cmdName := cmd[0].Key

	// Extract $db
	db := "test"
	for _, e := range cmd {
		if e.Key == "$db" {
			if s, ok := e.Value.(string); ok {
				db = s
			}
		}
	}

	fn, ok := commands[strings.ToLower(cmdName)]
	if !ok {
		return errorResp(59, "CommandNotFound", fmt.Sprintf("no such command: '%s'", cmdName)), nil
	}

	resp, err := fn(h, db, cmd, extraSections)
	if err != nil {
		// Check for duplicate key error
		if dke, ok := err.(*engine.DuplicateKeyError); ok {
			return errorResp(11000, "DuplicateKey", dke.Error()), nil
		}
		return errorResp(2, "BadValue", err.Error()), nil
	}
	return resp, nil
}

func errorResp(code int32, codeName, msg string) bson.D {
	return bson.D{
		{Key: "ok", Value: float64(0)},
		{Key: "errmsg", Value: msg},
		{Key: "code", Value: code},
		{Key: "codeName", Value: codeName},
	}
}

func okResp() bson.D {
	return bson.D{{Key: "ok", Value: float64(1)}}
}

// getDocField extracts a typed field from a bson.D command.
func getStringField(cmd bson.D, key string) string {
	for _, e := range cmd {
		if e.Key == key {
			if s, ok := e.Value.(string); ok {
				return s
			}
		}
	}
	return ""
}

func getDocField(cmd bson.D, key string) bson.D {
	for _, e := range cmd {
		if e.Key == key {
			if d, ok := e.Value.(bson.D); ok {
				return d
			}
		}
	}
	return nil
}

func getArrayField(cmd bson.D, key string) bson.A {
	for _, e := range cmd {
		if e.Key == key {
			if a, ok := e.Value.(bson.A); ok {
				return a
			}
		}
	}
	return nil
}

func getInt64Field(cmd bson.D, key string) int64 {
	for _, e := range cmd {
		if e.Key == key {
			switch v := e.Value.(type) {
			case int32:
				return int64(v)
			case int64:
				return v
			case float64:
				return int64(v)
			case int:
				return int64(v)
			}
		}
	}
	return 0
}

func getBoolField(cmd bson.D, key string, def bool) bool {
	for _, e := range cmd {
		if e.Key == key {
			if b, ok := e.Value.(bool); ok {
				return b
			}
		}
	}
	return def
}
