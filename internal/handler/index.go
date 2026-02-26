package handler

import (
	"github.com/wricardo/mongolite/internal/engine"
	"github.com/wricardo/mongolite/internal/proto"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() {
	Register("createIndexes", cmdCreateIndexes)
	Register("createindexes", cmdCreateIndexes)
	Register("listIndexes", cmdListIndexes)
	Register("listindexes", cmdListIndexes)
	Register("dropIndexes", cmdDropIndexes)
	Register("dropindexes", cmdDropIndexes)
}

func cmdCreateIndexes(h *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "createIndexes requires a collection name"), nil
	}

	indexArr := getArrayField(cmd, "indexes")
	if indexArr == nil {
		return errorResp(2, "BadValue", "createIndexes requires indexes array"), nil
	}

	var specs []engine.IndexSpec
	for _, item := range indexArr {
		specDoc, ok := item.(bson.D)
		if !ok {
			continue
		}
		var spec engine.IndexSpec
		for _, e := range specDoc {
			switch e.Key {
			case "key":
				if d, ok := e.Value.(bson.D); ok {
					spec.Keys = d
				}
			case "name":
				if s, ok := e.Value.(string); ok {
					spec.Name = s
				}
			case "unique":
				if b, ok := e.Value.(bool); ok {
					spec.Unique = b
				}
			}
		}
		specs = append(specs, spec)
	}

	if err := h.Engine.CreateIndexes(db, collName, specs); err != nil {
		return nil, err
	}

	return bson.D{
		{Key: "createdCollectionAutomatically", Value: false},
		{Key: "numIndexesBefore", Value: int32(1)},
		{Key: "numIndexesAfter", Value: int32(1 + int32(len(specs)))},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdListIndexes(h *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "listIndexes requires a collection name"), nil
	}

	indexes := h.Engine.ListIndexes(db, collName)
	batch := bson.A{}
	for _, idx := range indexes {
		batch = append(batch, bson.D{
			{Key: "v", Value: int32(2)},
			{Key: "key", Value: idx.Keys},
			{Key: "name", Value: idx.Name},
		})
	}

	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "firstBatch", Value: batch},
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: db + "." + collName},
		}},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdDropIndexes(h *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "dropIndexes requires a collection name"), nil
	}

	indexName := getStringField(cmd, "index")
	if indexName == "" {
		indexName = "*"
	}

	if err := h.Engine.DropIndexes(db, collName, indexName); err != nil {
		return nil, err
	}

	return okResp(), nil
}
