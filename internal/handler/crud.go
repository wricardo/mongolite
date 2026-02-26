package handler

import (
	"fmt"

	"github.com/wricardo/mongolite/internal/engine"
	"github.com/wricardo/mongolite/internal/proto"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() {
	Register("insert", cmdInsert)
	Register("find", cmdFind)
	Register("update", cmdUpdate)
	Register("delete", cmdDelete)
	Register("findAndModify", cmdFindAndModify)
	Register("findandmodify", cmdFindAndModify)
	Register("count", cmdCount)
	Register("getMore", cmdGetMore)
	Register("getmore", cmdGetMore)
}

func cmdInsert(h *Handler, db string, cmd bson.D, sections []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "insert requires a collection name"), nil
	}

	var docs []bson.D

	// Documents can come from the body or from Kind 1 sections
	if arr := getArrayField(cmd, "documents"); arr != nil {
		for _, item := range arr {
			if d, ok := item.(bson.D); ok {
				docs = append(docs, d)
			}
		}
	}

	// Also check Kind 1 sections with identifier "documents"
	for _, sec := range sections {
		if sec.Kind == proto.SectionDocSeq && sec.Identifier == "documents" {
			for _, raw := range sec.Documents {
				var d bson.D
				if err := bson.Unmarshal(raw, &d); err != nil {
					return nil, fmt.Errorf("unmarshal insert doc: %w", err)
				}
				docs = append(docs, d)
			}
		}
	}

	if len(docs) == 0 {
		return errorResp(2, "BadValue", "no documents to insert"), nil
	}

	ids, err := h.Engine.Insert(db, collName, docs)
	if err != nil {
		return nil, err
	}

	return bson.D{
		{Key: "n", Value: int32(len(ids))},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdFind(h *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "find requires a collection name"), nil
	}

	filter := getDocField(cmd, "filter")
	sort := getDocField(cmd, "sort")
	skip := getInt64Field(cmd, "skip")
	limit := getInt64Field(cmd, "limit")

	// Handle batchSize as limit if limit is 0
	if limit == 0 {
		if bs := getInt64Field(cmd, "batchSize"); bs > 0 {
			limit = bs
		}
	}

	// Handle singleBatch
	if getBoolField(cmd, "singleBatch", false) && limit == 0 {
		limit = 1
	}

	results, err := h.Engine.Find(db, collName, filter, sort, skip, limit)
	if err != nil {
		return nil, err
	}

	// Convert to bson.A
	batch := bson.A{}
	for _, doc := range results {
		batch = append(batch, doc)
	}

	ns := db + "." + collName
	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "firstBatch", Value: batch},
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: ns},
		}},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdUpdate(h *Handler, db string, cmd bson.D, sections []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "update requires a collection name"), nil
	}

	var updates bson.A
	if arr := getArrayField(cmd, "updates"); arr != nil {
		updates = arr
	}

	// Also check Kind 1 sections with identifier "updates"
	for _, sec := range sections {
		if sec.Kind == proto.SectionDocSeq && sec.Identifier == "updates" {
			for _, raw := range sec.Documents {
				var d bson.D
				if err := bson.Unmarshal(raw, &d); err != nil {
					return nil, fmt.Errorf("unmarshal update spec: %w", err)
				}
				updates = append(updates, d)
			}
		}
	}

	var totalMatched, totalModified int64
	var upsertedDocs bson.A

	for _, u := range updates {
		spec, ok := u.(bson.D)
		if !ok {
			continue
		}
		q := getDocField(spec, "q")
		upd := getDocField(spec, "u")
		multi := getBoolField(spec, "multi", false)
		upsert := getBoolField(spec, "upsert", false)

		matched, modified, upsertedID, err := h.Engine.Update(db, collName, q, upd, multi, upsert)
		if err != nil {
			if dke, ok := err.(*engine.DuplicateKeyError); ok {
				return errorResp(11000, "DuplicateKey", dke.Error()), nil
			}
			return nil, err
		}
		totalMatched += matched
		totalModified += modified
		if upsertedID != nil {
			upsertedDocs = append(upsertedDocs, bson.D{
				{Key: "index", Value: int32(0)},
				{Key: "_id", Value: upsertedID},
			})
		}
	}

	resp := bson.D{
		{Key: "n", Value: int32(totalMatched)},
		{Key: "nModified", Value: int32(totalModified)},
	}
	if len(upsertedDocs) > 0 {
		resp = append(resp, bson.E{Key: "upserted", Value: upsertedDocs})
	}
	resp = append(resp, bson.E{Key: "ok", Value: float64(1)})
	return resp, nil
}

func cmdDelete(h *Handler, db string, cmd bson.D, sections []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "delete requires a collection name"), nil
	}

	var deletes bson.A
	if arr := getArrayField(cmd, "deletes"); arr != nil {
		deletes = arr
	}

	// Also check Kind 1 sections with identifier "deletes"
	for _, sec := range sections {
		if sec.Kind == proto.SectionDocSeq && sec.Identifier == "deletes" {
			for _, raw := range sec.Documents {
				var d bson.D
				if err := bson.Unmarshal(raw, &d); err != nil {
					return nil, fmt.Errorf("unmarshal delete spec: %w", err)
				}
				deletes = append(deletes, d)
			}
		}
	}

	var totalDeleted int64
	for _, d := range deletes {
		spec, ok := d.(bson.D)
		if !ok {
			continue
		}
		q := getDocField(spec, "q")
		limitVal := getInt64Field(spec, "limit")
		multi := limitVal == 0 // limit=0 means delete all matching

		n, err := h.Engine.Delete(db, collName, q, multi)
		if err != nil {
			return nil, err
		}
		totalDeleted += n
	}

	return bson.D{
		{Key: "n", Value: int32(totalDeleted)},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdFindAndModify(h *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "findAndModify requires a collection name"), nil
	}

	query := getDocField(cmd, "query")
	sort := getDocField(cmd, "sort")
	update := getDocField(cmd, "update")
	remove := getBoolField(cmd, "remove", false)
	returnNew := getBoolField(cmd, "new", false)
	upsert := getBoolField(cmd, "upsert", false)

	result, err := h.Engine.FindAndModify(db, collName, query, sort, update, remove, returnNew, upsert)
	if err != nil {
		return nil, err
	}

	resp := bson.D{
		{Key: "ok", Value: float64(1)},
	}
	if result != nil {
		resp = append(resp, bson.E{Key: "value", Value: result})
	} else {
		resp = append(resp, bson.E{Key: "value", Value: nil})
	}
	resp = append(resp, bson.E{Key: "lastErrorObject", Value: bson.D{
		{Key: "n", Value: int32(1)},
		{Key: "updatedExisting", Value: result != nil && !remove},
	}})
	return resp, nil
}

func cmdCount(h *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "count requires a collection name"), nil
	}

	filter := getDocField(cmd, "query")
	if filter == nil {
		filter = getDocField(cmd, "filter")
	}

	n, err := h.Engine.Count(db, collName, filter)
	if err != nil {
		return nil, err
	}

	return bson.D{
		{Key: "n", Value: int32(n)},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdGetMore(_ *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	// We always return cursor ID 0 (all results in firstBatch), so getMore is a no-op.
	collName := getStringField(cmd, "collection")
	ns := db + "." + collName
	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "nextBatch", Value: bson.A{}},
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: ns},
		}},
		{Key: "ok", Value: float64(1)},
	}, nil
}
