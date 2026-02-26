package handler

import (
	"github.com/wricardo/mongolite/internal/proto"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() {
	Register("bulkWrite", cmdBulkWrite)
	Register("bulkwrite", cmdBulkWrite)
}

func cmdBulkWrite(h *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "bulkWrite requires a collection name"), nil
	}

	ops := getArrayField(cmd, "ops")
	if ops == nil {
		return errorResp(2, "BadValue", "bulkWrite requires ops array"), nil
	}

	var nInserted, nModified, nRemoved int32

	for _, op := range ops {
		opDoc, ok := op.(bson.D)
		if !ok || len(opDoc) == 0 {
			continue
		}
		opName := opDoc[0].Key
		opVal, ok := opDoc[0].Value.(bson.D)
		if !ok {
			continue
		}

		switch opName {
		case "insertOne":
			doc := getDocField(opVal, "document")
			if doc != nil {
				_, err := h.Engine.Insert(db, collName, []bson.D{doc})
				if err != nil {
					return nil, err
				}
				nInserted++
			}
		case "updateOne":
			filter := getDocField(opVal, "filter")
			update := getDocField(opVal, "update")
			upsert := getBoolField(opVal, "upsert", false)
			_, modified, _, err := h.Engine.Update(db, collName, filter, update, false, upsert)
			if err != nil {
				return nil, err
			}
			nModified += int32(modified)
		case "updateMany":
			filter := getDocField(opVal, "filter")
			update := getDocField(opVal, "update")
			upsert := getBoolField(opVal, "upsert", false)
			_, modified, _, err := h.Engine.Update(db, collName, filter, update, true, upsert)
			if err != nil {
				return nil, err
			}
			nModified += int32(modified)
		case "deleteOne":
			filter := getDocField(opVal, "filter")
			n, err := h.Engine.Delete(db, collName, filter, false)
			if err != nil {
				return nil, err
			}
			nRemoved += int32(n)
		case "deleteMany":
			filter := getDocField(opVal, "filter")
			n, err := h.Engine.Delete(db, collName, filter, true)
			if err != nil {
				return nil, err
			}
			nRemoved += int32(n)
		case "replaceOne":
			filter := getDocField(opVal, "filter")
			replacement := getDocField(opVal, "replacement")
			upsert := getBoolField(opVal, "upsert", false)
			_, modified, _, err := h.Engine.Update(db, collName, filter, replacement, false, upsert)
			if err != nil {
				return nil, err
			}
			nModified += int32(modified)
		}
	}

	return bson.D{
		{Key: "nInserted", Value: nInserted},
		{Key: "nModified", Value: nModified},
		{Key: "nRemoved", Value: nRemoved},
		{Key: "ok", Value: float64(1)},
	}, nil
}
