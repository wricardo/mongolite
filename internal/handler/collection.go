package handler

import (
	"github.com/wricardo/mongolite/internal/proto"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() {
	Register("listCollections", cmdListCollections)
	Register("listcollections", cmdListCollections)
	Register("create", cmdCreateCollection)
	Register("drop", cmdDrop)
}

func cmdListCollections(h *Handler, db string, _ bson.D, _ []proto.Section) (bson.D, error) {
	names := h.Engine.ListCollections(db)

	var colls bson.A
	for _, name := range names {
		colls = append(colls, bson.D{
			{Key: "name", Value: name},
			{Key: "type", Value: "collection"},
			{Key: "options", Value: bson.D{}},
			{Key: "info", Value: bson.D{
				{Key: "readOnly", Value: false},
			}},
		})
	}
	if colls == nil {
		colls = bson.A{}
	}

	return bson.D{
		{Key: "cursor", Value: bson.D{
			{Key: "firstBatch", Value: colls},
			{Key: "id", Value: int64(0)},
			{Key: "ns", Value: db + ".$cmd.listCollections"},
		}},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdCreateCollection(h *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "create requires a collection name"), nil
	}

	if err := h.Engine.CreateCollection(db, collName); err != nil {
		return nil, err
	}
	return okResp(), nil
}

func cmdDrop(h *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "drop requires a collection name"), nil
	}

	if err := h.Engine.DropCollection(db, collName); err != nil {
		return nil, err
	}
	return bson.D{
		{Key: "nIndexesWas", Value: int32(1)},
		{Key: "ns", Value: db + "." + collName},
		{Key: "ok", Value: float64(1)},
	}, nil
}
