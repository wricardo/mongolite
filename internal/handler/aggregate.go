package handler

import (
	"github.com/wricardo/mongolite/internal/proto"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() {
	Register("aggregate", cmdAggregate)
}

func cmdAggregate(h *Handler, db string, cmd bson.D, _ []proto.Section) (bson.D, error) {
	collName, _ := cmd[0].Value.(string)
	if collName == "" {
		return errorResp(2, "BadValue", "aggregate requires a collection name"), nil
	}

	pipelineArr := getArrayField(cmd, "pipeline")
	if pipelineArr == nil {
		return errorResp(2, "BadValue", "aggregate requires pipeline array"), nil
	}

	var pipeline []bson.D
	for _, item := range pipelineArr {
		if d, ok := item.(bson.D); ok {
			pipeline = append(pipeline, d)
		}
	}

	results, err := h.Engine.Aggregate(db, collName, pipeline)
	if err != nil {
		return nil, err
	}

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
