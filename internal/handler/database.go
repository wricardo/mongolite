package handler

import (
	"github.com/wricardo/mongolite/internal/proto"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func init() {
	Register("listDatabases", cmdListDatabases)
	Register("listdatabases", cmdListDatabases)
	Register("dropDatabase", cmdDropDatabase)
	Register("dropdatabase", cmdDropDatabase)
}

func cmdListDatabases(h *Handler, _ string, _ bson.D, _ []proto.Section) (bson.D, error) {
	names := h.Engine.ListDatabases()

	var dbs bson.A
	for _, name := range names {
		dbs = append(dbs, bson.D{
			{Key: "name", Value: name},
			{Key: "sizeOnDisk", Value: int64(0)},
			{Key: "empty", Value: false},
		})
	}
	if dbs == nil {
		dbs = bson.A{}
	}

	return bson.D{
		{Key: "databases", Value: dbs},
		{Key: "totalSize", Value: int64(0)},
		{Key: "ok", Value: float64(1)},
	}, nil
}

func cmdDropDatabase(h *Handler, db string, _ bson.D, _ []proto.Section) (bson.D, error) {
	if err := h.Engine.DropDatabase(db); err != nil {
		return nil, err
	}
	return bson.D{
		{Key: "dropped", Value: db},
		{Key: "ok", Value: float64(1)},
	}, nil
}
