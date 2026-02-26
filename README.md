# mongolite

A lightweight MongoDB-compatible server that stores all data in a single file. Think SQLite, but for MongoDB.

mongolite implements the MongoDB wire protocol so any standard MongoDB client or driver can connect to it directly — no configuration changes needed beyond the connection string.

## Why?

- **No MongoDB installation required.** Run `go run .` and you have a working MongoDB-compatible server.
- **Single-file storage.** All data lives in one JSON file. Human-readable, diffable, committable.
- **Zero dependencies at runtime.** Pure Go, no external services.
- **Drop-in replacement for development.** Use the same driver code you'd use with a real MongoDB instance.

## Quick Start

```bash
# Start the server (default port 27017, data file mongolite.json)
go run .

# Or with custom options
go run . --port 27018 --file mydata.json
```

Connect with any MongoDB client:

```bash
# mongosh
mongosh mongodb://localhost:27017

# Go driver
client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017/?directConnection=true"))
```

## CLI Client

A command-line client is included for quick queries without needing `mongosh`:

```bash
go run ./cmd/mongolite-cli [--host HOST] [--port PORT] [--db DATABASE] <command> [args]
```

### Commands

```bash
# Insert
mongolite-cli insert users --doc '{"name": "Alice", "age": 30}'
mongolite-cli insert-many users --docs '[{"name": "Bob"}, {"name": "Charlie"}]'

# Query
mongolite-cli find users
mongolite-cli find users --filter '{"age": {"$gt": 25}}' --sort '{"age": -1}' --limit 10
mongolite-cli count users --filter '{"status": "active"}'

# Update & Delete
mongolite-cli update users --filter '{"name": "Alice"}' --update '{"$set": {"age": 31}}'
mongolite-cli delete users --filter '{"name": "Bob"}'

# Aggregation
mongolite-cli aggregate users --pipeline '[{"$group": {"_id": "$city", "count": {"$sum": 1}}}]'

# Admin
mongolite-cli list-dbs
mongolite-cli list-collections
```

### File Input

For complex JSON, write it to a file and use `--*-file` flags:

```bash
echo '{"age": {"$gt": 25}, "status": {"$in": ["active", "pending"]}}' > filter.json
mongolite-cli find users --filter-file filter.json
```

### Output

All output is newline-delimited JSON (ndjson), one document per line — pipe directly to `jq`:

```bash
mongolite-cli find users | jq '.name'
mongolite-cli find users --filter '{"age": {"$gt": 25}}' | jq -c '{name, age}'
```

## Supported Operations

### CRUD
- `insert` / `insertOne` / `insertMany`
- `find` (with filter, sort, skip, limit, projection)
- `update` / `updateOne` / `updateMany` (with upsert)
- `delete` / `deleteOne` / `deleteMany`
- `findAndModify`
- `count`
- `bulkWrite`

### Query Operators
`$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` `$exists` `$type` `$and` `$or` `$nor` `$not` `$all` `$elemMatch` `$size`

### Update Operators
`$set` `$unset` `$inc` `$mul` `$min` `$max` `$rename` `$push` `$pull` `$addToSet` `$currentDate`

### Aggregation Pipeline Stages
`$match` `$project` `$sort` `$limit` `$skip` `$unwind` `$group` `$lookup` `$count`

### Aggregation Accumulators
`$sum` `$avg` `$min` `$max` `$first` `$last` `$push` `$addToSet`

### Admin
- `listDatabases` / `dropDatabase`
- `listCollections` / `create` / `drop`
- `createIndexes` / `listIndexes` / `dropIndexes`

### Wire Protocol
- OP_MSG (opcode 2013) — modern protocol used by current drivers
- OP_QUERY (opcode 2004) — legacy protocol for driver handshake compatibility
- OP_REPLY (opcode 1) — legacy reply format

## Use Case: Agent State Persistence

mongolite works well as a local state store for multi-step AI agent workflows. Any MongoDB driver connects to it, so you can persist task state, variables, and control flow between steps without running a real database.

### Task state tracking

```bash
# Step 1: create a task
mongolite-cli insert tasks --doc '{"task_id": "abc", "status": "pending", "step": 1, "vars": {"url": "https://example.com"}}'

# Step 2: read current state
mongolite-cli find tasks --filter '{"task_id": "abc"}' | jq '.vars'

# Step 3: update after completing a step
mongolite-cli update tasks --filter '{"task_id": "abc"}' --update '{"$set": {"step": 2, "status": "in_progress", "vars.result": "fetched"}}'
```

### Step queue / control flow

```bash
# Enqueue steps
mongolite-cli insert-many steps --docs '[
  {"order": 1, "action": "fetch", "done": false},
  {"order": 2, "action": "parse", "done": false},
  {"order": 3, "action": "summarize", "done": false}
]'

# Get next pending step
mongolite-cli find steps --filter '{"done": false}' --sort '{"order": 1}' --limit 1

# Mark done
mongolite-cli update steps --filter '{"order": 1}' --update '{"$set": {"done": true, "result": "ok"}}'
```

### From Go code

```go
client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017/?directConnection=true"))
coll := client.Database("agent").Collection("state")

// Upsert current state
coll.UpdateOne(ctx, bson.M{"task_id": taskID}, bson.M{
    "$set": bson.M{"step": 3, "vars.output": result},
}, options.UpdateOne().SetUpsert(true))

// Read state
var state TaskState
coll.FindOne(ctx, bson.M{"task_id": taskID}).Decode(&state)
```

The advantage over a plain JSON file: you get query/filter/update semantics instead of read-modify-write-entire-file. Shell scripts (via `mongolite-cli`) and Go/Python/Node code can all hit the same data store. The data file persists across restarts, so an agent can crash and resume from its last saved state.

## Architecture

```
MongoDB Client
      │
      ▼
  TCP Listener (internal/server)
      │
      ▼
  Wire Protocol Parser (internal/proto)
      │  Reads OP_MSG / OP_QUERY frames
      ▼
  Command Handler (internal/handler)
      │  Dispatches to insert/find/update/delete/aggregate/...
      ▼
  Storage Engine (internal/engine)
      │  In-memory store protected by RWMutex
      │  Filter matching, update operators, aggregation pipeline
      ▼
  JSON File (mongolite.json)
      Atomic writes via temp file + rename
```

- **Storage:** All data is held in memory and persisted to a single JSON file on every write. Writes are atomic (write to `.tmp`, then `os.Rename`). The file uses MongoDB Extended JSON format — human-readable and git-diffable.
- **Concurrency:** A `sync.RWMutex` protects the in-memory store. Multiple readers, single writer.
- **IDs:** Documents without an `_id` field get an auto-generated `ObjectID`.

## Limitations

This is a development tool, not a production database.

- No authentication or TLS
- No replication or sharding
- No change streams or transactions
- No capped collections or TTL indexes
- Entire dataset must fit in memory
- Single-file storage means writes are serialized

## Building

```bash
# Server
go build -o mongolite .

# CLI
go build -o mongolite-cli ./cmd/mongolite-cli
```

## License

MIT
