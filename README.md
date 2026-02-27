# mongolite

A lightweight MongoDB-compatible database that stores all data in a single JSON file. Think SQLite, but for MongoDB.

mongolite ships as one binary with two modes:

- **CLI** — read and write the data file directly, no server needed
- **Server** — implements the MongoDB wire protocol so any standard MongoDB client or driver can connect

## Why?

- **No MongoDB installation required.** A single Go binary is all you need.
- **Single-file storage.** All data lives in one JSON file. Human-readable, diffable, committable.
- **Zero dependencies at runtime.** Pure Go, no external services.
- **Drop-in for development.** Use the same driver code you'd use with a real MongoDB instance.

## Installation

```bash
go install github.com/wricardo/mongolite@latest

# Install the Claude Code skill (for AI agent workflows)
mongolite install-skill
```

Or with Make:

```bash
make install          # installs the binary
make install-skill    # installs binary + Claude Code skill
```

## Quick Start

### CLI (no server required)

```bash
mongolite --file mydata.json insert users --doc '{"name": "Alice", "age": 30}'
mongolite --file mydata.json find users --filter '{"age": {"$gt": 25}}'
```

### Server mode

```bash
# Start the server (default port 27017, data file mongolite.json)
mongolite serve

# With custom options
mongolite serve --port 27018 --file mydata.json
```

Connect with any MongoDB client:

```bash
# mongosh
mongosh mongodb://localhost:27017

# Go driver
client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017/?directConnection=true"))
```

## CLI Reference

```bash
mongolite [--file FILE] [--db DATABASE] <command> [flags]
```

Global flags:
- `--file FILE` — data file path (default: `mongolite.json`)
- `--db DATABASE` — database name (default: `test`)

### Commands

```bash
# Insert
mongolite --file mydata.json insert users --doc '{"name": "Alice", "age": 30}'
mongolite --file mydata.json insert-many users --docs '[{"name": "Bob"}, {"name": "Charlie"}]'

# Query
mongolite --file mydata.json find users
mongolite --file mydata.json find users --filter '{"age": {"$gt": 25}}' --sort '{"age": -1}' --limit 10
mongolite --file mydata.json count users --filter '{"status": "active"}'

# Update & Delete
mongolite --file mydata.json update users --filter '{"name": "Alice"}' --update '{"$set": {"age": 31}}'
mongolite --file mydata.json delete users --filter '{"name": "Bob"}'

# Aggregation
mongolite --file mydata.json aggregate users --pipeline '[{"$group": {"_id": "$city", "count": {"$sum": 1}}}]'

# Admin
mongolite --file mydata.json list-dbs
mongolite --file mydata.json list-collections

# Server
mongolite serve [--port PORT] [--file FILE]
```

### File Input

For complex JSON, write it to a file and use `--*-file` flags:

```bash
echo '{"age": {"$gt": 25}, "status": {"$in": ["active", "pending"]}}' > filter.json
mongolite --file mydata.json find users --filter-file filter.json
```

### Output

All output is newline-delimited JSON (ndjson), one document per line — pipe directly to `jq`:

```bash
mongolite --file mydata.json find users | jq '.name'
mongolite --file mydata.json find users --filter '{"age": {"$gt": 25}}' | jq -c '{name, age}'
```

## Supported Operations

### CRUD
- `insert` / `insertOne` / `insertMany`
- `find` (with filter, sort, skip, limit)
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

mongolite works well as a local state store for multi-step AI agent workflows. The CLI operates directly on the file — no server process needed. Shell scripts, Go, Python, and Node code can all share the same data file.

### Task state tracking

```bash
# Step 1: create a task
mongolite --file agent.json insert tasks --doc '{"task_id": "abc", "status": "pending", "step": 1, "vars": {"url": "https://example.com"}}'

# Step 2: read current state
mongolite --file agent.json find tasks --filter '{"task_id": "abc"}' | jq '.vars'

# Step 3: update after completing a step
mongolite --file agent.json update tasks --filter '{"task_id": "abc"}' --update '{"$set": {"step": 2, "status": "in_progress", "vars.result": "fetched"}}'
```

### Step queue / control flow

```bash
# Enqueue steps
mongolite --file agent.json insert-many steps --docs '[
  {"order": 1, "action": "fetch", "done": false},
  {"order": 2, "action": "parse", "done": false},
  {"order": 3, "action": "summarize", "done": false}
]'

# Get next pending step
mongolite --file agent.json find steps --filter '{"done": false}' --sort '{"order": 1}' --limit 1

# Mark done
mongolite --file agent.json update steps --filter '{"order": 1}' --update '{"$set": {"done": true, "result": "ok"}}'
```

### From Go code (via server)

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

## Architecture

mongolite has two access paths that share the same storage engine:

```
  CLI mode (direct file access)        Server mode (wire protocol)
  ─────────────────────────────        ───────────────────────────
  mongolite --file F <command>          MongoDB Client
             │                                  │
             │                         TCP Listener (internal/server)
             │                                  │
             │                         Wire Protocol Parser (internal/proto)
             │                           Reads OP_MSG / OP_QUERY frames
             │                                  │
             │                         Command Handler (internal/handler)
             │                           Dispatches insert/find/update/...
             └──────────────────────────────────┘
                                        │
                               Storage Engine (internal/engine)
                                 In-memory store, RWMutex,
                                 filter matching, update operators,
                                 aggregation pipeline
                                        │
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
- No cursor pagination — all results are returned in the first batch

## Building

```bash
make build          # build ./mongolite
make install        # go install
make install-skill  # install binary + Claude Code skill
make test           # run tests
```

Or directly:

```bash
go build -o mongolite .
```

## License

MIT
