# mongolite

Persistent state for AI agents — a single-binary, single-file database that speaks MongoDB's query language. No server to provision, no cluster to manage.

> mongolite is not trying to reproduce 100% of MongoDB. The goal is to mimic the most common commands, operators, and data model conventions so AI agents and CLI workflows can read/write structured state without provisioning a real cluster.

mongolite ships as one binary with two modes:

- **CLI** — read and write the data file directly, no server needed
- **Server** — implements the MongoDB wire protocol so any standard MongoDB client or driver can connect

## Why?

- **Agent-friendly CLI.** Designed for AI workflows that need to persist task state, deduplicate work, checkpoint progress, or maintain a step queue — without spinning up infrastructure.
- **Single-file storage.** All data lives in one JSON file. Human-readable, diffable, committable.
- **No MongoDB installation required.** A single Go binary is all you need.
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
mongolite --file state.json insert go_tests --doc '{"name": "TestLogin", "status": "pass", "type": "unit", "last_execution_ms": 12}'
mongolite --file state.json find go_tests --filter '{"status": "fail"}'
mongolite --file state.json update go_tests --filter '{"name": "TestLogin"}' --update '{"$set": {"last_run": "2025-03-09T10:00:00Z"}}'
```

Point `--file` at a JSON file and operate on it directly. No server, no setup. For driver compatibility, see [Server Mode](#server-mode-optional).

## Agent State Persistence

mongolite is built for multi-step AI agent workflows. The CLI operates directly on the file — no server process needed. Shell scripts, Go, Python, and Node code can all share the same data file.

The point is not just storing data — it's storing *rich, structured data* that an AI agent can populate, scan, query, and act on across sessions. A plain text file can track a to-do list. mongolite lets you maintain a living catalog with enough context that an AI can reason over it.

### Example: Go test catalog

An AI agent inspects your test files and writes a record for each test — its type, category, execution history, a plain-language summary, and any gaps the agent noticed. Now you have a queryable knowledge base your agent can use to find broken tests, spot uncovered areas, or prioritize what to review next.

**Populate** (agent writes after reading test files):

```bash
mongolite --file tests.json insert go_tests --doc '{
  "name": "TestUserCreate",
  "package": "github.com/acme/api/users",
  "file": "users/user_test.go",
  "type": "unit",
  "category": "auth",
  "tldr": "Creates a user with valid email and password; checks returned ID is non-empty",
  "last_run": "2025-03-08T14:22:00Z",
  "last_inspected": "2025-03-08T14:00:00Z",
  "last_execution_ms": 18,
  "status": "pass",
  "tags": ["user", "create"],
  "gaps": "Does not test duplicate email — no conflict error coverage"
}'

mongolite --file tests.json insert go_tests --doc '{
  "name": "TestCheckoutFlow",
  "package": "github.com/acme/api/checkout",
  "file": "checkout/checkout_e2e_test.go",
  "type": "e2e",
  "category": "payments",
  "tldr": "Full checkout flow: cart → payment → order confirmation via HTTP",
  "last_run": "2025-03-07T09:00:00Z",
  "last_inspected": "2025-02-20T11:00:00Z",
  "last_execution_ms": 4300,
  "status": "fail",
  "tags": ["checkout", "payments", "http"],
  "gaps": "No test for declined card or partial refund path",
  "failure_reason": "Stripe mock returns 500 on test card 4000000000000002"
}'
```

**Query** (agent or human scans for actionable signal):

```bash
# All failing tests
mongolite --file tests.json find go_tests --filter '{"status": "fail"}'

# Slow e2e tests (over 2 seconds)
mongolite --file tests.json find go_tests \
  --filter '{"type": "e2e", "last_execution_ms": {"$gt": 2000}}' \
  --sort '{"last_execution_ms": -1}'

# Tests not inspected in the last 30 days
mongolite --file tests.json find go_tests \
  --filter '{"last_inspected": {"$lt": "2025-02-07T00:00:00Z"}}'

# Tests with documented gaps (agent-identified missing coverage)
mongolite --file tests.json find go_tests \
  --filter '{"gaps": {"$exists": true}}' \
  --projection '{"name": 1, "category": 1, "gaps": 1}'

# Count by type
mongolite --file tests.json aggregate go_tests \
  --pipeline '[{"$group": {"_id": "$type", "count": {"$sum": 1}}}]'

# Categories with no passing tests
mongolite --file tests.json aggregate go_tests --pipeline '[
  {"$group": {"_id": "$category", "passing": {"$sum": {"$cond": [{"$eq": ["$status", "pass"]}, 1, 0]}}}},
  {"$match": {"passing": 0}}
]'
```

**Update** (agent records test run results):

```bash
# After running go test, agent updates execution record
mongolite --file tests.json update go_tests \
  --filter '{"name": "TestCheckoutFlow"}' \
  --update '{"$set": {
    "status": "pass",
    "last_run": "2025-03-09T10:00:00Z",
    "last_execution_ms": 3900,
    "failure_reason": null
  }}'
```

**Feed to AI** (pipe the catalog into an LLM for analysis):

```bash
# Ask an AI to find missing test coverage across categories
mongolite --file tests.json find go_tests | \
  venu "Which categories have weak coverage? List gaps and suggest new test names."

# Pipe only failing tests for focused triage
mongolite --file tests.json find go_tests --filter '{"status": "fail"}' | \
  venu "Summarize each failure and suggest a fix."

# Ask for a health report
mongolite --file tests.json aggregate go_tests \
  --pipeline '[{"$group": {"_id": "$category", "total": {"$sum": 1}, "failing": {"$sum": {"$cond": [{"$eq": ["$status","fail"]},1,0]}}, "avg_ms": {"$avg": "$last_execution_ms"}}}]' | \
  venu "Format this as a test health report with recommendations."
```

This pattern — agent populates structured records, you query and filter them, pipe results to an LLM — is what mongolite is for. The data survives between sessions, accumulates over time, and stays human-readable enough to inspect or commit alongside your code.

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

### Schema Metadata

Document expected fields so downstream agents know the contract for each collection. `set-schema` stores JSON schema + optional description alongside the data file, and `get-schema`, `delete-schema`, and `list-schemas` help you inspect or clean up that metadata. Enumerate every key, type, and constraint you rely on; partial schemas defeat the purpose.

```bash
# Define the structure of the tasks collection
mongolite --file state.json set-schema tasks --schema '{
  "bsonType": "object",
  "required": ["task_id", "status"],
  "properties": {
    "task_id": {"bsonType": "string"},
    "status": {"enum": ["pending", "done"]},
    "vars": {"bsonType": "object"}
  }
}' --description "Workflow tasks tracked by the agent"

# Read it back (ndjson output)
mongolite --file state.json get-schema tasks

# List all schemas stored in the file
mongolite --file state.json list-schemas
```

## Supported Operations

### CRUD
- `insert` / `insertOne` / `insertMany`
- `find` (with filter, sort, skip, limit, projection)
- `update` / `updateOne` / `updateMany` (with upsert)
- `delete` / `deleteOne` / `deleteMany`
- `findAndModify`
- `count`
- `distinct`
- `bulkWrite`

### Query Operators
`$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` `$exists` `$type` `$and` `$or` `$nor` `$not` `$all` `$elemMatch` `$size` `$expr`

### Update Operators
`$set` `$unset` `$inc` `$mul` `$min` `$max` `$rename` `$push` `$pull` `$addToSet` `$currentDate`

### Aggregation Pipeline Stages
`$match` `$project` `$group` `$sort` `$limit` `$skip` `$unwind` `$lookup` `$count` `$addFields` `$set` `$unset` `$replaceRoot` `$replaceWith` `$sortByCount`

### Aggregation Accumulators
`$sum` `$avg` `$min` `$max` `$first` `$last` `$push` `$addToSet` `$count` `$stdDevPop` `$stdDevSamp` `$mergeObjects`

### Aggregation Expression Operators

**Arithmetic:** `$add` `$subtract` `$multiply` `$divide` `$mod` `$abs` `$ceil` `$floor` `$round` `$trunc` `$sqrt` `$pow` `$exp` `$log` `$log10`

**Comparison:** `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$cmp`

**Boolean:** `$and` `$or` `$not`

**Conditional:** `$cond` `$ifNull` `$switch`

**String:** `$concat` `$toLower` `$toUpper` `$trim` `$ltrim` `$rtrim` `$split` `$strLenBytes` `$strLenCP` `$substr` `$substrBytes` `$substrCP` `$replaceOne` `$replaceAll` `$strcasecmp` `$indexOfBytes` `$toString`

**Array:** `$size` `$arrayElemAt` `$isArray` `$concatArrays` `$slice` `$reverseArray` `$in` `$indexOfArray` `$range` `$firstN` `$lastN` `$filter` `$map` `$reduce` `$sortArray` `$arrayToObject` `$objectToArray` `$zip`

**Type:** `$toInt` `$toLong` `$toDouble` `$toDecimal` `$toBool` `$toObjectId` `$isNumber` `$type` `$convert`

**Miscellaneous:** `$literal` `$mergeObjects`

### Admin
- `listDatabases` / `dropDatabase`
- `listCollections` / `create` / `drop`
- `createIndexes` / `listIndexes` / `dropIndexes`

### Wire Protocol
- OP_MSG (opcode 2013) — modern protocol used by current drivers
- OP_QUERY (opcode 2004) — legacy protocol for driver handshake compatibility
- OP_REPLY (opcode 1) — legacy reply format

## Server Mode (optional)

Need driver compatibility or remote access? Launch the lightweight server that mimics MongoDB wire operations while still persisting to the same single JSON file.

```bash
# Start the server (default port 27017, data file mongolite.json)
mongolite serve

# With custom options
mongolite serve --port 27018 --file mydata.json
```

Connect using standard tools:

```bash
# mongosh
mongosh mongodb://localhost:27017

# Any driver (example: Go)
client, _ := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017/?directConnection=true"))
```

Once connected, treat it like a MongoDB instance—ideal when an agent step must reuse existing driver code:

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

Built for agent workflows and local development, not production workloads.

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
