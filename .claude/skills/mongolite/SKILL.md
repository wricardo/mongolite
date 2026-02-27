---
name: mongolite
description: >
  Use mongolite to store, query, and update persistent state for AI agent workflows.
  Use this skill when an agent needs to: track task progress across steps, deduplicate
  work (seen URLs, processed IDs), checkpoint and resume after failure, accumulate
  results, maintain a task queue, or store any key-value state that must survive between
  tool calls or conversation turns. The CLI operates directly on a JSON file — no server
  needed.
---

# mongolite

## Invocation

```bash
mongolite --file <state-file> --db agent [command]
```

If `mongolite` is not on PATH, install it first:

```bash
go install github.com/wricardo/mongolite@latest
```

Use a dedicated file per workflow (e.g. `/tmp/my-workflow.json`). The file is created automatically on first write.

## Core patterns

### Store a value (insert once)
```bash
mongolite --file state.json --db agent insert state \
  --doc '{"_id": "job:config", "target": "https://example.com", "started_at": "2024-01-01"}'
```

Use a meaningful `_id` (e.g. `"task:42"`, `"seen:https://example.com"`, `"step:parse"`) so you can look it up without knowing the ObjectID.

### Read state
```bash
mongolite --file state.json --db agent find state \
  --filter '{"_id": "job:config"}'
```

### Update state
```bash
mongolite --file state.json --db agent update state \
  --filter '{"_id": "step:parse"}' \
  --update '{"$set": {"status": "done", "result": "42 items"}}'
```

### Check existence (returns 0 or 1)
```bash
mongolite --file state.json --db agent count state \
  --filter '{"_id": "seen:https://example.com"}'
```

### Delete
```bash
mongolite --file state.json --db agent delete state \
  --filter '{"_id": "step:parse"}'
```

## No upsert in CLI

The `update` command does **not** support `--upsert`. To create-or-update:
1. `count` to check if the doc exists
2. If 0 → `insert`; if 1 → `update`

## Common agent workflows

See [references/patterns.md](references/patterns.md) for:
- Task queue (enqueue, claim next, mark done)
- Deduplication / seen-set
- Checkpoint / resume
- Result accumulation

See [references/commands.md](references/commands.md) for full flag reference and all supported operators.
