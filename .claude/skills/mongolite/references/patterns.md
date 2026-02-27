# Agent State Patterns

## Task queue

```bash
# Enqueue
mongolite --file state.json --db agent insert tasks \
  --doc '{"_id": "task:1", "action": "fetch", "url": "https://a.com", "status": "pending"}'

# Claim next pending task (read the first result)
mongolite --file state.json --db agent find tasks \
  --filter '{"status": "pending"}' --sort '{"_id": 1}' --limit 1

# Mark done
mongolite --file state.json --db agent update tasks \
  --filter '{"_id": "task:1"}' \
  --update '{"$set": {"status": "done", "result": "ok"}}'

# Count remaining
mongolite --file state.json --db agent count tasks \
  --filter '{"status": "pending"}'
```

## Deduplication / seen-set

```bash
# Check if already seen
mongolite --file state.json --db agent count seen \
  --filter '{"_id": "url:https://example.com"}'
# → 0 means not seen; proceed and then mark seen:

mongolite --file state.json --db agent insert seen \
  --doc '{"_id": "url:https://example.com"}'
```

## Checkpoint / resume

```bash
# Save checkpoint
mongolite --file state.json --db agent update progress \
  --filter '{"_id": "checkpoint"}' \
  --update '{"$set": {"last_page": 5, "items_processed": 142}}'
# (insert first if it doesn't exist yet)

# Read checkpoint on resume
mongolite --file state.json --db agent find progress \
  --filter '{"_id": "checkpoint"}'
```

## Result accumulation

```bash
# Append a result
mongolite --file state.json --db agent update results \
  --filter '{"_id": "run:1"}' \
  --update '{"$push": {"items": {"url": "https://a.com", "title": "A"}}}'

# Or insert individual result docs and query later
mongolite --file state.json --db agent insert results \
  --doc '{"run": "run:1", "url": "https://a.com", "score": 0.9}'

mongolite --file state.json --db agent find results \
  --filter '{"run": "run:1"}' --sort '{"score": -1}' --limit 10
```

## Key-value store

```bash
# Set a named value
mongolite --file state.json --db agent insert kv \
  --doc '{"_id": "api_token", "value": "abc123"}'

# Read it
mongolite --file state.json --db agent find kv \
  --filter '{"_id": "api_token"}' | jq -r '.value'
```

## Numeric counter

```bash
# Increment
mongolite --file state.json --db agent update counters \
  --filter '{"_id": "pages_fetched"}' \
  --update '{"$inc": {"n": 1}}'

# Read
mongolite --file state.json --db agent find counters \
  --filter '{"_id": "pages_fetched"}' | jq '.n'
```
