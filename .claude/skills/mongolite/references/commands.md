# Command Reference

## Global flags
```
--file FILE    data file path (default: ./mongolite.json)
--db DATABASE  database name (default: test)
```

## Commands

### find
```
find <collection> [--filter JSON] [--filter-file FILE]
                  [--sort JSON] [--sort-file FILE]
                  [--projection JSON] [--projection-file FILE]
                  [--limit N] [--skip N]
```
Output: ndjson, one doc per line. Pipe to `jq` for extraction.

Projection controls which fields are returned. `1` = include, `0` = exclude:
```bash
mongolite --file state.json find tasks --filter '{"status":"pending"}' \
  --projection '{"_id":1,"action":1}'
```

### distinct
```
distinct <collection> --field FIELD [--filter JSON] [--filter-file FILE]
```
Output: `{"values": [...]}` — deduplicated values for a field across matching docs.

```bash
mongolite --file state.json distinct tasks --field status
```

### insert
```
insert <collection> (--doc JSON | --doc-file FILE)
```
Output: `{"insertedId": ...}`

### insert-many
```
insert-many <collection> (--docs JSON_ARRAY | --docs-file FILE)
```
Output: `{"insertedCount": N}`

### update
```
update <collection> [--filter JSON | --filter-file FILE]
                    (--update JSON | --update-file FILE)
                    [--multi]
```
No `--upsert` flag. Default updates first match only; `--multi` updates all matches.
Output: `{"matchedCount": N, "modifiedCount": N}`

### delete
```
delete <collection> [--filter JSON | --filter-file FILE] [--multi]
```
Output: `{"deletedCount": N}`

### count
```
count <collection> [--filter JSON | --filter-file FILE]
```
Output: `{"count": N}`

### aggregate
```
aggregate <collection> (--pipeline JSON_ARRAY | --pipeline-file FILE)
```
Output: ndjson

### list-dbs / list-collections
```
list-dbs
list-collections
```

### set-schema / get-schema / delete-schema / list-schemas
```
set-schema <collection> [--schema JSON] [--schema-file FILE] [--description TEXT]
get-schema <collection>
delete-schema <collection>
list-schemas
```
Output of `get-schema`: `{"db":..., "collection":..., "schema":..., "description":...}`
Output of `list-schemas`: ndjson, one schema entry per line.

## Query operators
`$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` `$exists` `$type`
`$and` `$or` `$nor` `$not` `$all` `$elemMatch` `$size` `$expr`

Use `$expr` to compare fields against each other or evaluate expressions inside a filter:
```bash
# find docs where spend > budget
mongolite --file state.json find tasks \
  --filter '{"$expr": {"$gt": ["$spend", "$budget"]}}'
```

## Update operators
`$set` `$unset` `$inc` `$mul` `$min` `$max` `$rename`
`$push` `$pull` `$addToSet` `$currentDate`

## Aggregation stages
`$match` `$project` `$group` `$sort` `$limit` `$skip` `$unwind` `$lookup` `$count`
`$addFields` `$set` `$unset` `$replaceRoot` `$replaceWith` `$sortByCount`

`$addFields` / `$set` — add or overwrite fields using expressions without dropping other fields:
```bash
mongolite --file state.json aggregate tasks --pipeline '[
  {"$addFields": {"total": {"$multiply": ["$qty", "$price"]}}}
]'
```

`$sortByCount` — shorthand for group + sort by count descending:
```bash
mongolite --file state.json aggregate tasks --pipeline '[{"$sortByCount": "$status"}]'
```

## Accumulators (in $group)
`$sum` `$avg` `$min` `$max` `$first` `$last` `$push` `$addToSet`
`$count` `$stdDevPop` `$stdDevSamp` `$mergeObjects`

Accumulator values support full expressions:
```bash
# sum of qty * price per category
mongolite --file state.json aggregate items --pipeline '[
  {"$group": {"_id": "$category", "revenue": {"$sum": {"$multiply": ["$qty","$price"]}}}}
]'
```

## Aggregation expression operators

**Arithmetic:** `$add` `$subtract` `$multiply` `$divide` `$mod` `$abs` `$ceil` `$floor` `$round` `$trunc` `$sqrt` `$pow` `$exp` `$log` `$log10`

**Comparison:** `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$cmp`

**Boolean:** `$and` `$or` `$not`

**Conditional:** `$cond` `$ifNull` `$switch`

**String:** `$concat` `$toLower` `$toUpper` `$trim` `$ltrim` `$rtrim` `$split` `$strLenBytes` `$strLenCP` `$substr` `$substrBytes` `$substrCP` `$replaceOne` `$replaceAll` `$strcasecmp` `$indexOfBytes` `$toString`

**Array:** `$size` `$arrayElemAt` `$isArray` `$concatArrays` `$slice` `$reverseArray` `$in` `$indexOfArray` `$range` `$firstN` `$lastN` `$filter` `$map` `$reduce` `$sortArray` `$arrayToObject` `$objectToArray` `$zip`

**Type:** `$toInt` `$toLong` `$toDouble` `$toDecimal` `$toBool` `$toObjectId` `$isNumber` `$type` `$convert`

**Misc:** `$literal` `$mergeObjects`
