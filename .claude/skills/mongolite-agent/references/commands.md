# Command Reference

## Global flags
```
--file FILE    data file path (default: mongolite.json)
--db DATABASE  database name (default: test)
```

## Commands

### find
```
find <collection> [--filter JSON] [--filter-file FILE]
                  [--sort JSON] [--sort-file FILE]
                  [--limit N] [--skip N]
```
Output: ndjson, one doc per line. Pipe to `jq` for extraction.

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

## Query operators
`$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$in` `$nin` `$exists` `$type`
`$and` `$or` `$nor` `$not` `$all` `$elemMatch` `$size`

## Update operators
`$set` `$unset` `$inc` `$mul` `$min` `$max` `$rename`
`$push` `$pull` `$addToSet` `$currentDate`

## Aggregation stages
`$match` `$project` `$sort` `$limit` `$skip` `$unwind` `$group` `$lookup` `$count`

## Accumulators (in $group)
`$sum` `$avg` `$min` `$max` `$first` `$last` `$push` `$addToSet`
