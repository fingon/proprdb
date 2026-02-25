# PROPRDB (PROtobuf PackRat DataBase) #

`proprdb` is the third iteration of a "personal stuff database" concept.

- `prdb` (2014-2019, Python, still in personal use, not open source)
- `sdb` (used in https://timeatlaslabs.com since 2024, not open source)

## Goals

- Preserve long-term readability of personal metadata and its change history.
- Keep the interchange format simple and implementation-independent.
- Use strongly typed schemas for object payloads.

## Non-goals

- Encryption (use external tools such as `gpg` if needed)
- Compression (use external tools such as `zstd` if needed)
- Transport protocol design

## High-level design

The shared format is JSON Lines (`.jsonl`) where each line is one object update.
Object payloads are typed via `protobuf.Any` (`@type` determines the concrete Protobuf message).

Updates can be received in any order. Conflict resolution is timestamp-based:

- Newer `atNs` wins.
- If timestamps are equal, the update should be treated as idempotent and payload-equal.

## Object model

Each object update has:

- `id`: unique object identifier which is valid and unique within the type UUID (`string`)
- `deleted`: whether the object is deleted (optional, `bool`)
- `atNs`: last update time as Unix epoch nanoseconds (`int64`)
- `data`: object payload as `protobuf.Any`

Example JSONL line:

```json
{"id":"person:123","atNs":1761736535123456789,"data":{"@type":"type.googleapis.com/github.com.fingon.proprdb.v1.example.Person","name":"Ada"}}
```

Deletion marker example:

```json
{"id":"person:123","deleted":true,"atNs":1761736599000000000,"data":{"@type":"type.googleapis.com/github.com.fingon.proprdb.v1.example.Person"}}
```

## Local storage (SQLite backend)

The wire format is JSONL; local storage is implementation-defined.
This repository uses SQLite with one table per supported object type.

Each object table stores:

- `id` (`TEXT PRIMARY KEY`)
- `at_ns` (`INTEGER NOT NULL`)
- `data` (`BLOB NOT NULL`) as encoded `protobuf.Any`

`_deleted` table stores tombstones:

- `id` (`TEXT NOT NULL`)
- `table_name` (`TEXT NOT NULL`)
- `at_ns` (`INTEGER NOT NULL`)
- primary key: (`table_name`, `id`)

`_sync` table tracks what has been exchanged with each remote:

- `object_id` (`TEXT NOT NULL`)
- `table_name` (`TEXT NOT NULL`)
- `at_ns` (`INTEGER NOT NULL`)
- `remote` (`TEXT NOT NULL`)
- primary key: (`object_id`, `table_name`, `remote`)

Implementations may also project selected typed fields from `data` into additional tables for queryability.

## Protobuf extensions

`proprdb` defines generator options in `proto/proprdb/options.proto`.

### Field option

- `proprdb.external` (`bool`, field-level):
  - Marks scalar message fields to be projected into SQLite columns in addition to `data`.
  - If omitted or `false`, field stays only inside serialized protobuf payload.

Example:

```proto
message Person {
  string name = 1 [(proprdb.external) = true];
  int64 age = 2 [(proprdb.external) = true];
}
```

### Message options

- `proprdb.omit_table` (`bool`, message-level):
  - Do not generate table/CRUD code for this message.

- `proprdb.omit_sync` (`bool`, message-level):
  - Generate table/CRUD code, but exclude the message from JSONL syncing.
  - `WriteJSONL` will not export it.
  - `ReadJSONL` will ignore incoming records for the message and log an error.

- `proprdb.validate_write` (`bool`, message-level):
  - Generated `Insert`/`UpdateByID`/`UpdateRow` call `data.Valid() error`.
  - Validation is not applied to data imported through JSONL.

- `proprdb.allow_custom_id_insert` (`bool`, message-level):
  - Generated table keeps `Insert(data)` and additionally gets `InsertWithID(id, data)`.
  - `InsertWithID` requires `id` to be a valid UUID.

Example:

```proto
message Person {
  option (proprdb.validate_write) = true;
  option (proprdb.allow_custom_id_insert) = true;
  string name = 1 [(proprdb.external) = true];
}

message Note {
  option (proprdb.omit_sync) = true;
  string text = 1 [(proprdb.external) = true];
}

message InternalOnly {
  option (proprdb.omit_table) = true;
  string data = 1;
}
```

## Getting started

Prerequisites:

- Go `1.25+`

Commands:

```bash
make test
make lint
```

### Generate from proto (example)

The example schema is in `test/fixtures/system.proto`. To generate both protobuf Go types and `proprdb` CRUD code:

```bash
# Build plugin
go build -o /tmp/protoc-gen-proprdb ./cmd/protoc-gen-proprdb

# Generate code
protoc \
  -I test/fixtures \
  -I . \
  --plugin=protoc-gen-proprdb=/tmp/protoc-gen-proprdb \
  --go_out=test/system \
  --go_opt=paths=source_relative \
  --proprdb_out=paths=source_relative:test/system \
  test/fixtures/system.proto
```
