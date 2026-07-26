# Protobuf options and code generation

[Project overview](../README.md) · [Design and data model](design.md) ·
[Runtime behavior](runtime.md)

## Protobuf extensions

`proprdb` defines generator options in `proto/proprdb/options.proto`.

### Field option

- `proprdb.external` (`bool`, field-level):
  - Marks scalar message fields to be projected into SQLite columns in addition
    to `data`.
  - If omitted or `false`, field stays only inside serialized protobuf payload.
  - Fields with protobuf presence, including scalar oneof fields, use nullable
    projection columns.

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
  - Generated table keeps `Insert(data)` and additionally gets
    `InsertWithID(id, data)`.
  - `InsertWithID` requires a canonical lowercase UUIDv7.

Existing protobuf field names, numbers, types, and presence semantics are
immutable. Projection membership may be added or removed. Initialization adds
new projection columns, removes obsolete ones, and recomputes projection values
from the protobuf payload. Removing columns requires SQLite 3.35 or newer.
Incompatible existing projection definitions fail initialization.

- `proprdb.change_listeners` (`bool`, message-level):
  - Generates a typed change stream for the table.

- `proprdb.query_statistics` (`bool`, message-level):
  - Accumulates generated select call counts and duration sums for the table.

- `proprdb.indexes` (`repeated proprdb.Index`, message-level):
  - Declares non-unique SQLite indexes for projected fields
    (`(proprdb.external)=true`).
  - Supports both single-field and multi-field indexes.

Example:

```proto
message Person {
  option (proprdb.validate_write) = true;
  option (proprdb.allow_custom_id_insert) = true;
  option (proprdb.change_listeners) = true;
  option (proprdb.indexes) = { fields: "name" };
  option (proprdb.indexes) = { fields: "name" fields: "age" };
  string name = 1 [(proprdb.external) = true];
  int64 age = 2 [(proprdb.external) = true];
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

## Generate from proto

The example schema is in `test/fixtures/system.proto`. To generate both
protobuf Go types and `proprdb` CRUD code, run the following commands from the
repository root:

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
