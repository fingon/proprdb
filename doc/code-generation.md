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
  - `InsertWithID` requires a canonical lowercase UUID.

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

## Go runtime

The Go runtime and generated bindings require Go 1.27 or newer. Generated row
names alias `rt.Row[*Message]`; table methods delegate to generic methods on
`rt.Table`. Message bindings retain projection and index metadata and an
optional write-validation callback. Shared selection, protobuf decoding,
CRUD validation, deletion, initialization, and unknown-row draining live in
the runtime. Custom-ID inserts and change listeners remain opt-in generated APIs.

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

## Rust target

Build the protoc plugin with `make protoc-gen-proprdb-rust`. Generate message
structs using Prost, then generate the database bindings:

```sh
protoc -I test/fixtures -I . \
  --plugin=protoc-gen-proprdb-rust=./protoc-gen-proprdb-rust \
  --proprdb-rust_out=paths=source_relative:test/rust/src \
  test/fixtures/system.proto
```

The plugin emits `<first-file>.proprdb.pb.rs`, with input files sorted by path.
Pass all schemas for a database in one invocation to get one `Crud` wrapper.
The generated bindings reference Prost types through `crate::<proto package>`;
use `prost_build::Config::include_file` to include the message module tree at
crate root. A `go_package` option is optional for the Rust target.

Add the `proprdb-runtime` crate from `rt/rust` as a dependency and include the
bindings in a module. The runtime uses Prost 0.14 and rusqlite 0.40. See
`test/rust` for a complete example.

```rust
let connection = proprdb_runtime::Connection::open_in_memory()?;
let crud = system::Crud::new(&connection);
crud.initialize()?;
let row = crud.person.insert(&person)?;
let found = crud.person.select_by_id(&row.id)?;
```

The Rust target supports typed CRUD, UUID IDs (generated as v7), scalar projections (including
optional and oneof presence), generated indexes, projection reconciliation,
write validation, custom ID insertion, change listeners, and query statistics.
With `validate_write`, implement `valid(&self) -> proprdb_runtime::Result<()>`
on the message type. `insert_with_id` is only available for messages declaring
`allow_custom_id_insert`. SQL selection takes a predicate and bound
`proprdb_runtime::Value` arguments; an empty predicate is rejected.

Tables borrow a `proprdb_runtime::Connection`, which wraps rusqlite and shares
change listeners across table and CRUD wrappers. Use `connection.transaction()`
for an explicit transaction: notifications are delivered after commit and
discarded on rollback, including rollback on drop. Writes and schema
reconciliation use savepoints. `proprdb_runtime::atomic` supports nested
savepoint operations with the same notification semantics. Failed validation
and writes produce no notifications.

Updates insert a row when its ID does not exist. Deleting an absent ID still
records a tombstone, unless `omit_sync` is set. Write IDs must be canonical
lowercase UUID values. Initialization audits stored IDs and reconciles all
generated tables atomically.

`Crud` exposes `read_jsonl`, `prepare_jsonl`, `acknowledge_jsonl`,
`discard_jsonl`, and `write_jsonl`, using the same JSONL, checkpoint, projection
history, and core table formats as Go. Import commits each physical record
separately, preserves unknown types for later replay, rejects conflicting
equal timestamps, and transfers remote watermarks when unknown types drain.
Local write validation does not apply to sync imports. An empty remote disables
watermark bookkeeping; whitespace is a remote name. Prepared exports stage a
stable snapshot, and acknowledging a checkpoint only marks exported versions
as delivered. Failed writes discard the batch without advancing watermarks.

`Crud::introspect_tables` reports generated and core table descriptors, counts,
and payload sizes. `proprdb_runtime::query_statistics` lists persistent call
counts and durations keyed by full parameterized SQL; `clear_query_statistics`
clears them. Table-level `query_statistics(predicate)` remains a convenience
lookup for a specific predicate.

Run `make rust-test`, `make rust-build`, and `make rust-lint` for the Rust
checks. These are also included in the project-wide test, build, and lint
targets; `make generate` and `make verify-generated` cover Rust fixtures and
golden files as well.
