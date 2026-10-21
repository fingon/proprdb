# Runtime behavior

[Project overview](../README.md) · [Design and data model](design.md) ·
[Protobuf options and code generation](code-generation.md)

## JSONL sync API semantics

Generated CRUD wrappers include:

- `WriteJSONL(remote string, w io.Writer) error`
- `PrepareJSONL`, `AcknowledgeJSONL`, and `DiscardJSONL`
- `ReadJSONL(remote string, r io.Reader) error`

Go table operations and CRUD sync operations also provide `Context` variants,
such as `SelectContext(ctx, where, args...)`, `InitContext(ctx)`, and
`PrepareJSONLContext(ctx, remote, writer)`. Pass a context to cancel queries,
schema reconciliation, exports, or imports. The existing methods use
`context.Background()` for convenience. Cancellation does not undo records
already committed by an import.

Swift exposes the same workflow as `prepareJSONL`, `acknowledgeJSONL`,
`discardJSONL`, `writeJSONL`, and `readJSONL`. A prepared export is a stable
database snapshot. Acknowledging its checkpoint advances sync watermarks;
discarding it leaves them unchanged. `WriteJSONL`/`writeJSONL` are convenience
operations that prepare and then acknowledge. Acknowledging or discarding an
already-consumed checkpoint from the same database succeeds.

Swift also provides `prepareJSONL(remote:write:)`, whose throwing callback
receives one JSONL record as `Data` at a time. It returns a checkpoint to
acknowledge after delivery succeeds, or discard without advancing watermarks.
A writer failure discards the staged checkpoint and propagates the error.
`writeJSONL(remote:write:)` streams and then acknowledges automatically.
The string convenience API collects this stream. Source rows are staged in
bounded keyset batches within one transaction, preserving snapshot semantics.

`remote` controls whether `_sync` bookkeeping is used:

- `remote == ""` (exact empty string):
  - `WriteJSONL` exports records without `_sync`-based deduplication.
  - `ReadJSONL` imports records without creating/updating `_sync` rows.
- `remote != ""`:
  - Both methods use `_sync` rows scoped by that remote value.

Whitespace-only strings are treated as non-empty remote names.

Imports commit one JSONL record at a time. State changes and their sync
watermark are atomic, so a failing record leaves no partial state while earlier
successful records remain committed. Older timestamps are ignored. Equal
timestamps must describe semantically equal protobuf state (or the same
tombstone); otherwise import returns a conflict.

Every nonblank physical JSONL line contains exactly one object. Object IDs are
canonical lowercase UUID values. `deleted` is absent or a JSON boolean,
`atNs` is a signed decimal `int64` number or decimal string, and `data` is an
object with a nonempty string `@type`. Invalid scalar coercions are rejected
with the physical line number.

Bindings with `omit_sync` never export records, including records parked in the
unknown-type table before the binding became available. Parked records remain
stored so a future sync-enabled binding can drain them.

## Change listeners

Tables with `proprdb.change_listeners = true` expose typed, future-only change
streams. Go uses `table.Changes(ctx)` and Swift uses
`await actor.person.changes()`. Streams are unbounded and lossless while the
subscriber remains registered. Successful local writes, accepted JSONL
imports, and accepted unknown-row drains publish upsert or delete state after
the operation's transaction or savepoint succeeds.

Swift generates the actor and its table proxies as the public API by default.
The synchronous `CRUD` and table APIs remain internal. Pass
`PublicSynchronousAPI=true` to `protoc-gen-proprdb-swift` to make the
synchronous API public as well. `Visibility=Public|Internal|Package` controls
the actor-facing API and defaults to `Public`.

## Query statistics

Tables with `proprdb.query_statistics = true` accumulate statistics for
successful generated `Select`/`select` calls. Statistics are stored in the
`_querystat` core table by table name and exact parameterized SQL string.
Bind values are not stored. Each row contains a call count and the sum of
full select durations in nanoseconds, including row decoding but excluding
the statistics update.

Go exposes `rt.QueryStatistics` and `rt.ClearQueryStatistics`. Swift exposes
`queryStatistics(_:)` and `clearQueryStatistics(_:)`; actor users can call
them through `withDatabase`. Statistics persist until cleared.

## Nested projection reconciliation

Message-level `external_paths` declarations select scalar fields through
singular nested messages. Generated indexes reference the protobuf paths, while
SQL predicates use flattened column names (for example, `location_lon`). Missing
messages or optional/oneof leaves produce NULL; present ordinary scalar leaves
produce their default value, including zero.

Adding projections triggers transactional backfill from stored protobuf payloads.
Reconciliation changes only projection columns and managed schema/index state;
it preserves payload bytes, unknown fields, IDs, object timestamps, tombstones,
and sync checkpoints. Decode or SQL failures roll back reconciliation. Subsequent
CRUD writes, JSONL imports, and unknown-type draining use the same projection
logic. Repeated initialization with an unchanged schema needs no backfill.

Initialization and ordinary queries trust stored IDs and do not run write
validators over stored payloads. IDs are validated at write boundaries instead.
Unchanged initialization inspects schema and index metadata without scanning
object IDs or decoding payloads. Schema changes can still require a projection
backfill, and initialization applies parked sync records for supported types.

Run `make benchmark-init SWIFT_ARGS=--disable-sandbox` to measure repeated CRUD
initialization in Go, Swift, and Rust. The benchmarks use the same fixture with
0, 1,000, or 10,000 objects in both memory and file-backed databases, including
shared tombstone, sync, unknown-type, and export-entry rows. Timings exclude
fixture setup and report SQL statement counts; Go counts exclude transaction
control statements, while Swift and Rust count them through SQLite tracing.

A local Apple M1 Pro run with 10 initialization iterations and 10,000 rows in
each populated fixture table measured the following elapsed times. Swift uses
the debug test build. Rust measurements were blocked by Cargo cache permissions.

| Runtime | Storage | Before | After |
| --- | --- | ---: | ---: |
| Go | Memory | 62.4 ms | 0.17 ms |
| Go | File | 64.8 ms | 0.25 ms |
| Swift | Memory | 134.0 ms | 0.40 ms |
| Swift | File | 133.2 ms | 0.50 ms |

After the change, SQL counts stayed constant at 29 statements for Go and 51 for
Swift across all three fixture sizes.

## Mutation generations and dirty windows

All generated object tables have a persistent, local signed 64-bit mutation
generation, including `omit_sync` tables. SQLite triggers advance it for actual
inserts, updates, and deletes through CRUD, accepted sync imports, unknown-row
draining, or direct SQL. SQL updates that leave every stored column unchanged
and deletes of absent rows do not advance it. Tombstones, sync watermarks, and
other core bookkeeping are excluded. Rollback and savepoint rollback restore
both generations and pending dirty state; integer overflow fails the write.

`rt.TableGenerations(ctx, q, tables)` in Go, `tableGenerations(_:tables:)` in
Swift, and `table_generations(connection, tables)` in Rust read generations for
explicitly named initialized tables. Unknown tables return an error. Use the
same transaction for these reads and dependent data reads to obtain one snapshot.
Multiple reads through a plain database handle do not promise a common snapshot.
When saving refresh checkpoints, save the dependency generations transactionally
with the derived output, accounting for any output writes to those dependencies.
Missing checkpoints and algorithm changes still require bootstrap/full refresh.

Tables declaring `dirty_windows` support independently registered consumers.
Each `(table, window, consumer)` stores one enclosing range, rather than events
or row history. Inserts contribute new bounds, deletes contribute old bounds,
and updates contribute both, even when only payload or another field changes.
Bounds are inclusive and use the declared fields' units. Distant changes include
the gap between them. NULL, noninteger, or reversed bounds require full refresh.
Primary-key replacements through `INSERT OR REPLACE` and `UPDATE OR REPLACE`
preserve displaced bounds even when recursive triggers are disabled. An ignored
conflicting write may conservatively dirty the existing range, without advancing
the table generation. Prefer `ON CONFLICT DO UPDATE` for upserts.
Application-specific padding and nonlocal dependencies remain the caller's
responsibility. Changed IDs and parent IDs are not retained.

The runtime API families are:

| Operation | Go (`rt`) | Swift | Rust |
| --- | --- | --- | --- |
| Register | `RegisterDirtyWindow` | `registerDirtyWindow` | `register_dirty_window` |
| Read | `ReadDirtyWindow` | `readDirtyWindow` | `read_dirty_window` |
| Acknowledge | `AcknowledgeDirtyWindow` | `acknowledgeDirtyWindow` | `acknowledge_dirty_window` |
| Remove consumer | `RemoveDirtyWindowConsumer` | `removeDirtyWindowConsumer` | `remove_dirty_window_consumer` |

Registration requires an initialized declaration and a nonempty consumer name.
It is idempotent and starts new consumers dirty with a full-refresh requirement,
without scanning existing objects. Unregistered consumers accumulate no state.
Reads do not clear pending work. A clean state has no bounds; a full-refresh state
may have partial bounds, which must not restrict the refresh.

Each read returns an opaque acknowledgment token. Re-read and retry if
acknowledgment reports `ErrStaleDirtyWindow` (Go), `StaleDirtyWindowError` (Swift),
or `Error::StaleDirtyWindow` (Rust). Tokens become stale after subsequent writes,
acknowledgment, schema invalidation, or removal and re-registration. A failed
acknowledgment never clears pending work. Tokens are process-local values and
should not be persisted; read pending state again after reopening the database.

Commit acknowledgment in the same transaction as the derived output. For example,
a Go refresh can register at startup and then use this transaction body:

```go
err := q.WithTransaction(ctx, func(tx rt.DBTX) error {
    state, err := rt.ReadDirtyWindow(ctx, tx, recordingTable, "time", "movement")
    if err != nil {
        return err
    }
    if !state.Dirty {
        return nil
    }
    if err := rebuildMovement(ctx, tx, state); err != nil {
        return err
    }
    return rt.AcknowledgeDirtyWindow(ctx, tx, state.Token)
})
```

`rebuildMovement` must honor `Full`, use current data, and write its results through
`tx`. If it changes its own tracked inputs, re-read pending state and ensure those
new inputs are processed before acknowledging. Propagate errors from the enclosing
transaction; a failed output write or stale acknowledgment rolls back the output.
Swift uses `withTransaction`; Rust passes a `Transaction` to the same runtime
functions and commits after acknowledgment. Swift actor users access the runtime
functions through `withDatabase`.

Tracking storage is local to SQLite and is not exported through JSONL. All three
runtimes use the same schema and trigger semantics. Initialization installs
tracking for existing databases without generating historical ranges. Projection
or dirty-window declaration changes advance the affected generation and require
full refresh for its existing consumers; removed declarations remove their
consumers. Unchanged initialization reads metadata without scanning object rows.
Direct SQL writers remain responsible for consistency between payloads and
projection columns and must preserve the managed tracking tables and triggers.

## Dirty-key trackers

Tables declaring `dirty_keys` retain affected object IDs for independently
registered `(table, tracker, consumer)` identities. Each consumer stores a set,
not an event history. Repeated writes deduplicate IDs while advancing its
revision. Deletion/reinsertion does not cancel pending work. Deleted IDs remain
until acknowledged, independently of sync tombstones. Read the current object
by ID to distinguish a surviving object from a deletion; historical payloads
and deletion flags are not retained.

Inserts and deletes always contribute their IDs. Updates contribute the ID when
any watched projected value changes, using NULL-safe comparisons. With no
`watched_fields`, any actual stored-column change qualifies. ID changes
contribute both old and new IDs regardless of watched values. Primary-key
replacements through `INSERT OR REPLACE` and `UPDATE OR REPLACE` retain affected
IDs with recursive triggers enabled or disabled, even when the replacement
has identical watched values. This covers managed object-table primary keys;
application-defined unique constraints are outside the managed schema.

CRUD, direct SQL, accepted sync imports, and unknown-row draining use the same
triggers. Ignored stale/idempotent sync records and sync bookkeeping alone do
not dirty keys. Tracking is persistent, local to SQLite, and excluded from JSONL.

| Operation | Go (`rt`) | Swift | Rust |
| --- | --- | --- | --- |
| Register | `RegisterDirtyKeys` | `registerDirtyKeys` | `register_dirty_keys` |
| Read | `ReadDirtyKeys` | `readDirtyKeys` | `read_dirty_keys` |
| Acknowledge | `AcknowledgeDirtyKeys` | `acknowledgeDirtyKeys` | `acknowledge_dirty_keys` |
| Remove consumer | `RemoveDirtyKeysConsumer` | `removeDirtyKeysConsumer` | `remove_dirty_keys_consumer` |

Registration requires an initialized declaration and nonempty consumer name.
It is idempotent. New consumers start dirty with `Full = true` without scanning
existing objects. Full repair must cover all application dependencies, including
obsolete derived output; pending IDs cannot restrict that repair. Unregistered
consumers accumulate no state. Algorithm changes require application-controlled
full repair, for example by removing and re-registering the consumer.

Reads return `Dirty`, `Full`, sorted unique `IDs` (`ids` in Swift/Rust), and an
opaque token. A read obtains metadata and IDs in one transaction or savepoint
and does not clear them. There is no pagination. Full-repair state can coexist
with pending IDs. Clean state has neither repair flags nor pending IDs.

Acknowledgment covers the entire read. It atomically compares the registration
and revision, clears pending IDs and repair flags, and advances the revision.
Any later qualifying mutation, including a repeated mutation to an already
pending ID, makes the token stale. Acknowledgment, schema invalidation, and
removal/re-registration also invalidate tokens. Failed acknowledgment clears
nothing and reports `ErrStaleDirtyKeys`, `StaleDirtyKeysError`, or
`Error::StaleDirtyKeys`. Re-read and retry. Tokens are process-local values;
read new state after reopening rather than persisting tokens.

Commit derived output and acknowledgment in the same caller transaction. All
operations support nested savepoints. Rollback restores both pending keys and
derived output. If output writes alter tracked inputs, process and re-read those
inputs before acknowledging. Propagate stale acknowledgment errors so dependent
output rolls back.

```go
err := q.WithTransaction(ctx, func(tx rt.DBTX) error {
    state, err := rt.ReadDirtyKeys(ctx, tx, itemTable, "labels", "search")
    if err != nil {
        return err
    }
    if !state.Dirty {
        return nil
    }
    if err := repairSearch(ctx, tx, state.Full, state.IDs); err != nil {
        return err
    }
    return rt.AcknowledgeDirtyKeys(ctx, tx, state.Token)
})
```

All runtimes share tracker definitions, storage, and canonical mutation
signatures. Initialization upgrades existing databases transactionally without
scanning objects for historical keys. Changed projection or mutation declarations
invalidate tokens and require full repair for surviving consumers; removed
trackers remove their consumers and pending IDs. Unchanged initialization
preserves pending state. Revision overflow fails the write.
