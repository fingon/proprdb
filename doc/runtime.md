# Runtime behavior

[Project overview](../README.md) · [Design and data model](design.md) ·
[Protobuf options and code generation](code-generation.md)

## JSONL sync API semantics

Generated CRUD wrappers include:

- `WriteJSONL(remote string, w io.Writer) error`
- `PrepareJSONL`, `AcknowledgeJSONL`, and `DiscardJSONL`
- `ReadJSONL(remote string, r io.Reader) error`

Swift exposes the same workflow as `prepareJSONL`, `acknowledgeJSONL`,
`discardJSONL`, `writeJSONL`, and `readJSONL`. A prepared export is a stable
database snapshot. Acknowledging its checkpoint advances sync watermarks;
discarding it leaves them unchanged. `WriteJSONL`/`writeJSONL` are convenience
operations that prepare and then acknowledge. Acknowledging or discarding an
already-consumed checkpoint from the same database succeeds.

Swift `readJSONL` accepts an unopened `InputStream`. The runtime opens and
closes the stream and reads it incrementally, retaining at most its read buffer
and the current JSONL line.

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
canonical lowercase UUIDv7 values. `deleted` is absent or a JSON boolean,
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
