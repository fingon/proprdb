# Design and data model

[Project overview](../README.md) · [Runtime behavior](runtime.md) ·
[Protobuf options and code generation](code-generation.md)

## Goals

- Preserve long-term readability of personal metadata through durable JSONL
  exports.
- Keep the interchange format simple and implementation-independent.
- Use strongly typed schemas for object payloads.

## Non-goals

- Encryption (use external tools such as `gpg` if needed)
- Compression (use external tools such as `zstd` if needed)
- Transport protocol design

## High-level design

The shared format is JSON Lines (`.jsonl`) where each line is one object update.
Object payloads are typed via `protobuf.Any` (`@type` determines the concrete
Protobuf message).

Updates can be received in any order. Conflict resolution is timestamp-based:

- Newer `atNs` wins.
- If timestamps are equal, the update should be treated as idempotent and
  payload-equal.

SQLite stores only the latest state of each object. Change history exists only
in JSONL exports retained by the application; ProprDB does not keep a local
append-only history.

## Object model

Each object update has:

- `id`: canonical lowercase RFC 9562 UUIDv7, unique within the protobuf type
  (`string`)
- `deleted`: whether the object is deleted (optional, `bool`)
- `atNs`: last update time as Unix epoch nanoseconds (`int64`)
- `data`: object payload as `protobuf.Any`

Example JSONL line:

```json
{"id":"018f4f3f-6f9f-7a1b-8f55-1234567890ab","atNs":1761736535123456789,"data":{"@type":"type.googleapis.com/github.com.fingon.proprdb.v1.example.Person","name":"Ada"}}
```

Deletion marker example:

```json
{"id":"018f4f3f-6f9f-7a1b-8f55-1234567890ab","deleted":true,"atNs":1761736599000000000,"data":{"@type":"type.googleapis.com/github.com.fingon.proprdb.v1.example.Person"}}
```

## Local storage (SQLite backend)

The wire format is JSONL; local storage is implementation-defined.
This repository uses SQLite with one table per supported object type.

Each object table stores:

- `id` (`TEXT PRIMARY KEY`)
- `at_ns` (`INTEGER NOT NULL`)
- `data` (`BLOB NOT NULL`) as encoded `protobuf.Any`

`_deleted` table stores tombstones for sync-enabled tables:

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

Implementations may also project selected typed fields from `data` into
additional columns for queryability. Initialization owns only ProprDB core
tables and generated tables for message types supported by the current
application. Other SQLite tables are ignored.
