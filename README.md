# PROPRDB (PROtobuf PackRat DataBase)

`proprdb` is the third iteration of a "personal stuff database" concept.

- `prdb` (2014-2019, Python, still in personal use, not open source)
- `sdb` (used in https://timeatlaslabs.com since 2024, not open source)

## Caveat emptor

This software is primarily for my own use and due to that there is not much of a
compatibility guarantee across versions. The SQLite database should be
backwards (but not forwards) compatible, and JSONL format should be constant,
but the generated code (and runtime it refers to) may change drastically even
in minor versions. Patch versions should be backwards compatible, and I don't
think this will ever have major version different than zero as I hobby projects
of mine don't roll that way.

## Documentation

- [Design and data model](doc/design.md)
- [Runtime behavior](doc/runtime.md)
- [Protobuf options and code generation](doc/code-generation.md)

Scalar fields and explicitly selected nested message paths can be projected
into indexed SQL columns in Go, Swift, and Rust. Missing parents remain SQL NULL;
initialization backfills existing objects without changing their protobuf
payloads or sync timestamps. See [projection options](doc/code-generation.md).

Named hash projections fingerprint selected protobuf fields, including complete
nested messages, repeated values, and maps, without exposing their contents as
SQL columns. Dirty-key trackers can watch these eight-byte XXH3-64 fingerprints
to skip metadata-only edits. See [hash projections](doc/code-generation.md#hash-projections).

## Getting started

Prerequisites:

- Go `1.27+`
- SQLite `3.35+` for projection removal
- Rust (edition 2024), Cargo, and `protoc` for the Rust target
- Swift `6.2+` on macOS; Swift `6.2+` or Podman on Linux
- SQLite development headers when using native Swift on Linux

The Makefile routes Swift tools through `scripts/swift-tool`. It uses an
installed toolchain when available and otherwise builds and runs a Podman image
on Linux. The image includes Swift, SQLite development headers, and Go for the
interoperability tests. Container build artifacts and caches persist in named
volumes per checkout, separately from native `.build` artifacts. Native command
failures are reported directly; they do not trigger a container retry.

The wrapper also supports direct commands, for example
`./scripts/swift-tool swift --version` or `./scripts/swift-tool swiftc --version`.
Protobuf generation uses `scripts/protoc-gen-swift` through the same wrapper.

Commands:

```bash
make test
make lint
```

Rust bindings use Prost and the runtime in `rt/rust`. Build the plugin with
`make protoc-gen-proprdb-rust` and run its integration tests with
`make rust-test`. See [Rust code generation](doc/code-generation.md#rust-target)
for usage and the current feature coverage.
