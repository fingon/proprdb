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

## Getting started

Prerequisites:

- Go `1.25+`

Commands:

```bash
make test
make lint
```
