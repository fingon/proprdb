# Hash projection encoding v1

The fingerprint is XXH3-64 with seed zero, stored as eight big-endian bytes.
All integers in the canonical input are unsigned little-endian unless stated
otherwise. Hash implementations must match the stable xxHash 0.8+ algorithm.

## Selection framing

The input begins with the five bytes `50 44 42 48 01` (`PDBH`, version 1), then
a uint32 selection count. Selections are sorted lexicographically by numeric
protobuf field-number path, with shorter prefixes first. Overlapping paths are
not allowed. Each selection contains:

1. A uint32 path length, followed by that many uint32 field numbers.
2. A uint32 count of present ancestor messages, stopping at the first absent
   ancestor. This distinguishes absence at different depths.
3. The selected field record, only if all ancestors are present.

## Field records

A field record contains a uint32 field number, a uint8 protobuf descriptor kind,
a uint8 mode, and a uint64 value count. Modes are 0 for ordinary singular
scalars, 1 for singular fields with presence, 2 for repeated fields, and 3 for
maps. Each value follows as a uint64 byte length and its encoded contents.

Ordinary singular scalars always have one value, including their default.
Fields with presence have zero values when absent and one when present.
Repeated fields and maps have their element count; empty collections have zero.
Inactive oneof fields have zero values; an active default has one. Unknown
fields never contribute records. Nested-message records include all known
fields, sorted by numeric field number.

## Value contents

| Kind | Encoding |
| --- | --- |
| bool | One byte, 0 or 1 |
| int32, sint32, sfixed32, enum | Four-byte two's-complement integer |
| uint32, fixed32 | Four-byte unsigned integer |
| int64, sint64, sfixed64 | Eight-byte two's-complement integer |
| uint64, fixed64 | Eight-byte unsigned integer |
| float, double | Four/eight IEEE-754 bytes, little-endian |
| string | UTF-8 bytes, without Unicode normalization |
| bytes | Raw bytes |
| message | Concatenated known-field records |

Zigzag values are decoded before encoding. Packed and unpacked repeated numeric
values are equivalent. Repeated order is preserved. All float NaNs use bits
`7fc00000`, all double NaNs use `7ff8000000000000`; both signs of zero use zero
bits. Other floating-point bits are preserved.

Maps encode each entry as a message with key field 1 and value field 2. Entries
are sorted lexicographically by their encoded entry bytes. The key record,
including its length frame, precedes the value and determines ordering for
unique keys. Map iteration order therefore has no effect. Fingerprints are
computed from valid typed messages serialized by supported runtimes; the format
does not define normalization for malformed wire data or duplicate map keys.
Known nested-message depth is limited to 100 and excess depth fails the write.

## Definition compatibility

Generated schema metadata records numeric selected paths, field kinds, modes,
reachable known-message layouts, and oneof membership. Message references form a finite graph,
including recursive types. A SHA-256 digest of this generator metadata appears
in the SQL projection signature alongside `xxh3-64-v1`; this digest is computed
at generation time, not per write. Definition changes trigger transactional
reprojection and tracking invalidation, even when a row's resulting hash is
unchanged. Unknown fields are preserved in stored payloads but excluded from
both the fingerprint input and known-field traversal.

Shared vectors live in `test/testdata/hash-projection-vectors.json`. Their hash
values were calculated with the official C XXH3 implementation, independently
of the Go, Swift, and Rust implementations. All three runtimes verify both the
canonical bytes and fingerprints.
