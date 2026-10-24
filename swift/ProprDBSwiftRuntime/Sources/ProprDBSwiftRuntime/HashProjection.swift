import Foundation
import SwiftProtobuf
import Synchronization
import XXH3

private let hashFormat: [UInt8] = [80, 68, 66, 72, 1]
private let hashMaxDepth = 100
private let hashSchemas = Mutex<[String: HashSchema]>([:])

private enum HashKind {
    static let double = 1
    static let float = 2
    static let int64 = 3
    static let uint64 = 4
    static let int32 = 5
    static let fixed64 = 6
    static let fixed32 = 7
    static let bool = 8
    static let string = 9
    static let group = 10
    static let message = 11
    static let bytes = 12
    static let uint32 = 13
    static let enumValue = 14
    static let sfixed32 = 15
    static let sfixed64 = 16
    static let sint32 = 17
    static let sint64 = 18
}

private struct HashSelection: Decodable, Sendable {
    let p: [Int]
    let f: [Int]
}
private struct HashSchema: Decodable, Sendable {
    let p: [HashSelection]
    let m: [[[Int]]]

    func validate() throws {
        guard !p.isEmpty else { throw ProprDBError("empty hash selections") }
        for path in p {
            guard !path.p.isEmpty, path.p.last == path.f.first else { throw ProprDBError("invalid hash path") }
        }
        for field in p.map(\.f) + m.flatMap({ $0 }) {
            guard field.count == 4, field[0] > 0, (HashKind.double...HashKind.sint64).contains(field[1]), field[1] != HashKind.group,
                  (0...3).contains(field[2]), field[1] != HashKind.message || m.indices.contains(field[3]) else {
                throw ProprDBError("invalid hash field descriptor")
            }
        }
    }

    func field(_ descriptor: [Int], raw: [HashWireValue], depth: Int) throws -> [UInt8] {
        guard depth <= hashMaxDepth else { throw ProprDBError("hash message nesting exceeds limit") }
        let kind = descriptor[1]
        let mode = descriptor[2]
        var raw = raw
        if mode == 2 && ![HashKind.string, HashKind.message, HashKind.bytes].contains(kind) {
            var expanded: [HashWireValue] = []
            for value in raw {
                if value.wire != 2 { expanded.append(value); continue }
                var reader = HashReader(bytes: value.bytes)
                while !reader.finished {
                    expanded.append(try reader.value(wire: hashWireType(kind)))
                }
            }
            raw = expanded
        }
        if mode < 2 {
            if raw.isEmpty && mode == 0 { raw = [HashWireValue(wire: hashWireType(kind))] }
            if raw.count > 1 {
                if kind == HashKind.message {
                    raw = [HashWireValue(wire: 2, bytes: raw.flatMap(\.bytes))]
                } else if let last = raw.last { raw = [last] }
            }
        }
        var values = try raw.map { try value(descriptor, raw: $0, depth: depth) }
        if mode == 3 { values.sort { $0.lexicographicallyPrecedes($1) } }
        var result = hashInteger(UInt32(descriptor[0])) + [UInt8(kind), UInt8(mode)] + hashInteger(UInt64(values.count))
        for bytes in values { result += hashFrame(bytes) }
        return result
    }

    func value(_ descriptor: [Int], raw: HashWireValue, depth: Int) throws -> [UInt8] {
        let kind = descriptor[1]
        guard raw.wire == hashWireType(kind) else { throw ProprDBError("invalid hash scalar wire type") }
        var value = raw.value
        switch kind {
        case HashKind.message:
            let fields = try hashParseWire(raw.bytes)
            var result: [UInt8] = []
            for child in m[descriptor[3]] {
                result += try field(child, raw: fields[child[0]] ?? [], depth: depth + 1)
            }
            return result
        case HashKind.string, HashKind.bytes: return raw.bytes
        case HashKind.double:
            let number = Double(bitPattern: value)
            if number.isNaN { value = 0x7ff8000000000000 } else if number == 0 { value = 0 }
        case HashKind.float:
            let number = Float(bitPattern: UInt32(truncatingIfNeeded: value))
            if number.isNaN { value = 0x7fc00000 } else if number == 0 { value = 0 }
        case HashKind.bool: value = value == 0 ? 0 : 1
        case HashKind.sint32, HashKind.sint64: value = (value >> 1) ^ (0 &- (value & 1))
        default: break
        }
        if [HashKind.float, HashKind.int32, HashKind.fixed32, HashKind.uint32, HashKind.enumValue, HashKind.sfixed32, HashKind.sint32].contains(kind) { return hashInteger(UInt32(truncatingIfNeeded: value)) }
        if kind == HashKind.bool { return [UInt8(value)] }
        return hashInteger(value)
    }
}

private func hashWireType(_ kind: Int) -> Int {
    switch kind {
    case HashKind.double, HashKind.fixed64, HashKind.sfixed64: return 1
    case HashKind.float, HashKind.fixed32, HashKind.sfixed32: return 5
    case HashKind.string, HashKind.message, HashKind.bytes: return 2
    default: return 0
    }
}

private struct HashWireValue {
    let wire: Int
    var value: UInt64 = 0
    var bytes: [UInt8] = []
}

private struct HashReader {
    let bytes: [UInt8]
    private(set) var offset = 0
    var finished: Bool { offset == bytes.count }

    mutating func take(_ count: Int) throws -> [UInt8] {
        guard count >= 0, count <= bytes.count - offset else { throw ProprDBError("truncated hash input") }
        let result = Array(bytes[offset..<(offset + count)])
        offset += count
        return result
    }

    mutating func varint() throws -> UInt64 {
        var value: UInt64 = 0
        for shift in stride(from: 0, to: 70, by: 7) {
            let byte = try take(1)[0]
            guard shift != 63 || byte < 2 else { throw ProprDBError("overflow in hash varint") }
            value |= UInt64(byte & 127) << shift
            if byte < 128 { return value }
        }
        throw ProprDBError("invalid hash varint")
    }

    mutating func value(wire: Int) throws -> HashWireValue {
        switch wire {
        case 0: return HashWireValue(wire: wire, value: try varint())
        case 1, 5:
            let bytes = try take(wire == 1 ? 8 : 4)
            var value: UInt64 = 0
            for (index, byte) in bytes.enumerated() { value |= UInt64(byte) << (index * 8) }
            return HashWireValue(wire: wire, value: value)
        case 2:
            let count = try varint()
            guard count <= UInt64(Int.max) else { throw ProprDBError("hash input length overflow") }
            return HashWireValue(wire: wire, bytes: try take(Int(count)))
        default: throw ProprDBError("invalid hash wire type")
        }
    }

    mutating func skipGroup(number: Int, depth: Int) throws {
        guard depth <= hashMaxDepth else { throw ProprDBError("hash group nesting exceeds limit") }
        while !finished {
            let tag = try varint()
            let field = Int(tag >> 3)
            let wire = Int(tag & 7)
            if wire == 4 {
                guard field == number else { throw ProprDBError("mismatched hash group") }
                return
            }
            if wire == 3 { try skipGroup(number: field, depth: depth + 1) } else { _ = try value(wire: wire) }
        }
        throw ProprDBError("unterminated hash group")
    }
}

private func hashParseWire(_ bytes: [UInt8]) throws -> [Int: [HashWireValue]] {
    var reader = HashReader(bytes: bytes)
    var fields: [Int: [HashWireValue]] = [:]
    while !reader.finished {
        let tag = try reader.varint()
        let number = Int(tag >> 3)
        let wire = Int(tag & 7)
        guard number > 0, number < (1 << 29) else { throw ProprDBError("invalid hash field number") }
        if wire == 3 { try reader.skipGroup(number: number, depth: 0) } else {
            fields[number, default: []].append(try reader.value(wire: wire))
        }
    }
    return fields
}

private func hashInteger<T: FixedWidthInteger>(_ value: T) -> [UInt8] {
    withUnsafeBytes(of: value.littleEndian) { Array($0) }
}
private func hashFrame(_ bytes: [UInt8]) -> [UInt8] { hashInteger(UInt64(bytes.count)) + bytes }

public func canonicalHashBytes(_ data: Data, schema definition: String) throws -> Data {
    let schema = try hashSchemas.withLock { cache in
        if let schema = cache[definition] { return schema }
        let schema = try JSONDecoder().decode(HashSchema.self, from: Data(definition.utf8))
        try schema.validate()
        cache[definition] = schema
        return schema
    }
    var result = hashFormat + hashInteger(UInt32(schema.p.count))
    for selection in schema.p {
        result += hashInteger(UInt32(selection.p.count))
        for number in selection.p { result += hashInteger(UInt32(number)) }
        var current = Array(data)
        var depth = 0
        for number in selection.p.dropLast() {
            let fields = try hashParseWire(current)
            guard let values = fields[number], !values.isEmpty else { break }
            guard values.allSatisfy({ $0.wire == 2 }) else { throw ProprDBError("invalid hash message wire type") }
            current = values.flatMap(\.bytes)
            depth += 1
        }
        result += hashInteger(UInt32(depth))
        if depth == selection.p.count - 1 {
            let fields = try hashParseWire(current)
            result += try schema.field(selection.f, raw: fields[selection.f[0]] ?? [], depth: 0)
        }
    }
    return Data(result)
}

public func projectHash<M: SwiftProtobuf.Message>(_ message: M, schema: String) throws -> Data {
    let canonical = try canonicalHashBytes(message.serializedData(), schema: schema)
    let fingerprint = XXH3.hash(Array(canonical))
    return withUnsafeBytes(of: fingerprint.bigEndian) { Data($0) }
}
