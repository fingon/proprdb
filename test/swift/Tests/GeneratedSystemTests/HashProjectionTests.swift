@testable import GeneratedSystem
import Foundation
import ProprDBSwiftRuntime
import SwiftProtobuf
import XCTest

private let hashMemoryPath = ":memory:"
private let hashTracker = "support"
private let hashConsumer = "refresh"
private let hashSource = "source"

private struct HashVector: Decodable {
    let name: String
    let schema: String
    let wire: String
    let canonical: String
    let hash: String
}

private func hashDecodeHex(_ value: String) throws -> Data {
    var bytes: [UInt8] = []
    var index = value.startIndex
    while index < value.endIndex {
        let end = value.index(index, offsetBy: 2)
        let byte = try XCTUnwrap(UInt8(value[index..<end], radix: 16))
        bytes.append(byte)
        index = end
    }
    return Data(bytes)
}

final class HashProjectionTests: XCTestCase {
    func testSharedVectors() throws {
        let file = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
            .appendingPathComponent("../../../testdata/hash-projection-vectors.json").standardizedFileURL
        let vectors = try JSONDecoder().decode([HashVector].self, from: Data(contentsOf: file))
        for vector in vectors {
            let wire = try hashDecodeHex(vector.wire)
            XCTAssertEqual(try canonicalHashBytes(wire, schema: vector.schema), try hashDecodeHex(vector.canonical), vector.name)
            let message = try Google_Protobuf_Empty(serializedBytes: wire)
            XCTAssertEqual(try projectHash(message, schema: vector.schema), try hashDecodeHex(vector.hash), vector.name)
        }
    }

    func testTrackedWrites() throws {
        let db = try SQLiteDatabase(path: hashMemoryPath)
        let crud = CRUD(db)
        try crud.initialize()
        let table = HashRecordTableName
        let tracker = hashTracker
        let consumer = hashConsumer
        try registerDirtyKeys(db, table: table, tracker: tracker, consumer: consumer)
        var state = try readDirtyKeys(db, table: table, tracker: tracker, consumer: consumer)
        try acknowledgeDirtyKeys(db, token: state.token)
        var data = Generatedtest_Example_HashRecord()
        data.detail.name = "first"
        let row = try crud.hashrecord.insert(data)
        state = try readDirtyKeys(db, table: table, tracker: tracker, consumer: consumer)
        XCTAssertEqual(state.ids, [row.id])
        try acknowledgeDirtyKeys(db, token: state.token)
        data.label = "metadata"
        _ = try crud.hashrecord.updateByID(row.id, data: data)
        XCTAssertFalse(try readDirtyKeys(db, table: table, tracker: tracker, consumer: consumer).dirty)
        data.detail.name = "second"
        _ = try crud.hashrecord.updateByID(row.id, data: data)
        state = try readDirtyKeys(db, table: table, tracker: tracker, consumer: consumer)
        XCTAssertEqual(state.ids, [row.id])
        try crud.hashrecord.deleteByID(row.id)
        XCTAssertThrowsError(try acknowledgeDirtyKeys(db, token: state.token))
        XCTAssertEqual(try readDirtyKeys(db, table: table, tracker: tracker, consumer: consumer).ids, [row.id])
    }
    func testBackfillPreservesPayloadAndTimestamp() throws {
        let db = try SQLiteDatabase(path: hashMemoryPath)
        try ensureCoreTables(db)
        try db.execute("CREATE TABLE generatedtest_example_hashrecord (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL, label TEXT NOT NULL DEFAULT '')")
        var data = Generatedtest_Example_HashRecord()
        data.detail.name = "existing"
        var payload = try data.serializedData()
        payload.append(contentsOf: [0xa0, 0x06, 0x01])
        let id = "01951d6e-a000-7000-8000-000000000001"
        try db.execute("INSERT INTO generatedtest_example_hashrecord (id, at_ns, data, label) VALUES (?, 7, ?, '')", arguments: [id, payload])
        let table = HashRecordTable(db)
        try table.initialize()
        let managed = try table.insert(data)
        let expected = try db.withRows("SELECT _proprdb_hash_support FROM generatedtest_example_hashrecord WHERE id = ?", arguments: [managed.id]) { rows in
            try XCTUnwrap(rows.next()).data(at: 0)
        }
        try db.withRows("SELECT data, at_ns, _proprdb_hash_support FROM generatedtest_example_hashrecord WHERE id = ?", arguments: [id]) { rows in
            let row = try XCTUnwrap(rows.next())
            XCTAssertEqual(try row.data(at: 0), payload)
            XCTAssertEqual(try row.int64(at: 1), 7)
            XCTAssertEqual(try row.data(at: 2), expected)
        }
    }

    func testSyncAndRollback() throws {
        let sourceDB = try SQLiteDatabase(path: hashMemoryPath)
        let targetDB = try SQLiteDatabase(path: hashMemoryPath)
        let source = CRUD(sourceDB)
        let target = CRUD(targetDB)
        try source.initialize()
        try target.initialize()
        let row = try source.hashrecord.insert(Generatedtest_Example_HashRecord())
        try target.readJSONL(remote: hashSource, text: source.writeJSONL(remote: ""))
        let table = HashRecordTableName
        let tracker = hashTracker
        let consumer = hashConsumer
        try registerDirtyKeys(targetDB, table: table, tracker: tracker, consumer: consumer)
        let initial = try readDirtyKeys(targetDB, table: table, tracker: tracker, consumer: consumer)
        try acknowledgeDirtyKeys(targetDB, token: initial.token)
        var changed = row.data
        changed.enabled = true
        let transaction = try targetDB.beginTransaction()
        _ = try HashRecordTable(transaction).updateByID(row.id, data: changed)
        try transaction.rollback()
        XCTAssertFalse(try readDirtyKeys(targetDB, table: table, tracker: tracker, consumer: consumer).dirty)
        _ = try source.hashrecord.updateByID(row.id, data: changed)
        try target.readJSONL(remote: hashSource, text: source.writeJSONL(remote: ""))
        XCTAssertTrue(try readDirtyKeys(targetDB, table: table, tracker: tracker, consumer: consumer).dirty)
    }

}
