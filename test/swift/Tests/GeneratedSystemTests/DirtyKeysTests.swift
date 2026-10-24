@testable import GeneratedSystem
import Foundation
import ProprDBSwiftRuntime
import XCTest

private let keysMemoryPath = ":memory:"
private let keysRemote = "peer"
private let keysConsumer = "refresh"
private let keysOtherConsumer = "other"
private let keysAge = "age"
private let keysAll = "all"
private let keysUpdateAgeSQL = "UPDATE generatedtest_example_person SET age = ? WHERE id = ?"
private let keysFirstID = "01951d6e-a000-7000-8000-000000000001"
private let keysSecondID = "01951d6e-a000-7000-8000-000000000002"

private func keysState(_ db: any DBTX, tracker: String = keysAge) throws -> DirtyKeysState {
    try readDirtyKeys(db, table: PersonTableName, tracker: tracker, consumer: keysConsumer)
}

private func registerKeys(_ db: any DBTX, tracker: String = keysAge) throws {
    try registerDirtyKeys(db, table: PersonTableName, tracker: tracker, consumer: keysConsumer)
    let state = try keysState(db, tracker: tracker)
    XCTAssertTrue(state.dirty && state.full && state.ids.isEmpty)
    try registerDirtyKeys(db, table: PersonTableName, tracker: tracker, consumer: keysConsumer)
    try acknowledgeDirtyKeys(db, token: state.token)
}

private func keysPerson() -> Generatedtest_Example_Person {
    var person = Generatedtest_Example_Person()
    person.name = "Ada"
    person.age = 10
    return person
}

final class DirtyKeysTests: XCTestCase {
    func testConsumersRepeatedMutationsAndDeletionReinsertion() throws {
        let db = try SQLiteDatabase(path: keysMemoryPath)
        let crud = CRUD(db)
        try crud.initialize()
        try registerKeys(db)
        try registerKeys(db, tracker: keysAll)
        try registerDirtyKeys(db, table: PersonTableName, tracker: keysAge, consumer: keysOtherConsumer)
        let row = try crud.person.insert(keysPerson())
        let first = try keysState(db)
        XCTAssertEqual(first.ids, [row.id])
        XCTAssertTrue(first.dirty && !first.full)
        for age in [Int64(20), 30, 10] { try db.execute(keysUpdateAgeSQL, arguments: [age, row.id]) }
        XCTAssertThrowsError(try acknowledgeDirtyKeys(db, token: first.token)) { error in
            XCTAssertTrue(error is StaleDirtyKeysError)
        }
        let state = try keysState(db)
        XCTAssertEqual(state.ids, [row.id])
        try acknowledgeDirtyKeys(db, token: state.token)
        XCTAssertThrowsError(try acknowledgeDirtyKeys(db, token: state.token))
        XCTAssertTrue(try readDirtyKeys(db, table: PersonTableName, tracker: keysAge, consumer: keysOtherConsumer).dirty)
        XCTAssertTrue(try keysState(db, tracker: keysAll).dirty)
        let clean = try keysState(db)
        XCTAssertFalse(clean.dirty || clean.full)
        XCTAssertTrue(clean.ids.isEmpty)
        try db.execute("UPDATE generatedtest_example_person SET name = 'Grace' WHERE id = ?", arguments: [row.id])
        try db.execute(keysUpdateAgeSQL, arguments: [Int64(10), row.id])
        try acknowledgeDirtyKeys(db, token: clean.token)
        try crud.person.deleteByID(row.id)
        let deleted = try keysState(db)
        XCTAssertEqual(deleted.ids, [row.id])
        _ = try crud.person.insertWithID(row.id, data: keysPerson())
        XCTAssertThrowsError(try acknowledgeDirtyKeys(db, token: deleted.token))
        XCTAssertEqual(try keysState(db).ids, [row.id])
        try removeDirtyKeysConsumer(db, table: PersonTableName, tracker: keysAge, consumer: keysConsumer)
        try registerDirtyKeys(db, table: PersonTableName, tracker: keysAge, consumer: keysConsumer)
        XCTAssertThrowsError(try acknowledgeDirtyKeys(db, token: deleted.token))
        XCTAssertTrue(try keysState(db).full)
    }

    func testReplacementsCaptureBothIDsWithoutRecursiveTriggers() throws {
        for recursive in [false, true] {
            let cases: [(String, [String])] = [
                ("INSERT OR REPLACE INTO generatedtest_example_person (id, at_ns, data, name, age) SELECT id, at_ns, data, name, age FROM generatedtest_example_person WHERE id = ?", [keysFirstID]),
                ("UPDATE OR REPLACE generatedtest_example_person SET id = '\(keysSecondID)' WHERE id = ?", [keysFirstID, keysSecondID]),
            ]
            for (query, expected) in cases {
                let db = try SQLiteDatabase(path: keysMemoryPath)
                let crud = CRUD(db)
                try crud.initialize()
                try db.execute("PRAGMA recursive_triggers = \(recursive)")
                for id in [keysFirstID, keysSecondID] { _ = try crud.person.insertWithID(id, data: keysPerson()) }
                try registerKeys(db)
                try db.execute(query, arguments: [keysFirstID])
                XCTAssertEqual(try keysState(db).ids, expected)
            }
        }
    }

    func testRollbackOutputsAcknowledgmentSavepointAndOverflow() throws {
        let db = try SQLiteDatabase(path: keysMemoryPath)
        let crud = CRUD(db)
        try crud.initialize()
        try registerKeys(db)
        let row = try crud.person.insert(keysPerson())
        let state = try keysState(db)
        XCTAssertThrowsError(try db.withTransaction { transaction in
            try registerDirtyKeys(transaction, table: PersonTableName, tracker: keysAge, consumer: keysOtherConsumer)
            try transaction.withTransaction { inner in try acknowledgeDirtyKeys(inner, token: state.token) }
            var note = Generatedtest_Example_Note()
            note.text = "derived"
            _ = try NoteTable(transaction).insert(note)
            throw ProprDBError("rollback refresh")
        })
        XCTAssertThrowsError(try readDirtyKeys(db, table: PersonTableName, tracker: keysAge, consumer: keysOtherConsumer))
        XCTAssertEqual(try keysState(db).ids, [row.id])
        XCTAssertEqual(try tableGenerations(db, tables: [NoteTableName])[NoteTableName], 0)
        try db.withTransaction { transaction in
            XCTAssertThrowsError(try transaction.withTransaction { inner in
                try inner.execute(keysUpdateAgeSQL, arguments: [Int64(20), row.id])
                throw ProprDBError("rollback mutation")
            })
            try acknowledgeDirtyKeys(transaction, token: state.token)
        }
        XCTAssertFalse(try keysState(db).dirty)
        try db.execute("UPDATE _proprdb_dirty_key_consumers SET revision = ? WHERE table_name = ? AND tracker_name = ? AND consumer = ?", arguments: [Int64.max, PersonTableName, keysAge, keysConsumer])
        XCTAssertThrowsError(try db.execute(keysUpdateAgeSQL, arguments: [Int64(20), row.id]))
        XCTAssertFalse(try keysState(db).dirty)
    }

    func testSynchronizationAndUnknownDrain() throws {
        let testsDirectory = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        let fixture = testsDirectory.appendingPathComponent("../../../system/testdata/mutation-import.jsonl").standardizedFileURL
        let input = try String(contentsOf: fixture, encoding: .utf8)
        for drain in [false, true] {
            let db = try SQLiteDatabase(path: keysMemoryPath)
            let crud = CRUD(db)
            try crud.initialize()
            try registerKeys(db)
            if drain {
                try readBoundJSONL(db, bindings: [], remote: keysRemote, text: input)
                XCTAssertFalse(try keysState(db).dirty)
                try crud.initialize()
            } else { try crud.readJSONL(remote: keysRemote, text: input) }
            let state = try keysState(db)
            XCTAssertEqual(state.ids, [keysFirstID, keysSecondID])
            try acknowledgeDirtyKeys(db, token: state.token)
            try crud.readJSONL(remote: keysRemote, text: input)
            XCTAssertFalse(try keysState(db).dirty)
        }
    }

    func testNestedNullableFieldsAndUnsyncedTable() throws {
        let db = try SQLiteDatabase(path: keysMemoryPath)
        let crud = CRUD(db)
        try crud.initialize()
        let tracker = "location"
        try registerDirtyKeys(db, table: PhotoTableName, tracker: tracker, consumer: keysConsumer)
        let photo = try crud.photo.insert(Generatedtest_Example_Photo())
        let initial = try readDirtyKeys(db, table: PhotoTableName, tracker: tracker, consumer: keysConsumer)
        try acknowledgeDirtyKeys(db, token: initial.token)
        for value: Int64? in [42, nil] {
            try db.execute("UPDATE generatedtest_example_photo SET location_altitude = ? WHERE id = ?", arguments: [value, photo.id])
            let state = try readDirtyKeys(db, table: PhotoTableName, tracker: tracker, consumer: keysConsumer)
            XCTAssertEqual(state.ids, [photo.id])
            try acknowledgeDirtyKeys(db, token: state.token)
        }
        try registerDirtyKeys(db, table: NoteTableName, tracker: keysAll, consumer: keysConsumer)
        let note = try crud.note.insert(Generatedtest_Example_Note())
        try crud.note.deleteByID(note.id)
        XCTAssertEqual(try readDirtyKeys(db, table: NoteTableName, tracker: keysAll, consumer: keysConsumer).ids, [note.id])
    }

    func testReconciliationInvalidatesTokens() throws {
        let db = try SQLiteDatabase(path: keysMemoryPath)
        let crud = CRUD(db)
        try crud.initialize()
        try registerKeys(db)
        let row = try crud.person.insert(keysPerson())
        let before = try keysState(db)
        try crud.initialize()
        XCTAssertEqual(try keysState(db).ids, [row.id])
        try acknowledgeDirtyKeys(db, token: before.token)
        let clean = try keysState(db)
        try db.execute("UPDATE _proprdb_generations SET signature = 'v2|legacy' WHERE table_name = ?", arguments: [PersonTableName])
        try crud.initialize()
        XCTAssertTrue(try keysState(db).full)
        XCTAssertThrowsError(try acknowledgeDirtyKeys(db, token: clean.token)) { error in
            XCTAssertTrue(error is StaleDirtyKeysError)
        }
        try acknowledgeDirtyKeys(db, token: keysState(db).token)
        try db.execute(keysUpdateAgeSQL, arguments: [Int64(20), row.id])
        XCTAssertEqual(try keysState(db).ids, [row.id])
    }

    func testConcurrentAcknowledgmentAndReopen() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer {
            do { try FileManager.default.removeItem(at: directory) }
            catch { XCTFail("remove temporary database: \(error)") }
        }
        let path = directory.appendingPathComponent("keys.db").path
        let first = try SQLiteDatabase(path: path)
        try first.withRows("PRAGMA journal_mode = WAL") { rows in
            while try rows.next() != nil {}
        }
        let crud = CRUD(first)
        try crud.initialize()
        try registerKeys(first)
        let row = try crud.person.insert(keysPerson())
        let stale = try keysState(first)
        let second = try SQLiteDatabase(path: path)
        try CRUD(second).initialize()
        try second.execute(keysUpdateAgeSQL, arguments: [Int64(20), row.id])
        XCTAssertThrowsError(try acknowledgeDirtyKeys(first, token: stale.token)) { error in
            XCTAssertTrue(error is StaleDirtyKeysError)
        }
        let token = try keysState(first).token
        let tasks = (0..<2).map { _ in
            Task.detached {
                let db = try SQLiteDatabase(path: path)
                try db.withRows("PRAGMA busy_timeout = 10000") { rows in
                    while try rows.next() != nil {}
                }
                do {
                    try acknowledgeDirtyKeys(db, token: token)
                    return true
                } catch is StaleDirtyKeysError { return false }
            }
        }
        var successes = 0
        for task in tasks { if try await task.value { successes += 1 } }
        XCTAssertEqual(successes, 1)
        try second.execute(keysUpdateAgeSQL, arguments: [Int64(30), row.id])
        let reopened = try SQLiteDatabase(path: path)
        try CRUD(reopened).initialize()
        XCTAssertEqual(try keysState(reopened).ids, [row.id])
    }
}
