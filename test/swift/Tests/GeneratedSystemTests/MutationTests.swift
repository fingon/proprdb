@testable import GeneratedSystem
import Foundation
import ProprDBSwiftRuntime
import XCTest

private let mutationConsumer = "refresh"
private let ageWindow = "age"
private let updateAgeSQL = "UPDATE generatedtest_example_person SET age = ? WHERE id = ?"

private func ageState(_ db: any DBTX) throws -> DirtyWindowState {
    try readDirtyWindow(db, table: PersonTableName, window: ageWindow, consumer: mutationConsumer)
}

private func registerAge(_ db: any DBTX) throws {
    try registerDirtyWindow(db, table: PersonTableName, window: ageWindow, consumer: mutationConsumer)
    let state = try ageState(db)
    XCTAssertTrue(state.dirty && state.full)
    try acknowledgeDirtyWindow(db, token: state.token)
}

final class MutationTests: XCTestCase {
    func testReplacementsPreserveDisplacedBounds() throws {
        for recursive in [false, true] {
            let db = try SQLiteDatabase(path: ":memory:")
            let crud = CRUD(db)
            try crud.initialize()
            try db.execute("PRAGMA recursive_triggers = \(recursive)")
            try registerAge(db)
            var person = Generatedtest_Example_Person()
            person.name = "Ada"
            person.age = 10
            let row = try crud.person.insert(person)
            try acknowledgeDirtyWindow(db, token: ageState(db).token)
            try db.execute("INSERT OR REPLACE INTO generatedtest_example_person (id, at_ns, data, name, age) VALUES (?, 2, X'', 'Ada', 100)", arguments: [row.id])
            let state = try ageState(db)
            XCTAssertEqual(state.lower, 10)
            XCTAssertEqual(state.upper, 100)
            person.age = 1000
            let target = try crud.person.insert(person)
            try acknowledgeDirtyWindow(db, token: ageState(db).token)
            try db.execute("UPDATE OR REPLACE generatedtest_example_person SET id = ?, age = 200 WHERE id = ?", arguments: [target.id, row.id])
            let after = try ageState(db)
            XCTAssertEqual(after.lower, 100)
            XCTAssertEqual(after.upper, 1000)
        }
    }

    func testGoAndSwiftShareMutationState() throws {
        let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer {
            do { try FileManager.default.removeItem(at: path) }
            catch { XCTFail("remove temporary database: \(error)") }
        }
        let db = try SQLiteDatabase(path: path.path)
        let crud = CRUD(db)
        try crud.initialize()
        try registerAge(db)
        var person = Generatedtest_Example_Person()
        person.name = "Ada"
        person.age = 10
        let id = "01951d6e-a000-7000-8000-000000000001"
        _ = try crud.person.insertWithID(id, data: person)
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["go", "test", "-count=1", "-run", "^TestSwiftMutationInterop$", "."]
        let testsDirectory = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
        process.currentDirectoryURL = testsDirectory.appendingPathComponent("../../../system").standardizedFileURL
        var environment = ProcessInfo.processInfo.environment
        environment["PROPRDB_SWIFT_MUTATION_DB"] = path.path
        environment["CGO_ENABLED"] = "0"
        process.environment = environment
        let output = Pipe()
        process.standardOutput = output
        process.standardError = output
        try process.run()
        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        XCTAssertEqual(process.terminationStatus, 0, String(decoding: data, as: UTF8.self))
        XCTAssertEqual(try tableGenerations(db, tables: [PersonTableName])[PersonTableName], 2)
        let state = try ageState(db)
        XCTAssertEqual(state.lower, 10)
        XCTAssertEqual(state.upper, 41)
        try acknowledgeDirtyWindow(db, token: state.token)
    }

    func testRangesConsumersAndGenerations() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()
        try registerAge(db)
        try registerDirtyWindow(db, table: PersonTableName, window: ageWindow, consumer: "other")
        XCTAssertEqual(try tableGenerations(db, tables: [PersonTableName])[PersonTableName], 0)
        var person = Generatedtest_Example_Person()
        person.name = "Ada"
        person.age = 10
        let row = try crud.person.insert(person)
        let first = try ageState(db)
        XCTAssertEqual(first.lower, 10)
        XCTAssertEqual(first.upper, 10)
        for age in [Int64(30), -5, 20] {
            try db.execute(updateAgeSQL, arguments: [age, row.id])
        }
        let state = try ageState(db)
        XCTAssertEqual(state.lower, -5)
        XCTAssertEqual(state.upper, 30)
        XCTAssertThrowsError(try acknowledgeDirtyWindow(db, token: first.token)) { error in
            XCTAssertTrue(error is StaleDirtyWindowError)
        }
        try acknowledgeDirtyWindow(db, token: state.token)
        XCTAssertFalse(try ageState(db).dirty)
        XCTAssertTrue(try readDirtyWindow(db, table: PersonTableName, window: ageWindow, consumer: "other").dirty)
        XCTAssertEqual(try tableGenerations(db, tables: [PersonTableName])[PersonTableName], 4)
        try db.execute(updateAgeSQL, arguments: [Int64(20), row.id])
        XCTAssertEqual(try tableGenerations(db, tables: [PersonTableName])[PersonTableName], 4)
        try crud.person.deleteByID(row.id)
        let deleted = try ageState(db)
        XCTAssertEqual(deleted.lower, 20)
        XCTAssertEqual(deleted.upper, 20)
        try removeDirtyWindowConsumer(db, table: PersonTableName, window: ageWindow, consumer: mutationConsumer)
        try registerDirtyWindow(db, table: PersonTableName, window: ageWindow, consumer: mutationConsumer)
        XCTAssertThrowsError(try acknowledgeDirtyWindow(db, token: deleted.token))
    }

    func testRollbackOutputAcknowledgmentAndOverflow() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()
        try registerAge(db)
        var person = Generatedtest_Example_Person()
        person.name = "Ada"
        person.age = 10
        let row = try crud.person.insert(person)
        let state = try ageState(db)
        XCTAssertThrowsError(try db.withTransaction { transaction in
            try transaction.execute(updateAgeSQL, arguments: [Int64(100), row.id])
            throw ProprDBError("rollback write")
        })
        XCTAssertEqual(try tableGenerations(db, tables: [PersonTableName])[PersonTableName], 1)
        XCTAssertThrowsError(try db.withTransaction { transaction in
            var note = Generatedtest_Example_Note()
            note.text = "derived"
            _ = try NoteTable(transaction).insert(note)
            try acknowledgeDirtyWindow(transaction, token: state.token)
            throw ProprDBError("rollback refresh")
        })
        XCTAssertTrue(try ageState(db).dirty)
        XCTAssertEqual(try tableGenerations(db, tables: [NoteTableName])[NoteTableName], 0)
        try acknowledgeDirtyWindow(db, token: state.token)
        try db.execute("UPDATE _proprdb_generations SET generation = ? WHERE table_name = ?", arguments: [Int64.max, PersonTableName])
        XCTAssertThrowsError(try db.execute(updateAgeSQL, arguments: [Int64(100), row.id]))
    }

    func testMissingAndReversedBounds() throws {
        let cases: [(Int64?, Int64?, Bool)] = [(10, 20, false), (nil, 20, true), (30, 20, true)]
        for (start, end, full) in cases {
            let db = try SQLiteDatabase(path: ":memory:")
            let crud = CRUD(db)
            try crud.initialize()
            try registerDirtyWindow(db, table: PhotoTableName, window: "capture", consumer: mutationConsumer)
            let row = try crud.photo.insert(Generatedtest_Example_Photo())
            try db.execute("UPDATE generatedtest_example_photo SET exif_create_utc_time_seconds = ?, exif_modify_utc_time_seconds = ? WHERE id = ?", arguments: [start, end, row.id])
            let state = try readDirtyWindow(db, table: PhotoTableName, window: "capture", consumer: mutationConsumer)
            try acknowledgeDirtyWindow(db, token: state.token)
            try db.execute("UPDATE generatedtest_example_photo SET at_ns = at_ns + 1 WHERE id = ?", arguments: [row.id])
            let after = try readDirtyWindow(db, table: PhotoTableName, window: "capture", consumer: mutationConsumer)
            XCTAssertTrue(after.dirty)
            XCTAssertEqual(after.full, full)
        }
    }

    func testReopenPreservesTracking() throws {
        let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer {
            do { try FileManager.default.removeItem(at: path) }
            catch { XCTFail("remove temporary database: \(error)") }
        }
        do {
            let db = try SQLiteDatabase(path: path.path)
            let crud = CRUD(db)
            try crud.initialize()
            try registerAge(db)
            var person = Generatedtest_Example_Person()
            person.name = "Ada"
            person.age = 10
            _ = try crud.person.insert(person)
        }
        let db = try SQLiteDatabase(path: path.path)
        try CRUD(db).initialize()
        XCTAssertEqual(try tableGenerations(db, tables: [PersonTableName])[PersonTableName], 1)
        let state = try ageState(db)
        XCTAssertTrue(state.dirty && !state.full)
        XCTAssertEqual(state.lower, 10)
        XCTAssertEqual(state.upper, 10)
    }
}
