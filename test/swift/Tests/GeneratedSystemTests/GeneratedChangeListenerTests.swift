@testable import GeneratedSystem
import ProprDBSwiftRuntime
import XCTest

private let countTombstoneByIDSQL = "SELECT COUNT(*) FROM _deleted WHERE table_name = ? AND id = ?"

final class GeneratedChangeListenerTests: XCTestCase {
    func testDirectTableInstancesShareChanges() async throws {
        let database = try SQLiteDatabase(path: ":memory:")
        let observingTable = PersonTable(database)
        let writingTable = PersonTable(database)
        try observingTable.initialize()
        var changes = observingTable.changes().makeAsyncIterator()

        let inserted = try writingTable.insert(makePerson(name: "Direct", age: 1))

        guard case let .upsert(id, _, data) = await changes.next() else {
            return XCTFail("expected direct table insert change")
        }
        XCTAssertEqual(id, inserted.id)
        XCTAssertEqual(data.name, "Direct")
    }

    func testLocalChangesThroughSynchronousAPI() async throws {
        let database = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(database)
        try crud.initialize()
        var changes = crud.person.changes().makeAsyncIterator()

        let inserted = try crud.person.insert(makePerson(name: "Ada", age: 37))
        guard case let .upsert(id, atNs, data) = await changes.next() else {
            return XCTFail("expected insert change")
        }
        XCTAssertEqual(id, inserted.id)
        XCTAssertEqual(atNs, inserted.atNs)
        XCTAssertEqual(data.name, "Ada")

        try crud.person.deleteByID(inserted.id)
        guard case let .delete(deletedID, deletedAtNs) = await changes.next() else {
            return XCTFail("expected delete change")
        }
        XCTAssertEqual(deletedID, inserted.id)
        XCTAssertGreaterThan(deletedAtNs, inserted.atNs)

        var noteChanges = crud.note.changes().makeAsyncIterator()
        let insertedNote = try crud.note.insert(makeNote(text: "Local only"))
        guard case let .upsert(noteID, noteAtNs, _) = await noteChanges.next() else {
            return XCTFail("expected note insert change")
        }
        XCTAssertEqual(noteID, insertedNote.id)
        XCTAssertEqual(noteAtNs, insertedNote.atNs)

        try crud.note.deleteByID(insertedNote.id)
        guard case let .delete(deletedNoteID, deletedNoteAtNs) = await noteChanges.next() else {
            return XCTFail("expected note delete change")
        }
        XCTAssertEqual(deletedNoteID, insertedNote.id)
        XCTAssertGreaterThan(deletedNoteAtNs, insertedNote.atNs)
        XCTAssertEqual(try crud.note.select(where: "id = ?", arguments: [.string(insertedNote.id)]).count, 0)
        XCTAssertEqual(try scalarInt(database, sql: countTombstoneByIDSQL, arguments: [NoteTableName, insertedNote.id]), 0)
    }

    func testActorProxyAPI() async throws {
        let databaseURL = FileManager.default.temporaryDirectory
            .appendingPathComponent("proprdb-\(UUID().uuidString).sqlite")
        let actor = try ProprDBActor(path: databaseURL.path)
        defer {
            try? FileManager.default.removeItem(at: databaseURL)
        }

        try await actor.initialize()
        var changes = try await actor.person.changes().makeAsyncIterator()
        let inserted = try await actor.person.insert(makePerson(name: "Grace", age: 42))
        guard case let .upsert(id, _, data) = await changes.next() else {
            return XCTFail("expected actor insert change")
        }
        XCTAssertEqual(id, inserted.id)
        XCTAssertEqual(data.name, "Grace")
        let selected = try await actor.person.select(where: "id = ?", arguments: [.string(inserted.id)])
        XCTAssertEqual(selected.count, 1)
        try await actor.close()
    }
}
