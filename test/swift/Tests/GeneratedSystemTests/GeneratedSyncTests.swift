@testable import GeneratedSystem
import ProprDBSwiftRuntime
import XCTest

private let testRemoteA = "remote-a"
private let testRemoteWS = "   "
private let testRemoteEmpty = ""
private let unknownTypeName = "generatedtest.example.UnknownThing"
private let unknownID = "018f4f3f-6f9f-7a1b-8f55-1234567890aa"
private let drainPersonID = "018f4f3f-6f9f-7a1b-8f55-1234567890ac"

final class GeneratedSyncTests: XCTestCase {
    func testGeneratedJSONLSync() throws {
        let sourceDB = try SQLiteDatabase(path: ":memory:")
        let targetDB = try SQLiteDatabase(path: ":memory:")

        let source = CRUD(sourceDB)
        try source.initialize()
        let target = CRUD(targetDB)
        try target.initialize()

        let personRow = try source.person.insert(makePerson(name: "Ada", age: 37))
        let noteRow = try source.note.insert(makeNote(text: "to be deleted"))
        try source.note.deleteByID(noteRow.id)

        let firstExport = try source.writeJSONL(remote: testRemoteA)
        XCTAssertEqual(firstExport.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: "\n").count, 1)

        XCTAssertEqual(try source.writeJSONL(remote: testRemoteA).trimmingCharacters(in: .whitespacesAndNewlines), "")

        try target.readJSONL(remote: testRemoteA, text: firstExport)
        let targetPeople = try target.person.select(where: "id = ?", arguments: [personRow.id])
        XCTAssertEqual(targetPeople.count, 1)
        XCTAssertEqual(targetPeople[0].data.name, "Ada")
        XCTAssertEqual(try scalarInt(targetDB, sql: "SELECT COUNT(*) FROM _sync WHERE remote = ?", arguments: [testRemoteA]), 1)

        let noteLine = "{\"id\":\(jsonString(noteRow.id)),\"atNs\":\(personRow.atNs + 10),\"data\":{\"@type\":\(jsonString(typeURL(NoteTypeName))),\"text\":\"ignored\"}}\n"
        try target.readJSONL(remote: testRemoteA, text: noteLine)
        XCTAssertEqual(try target.note.select(where: "id = ?", arguments: [noteRow.id]).count, 0)
        XCTAssertEqual(try scalarInt(targetDB, sql: "SELECT COUNT(*) FROM _sync WHERE object_id = ? AND table_name = ? AND remote = ?", arguments: [noteRow.id, NoteTableName, testRemoteA]), 0)

        _ = try source.person.updateByID(personRow.id, data: makePerson(name: "Ada Updated", age: 38))
        let thirdExport = try source.writeJSONL(remote: testRemoteA)
        XCTAssertEqual(thirdExport.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: "\n").count, 1)
        try target.readJSONL(remote: testRemoteA, text: thirdExport)
        XCTAssertEqual(try target.person.select(where: "id = ?", arguments: [personRow.id]).first?.data.name, "Ada Updated")

        let targetUpdatedAtNs = try XCTUnwrap(target.person.select(where: "id = ?", arguments: [personRow.id]).first?.atNs)
        let invalidByValidateLine = "{\"id\":\(jsonString(personRow.id)),\"atNs\":\(targetUpdatedAtNs + 1),\"data\":{\"@type\":\(jsonString(typeURL(PersonTypeName))),\"name\":\"\",\"age\":\"1\"}}\n"
        try target.readJSONL(remote: testRemoteA, text: invalidByValidateLine)
        XCTAssertEqual(try target.person.select(where: "id = ?", arguments: [personRow.id]).first?.data.name, "")

        let localNewer = try target.person.updateByID(personRow.id, data: makePerson(name: "Local Newer", age: 99))
        try target.readJSONL(remote: testRemoteA, text: "{\"id\":\(jsonString(personRow.id)),\"deleted\":true,\"atNs\":\(localNewer.atNs - 1),\"data\":{\"@type\":\(jsonString(typeURL(PersonTypeName)))}}\n")
        XCTAssertEqual(try target.person.select(where: "id = ?", arguments: [personRow.id]).first?.data.name, "Local Newer")

        let newerDeleteAtNs = localNewer.atNs + 1
        try target.readJSONL(remote: testRemoteA, text: "{\"id\":\(jsonString(personRow.id)),\"deleted\":true,\"atNs\":\(newerDeleteAtNs),\"data\":{\"@type\":\(jsonString(typeURL(PersonTypeName)))}}\n")
        XCTAssertEqual(try target.person.select(where: "id = ?", arguments: [personRow.id]).count, 0)
        XCTAssertEqual(try scalarInt(targetDB, sql: "SELECT at_ns FROM _deleted WHERE table_name = ? AND id = ?", arguments: [PersonTableName, personRow.id]), newerDeleteAtNs)
    }

    func testGeneratedJSONLEmptyRemoteNoSyncEntries() throws {
        let sourceDB = try SQLiteDatabase(path: ":memory:")
        let targetDB = try SQLiteDatabase(path: ":memory:")
        let source = CRUD(sourceDB)
        try source.initialize()
        let target = CRUD(targetDB)
        try target.initialize()

        let personRow = try source.person.insert(makePerson(name: "Empty Remote", age: 1))
        let firstExport = try source.writeJSONL(remote: testRemoteEmpty)
        XCTAssertFalse(firstExport.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
        XCTAssertEqual(try source.writeJSONL(remote: testRemoteEmpty).trimmingCharacters(in: .whitespacesAndNewlines), firstExport.trimmingCharacters(in: .whitespacesAndNewlines))

        try target.readJSONL(remote: testRemoteEmpty, text: firstExport)
        XCTAssertEqual(try target.person.select(where: "id = ?", arguments: [personRow.id]).count, 1)
        XCTAssertEqual(try scalarInt(sourceDB, sql: "SELECT COUNT(*) FROM _sync WHERE remote = ?", arguments: [testRemoteEmpty]), 0)
        XCTAssertEqual(try scalarInt(targetDB, sql: "SELECT COUNT(*) FROM _sync WHERE remote = ?", arguments: [testRemoteEmpty]), 0)

        let wsFirstExport = try source.writeJSONL(remote: testRemoteWS)
        XCTAssertFalse(wsFirstExport.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
        XCTAssertEqual(try source.writeJSONL(remote: testRemoteWS).trimmingCharacters(in: .whitespacesAndNewlines), "")
        try target.readJSONL(remote: testRemoteWS, text: wsFirstExport)
        XCTAssertEqual(try scalarInt(sourceDB, sql: "SELECT COUNT(*) FROM _sync WHERE remote = ?", arguments: [testRemoteWS]), 1)
        XCTAssertEqual(try scalarInt(targetDB, sql: "SELECT COUNT(*) FROM _sync WHERE remote = ?", arguments: [testRemoteWS]), 1)
    }

    func testGeneratedJSONLUnknownTypesAreCompacted() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()

        let firstLine = "{\"id\":\(jsonString(unknownID)),\"atNs\":10,\"data\":{\"@type\":\(jsonString(typeURL(unknownTypeName))),\"payload\":\"old\"}}\n"
        let secondLine = "{\"id\":\(jsonString(unknownID)),\"atNs\":20,\"data\":{\"@type\":\(jsonString(typeURL(unknownTypeName))),\"payload\":\"new\"}}\n"
        try crud.readJSONL(remote: testRemoteA, text: firstLine + secondLine)

        XCTAssertEqual(try scalarInt(db, sql: "SELECT COUNT(*) FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [unknownTypeName, unknownID]), 1)
        XCTAssertEqual(try scalarInt(db, sql: "SELECT at_ns FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [unknownTypeName, unknownID]), 20)
    }

    func testGeneratedInitDrainsUnknownRowsForKnownType() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()

        let personAnyJSON = "{\"@type\":\(jsonString(typeURL(PersonTypeName))),\"name\":\"Recovered\",\"age\":\"44\"}"
        try db.execute("INSERT INTO _unknown_types (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?)", arguments: [PersonTypeName, drainPersonID, Int64(77), 0, personAnyJSON])
        try crud.person.initialize()

        let recovered = try crud.person.select(where: "id = ?", arguments: [drainPersonID])
        XCTAssertEqual(recovered.count, 1)
        XCTAssertEqual(recovered[0].data.name, "Recovered")
        XCTAssertEqual(recovered[0].data.age, 44)
        XCTAssertEqual(try scalarInt(db, sql: "SELECT COUNT(*) FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [PersonTypeName, drainPersonID]), 0)
    }
}
