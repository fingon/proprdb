@testable import GeneratedSystem
import ProprDBSwiftRuntime
import XCTest

private let testRemoteA = "remote-a"
private let testRemoteWS = "   "
private let testRemoteEmpty = ""
private let unknownTypeName = "generatedtest.example.UnknownThing"
private let unknownID = "018f4f3f-6f9f-7a1b-8f55-1234567890aa"
private let drainPersonID = "018f4f3f-6f9f-7a1b-8f55-1234567890ac"

private final class FailingInputStream: InputStream, @unchecked Sendable {
    private let prefixBytes: [UInt8]
    private let failure = NSError(domain: "GeneratedSyncTests", code: 1)
    private var offsetBytes = 0
    private var failed = false
    private var status: Stream.Status = .notOpen

    init(prefix: String) {
        prefixBytes = Array(prefix.utf8)
        super.init(data: Data())
    }

    override var streamError: Error? {
        failed ? failure : nil
    }

    override var streamStatus: Stream.Status {
        status
    }

    override var hasBytesAvailable: Bool {
        !failed
    }

    override func open() {
        status = .open
    }

    override func close() {
        status = .closed
    }

    override func read(_ buffer: UnsafeMutablePointer<UInt8>, maxLength lengthBytes: Int) -> Int {
        if offsetBytes < prefixBytes.count {
            let readBytes = min(lengthBytes, prefixBytes.count - offsetBytes)
            for index in 0..<readBytes {
                buffer[index] = prefixBytes[offsetBytes + index]
            }
            offsetBytes += readBytes
            return readBytes
        }
        failed = true
        status = .error
        return -1
    }
}

final class GeneratedSyncTests: XCTestCase {
    func testEnsureCoreTablesMigratesLegacyUnknownRowsToLatestState() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        try db.execute("CREATE TABLE _unknown_types (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, deleted INTEGER NOT NULL, data_json TEXT NOT NULL, PRIMARY KEY (type_name, id, at_ns))")
        let insertSQL = "INSERT INTO _unknown_types (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?)"
        try db.execute(insertSQL, arguments: [unknownTypeName, unknownID, Int64(10), 0, "{\"value\":\"old\"}"])
        try db.execute(insertSQL, arguments: [unknownTypeName, unknownID, Int64(20), 0, "{\"value\":\"latest\"}"])

        try ensureCoreTables(db)

        XCTAssertEqual(try scalarInt(db, sql: "SELECT COUNT(*) FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [unknownTypeName, unknownID]), 1)
        XCTAssertEqual(try scalarInt(db, sql: "SELECT at_ns FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [unknownTypeName, unknownID]), 20)
        XCTAssertEqual(try scalarString(db, sql: "SELECT data_json FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [unknownTypeName, unknownID]), "{\"value\":\"latest\"}")
        XCTAssertThrowsError(try db.execute(insertSQL, arguments: [unknownTypeName, unknownID, Int64(30), 0, "{\"value\":\"duplicate\"}"]))
    }

    func testJSONLStreamHandlesChunkBoundariesAndPhysicalLines() throws {
        let payload = String(repeating: "x", count: 70_000)
        let line = "{\"id\":\(jsonString(unknownID)),\"atNs\":42,\"data\":{\"@type\":\(jsonString(typeURL(unknownTypeName))),\"payload\":\(jsonString(payload))}}"
        var records: [(JSONLRecord, Int)] = []

        try readJSONL(stream: jsonlInputStream("\r\n" + line)) { record, lineNumber in
            records.append((record, lineNumber))
        }

        XCTAssertEqual(records.count, 1)
        XCTAssertEqual(records[0].1, 2)
        XCTAssertGreaterThan(records[0].0.data.count, 64 * 1_024)
    }

    func testJSONLStreamFailurePreservesEarlierCommittedRecords() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()
        let line = "{\"id\":\(jsonString(unknownID)),\"atNs\":42,\"data\":{\"@type\":\(jsonString(typeURL(unknownTypeName)))}}\n"

        XCTAssertThrowsError(try crud.readJSONL(remote: testRemoteA, stream: FailingInputStream(prefix: line))) { error in
            XCTAssertTrue(String(describing: error).contains("GeneratedSyncTests"))
        }
        XCTAssertEqual(try scalarInt(db, sql: "SELECT COUNT(*) FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [unknownTypeName, unknownID]), 1)
    }

    func testJSONLStreamRejectsInvalidUTF8() {
        let stream = InputStream(data: Data([0xff, 0x0a]))
        XCTAssertThrowsError(try readJSONL(stream: stream) { _, _ in }) { error in
            XCTAssertTrue(String(describing: error).contains("line 1"))
            XCTAssertTrue(String(describing: error).contains("invalid utf8"))
        }
    }

    func testMarshalAnyJSONRoundTripsTypeAndPayload() throws {
        let encoded = try marshalAnyJSON(makePerson(name: "Ada", age: 37), typeName: PersonTypeName)
        XCTAssertEqual(try typeNameFromAnyJSON(encoded), PersonTypeName)
        let decoded = try decodeAnyJSON(encoded, as: Generatedtest_Example_Person.self)
        XCTAssertEqual(decoded, makePerson(name: "Ada", age: 37))
    }

    func testPreparedExportCheckpointLifecycle() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()
        _ = try crud.person.insert(makePerson(name: "Prepared", age: 1))

        let prepared = try crud.prepareJSONL(remote: testRemoteA)
        XCTAssertFalse(prepared.text.isEmpty)
        XCTAssertEqual(try JSONLCheckpoint.parse(prepared.checkpoint.serialized()), prepared.checkpoint)
        XCTAssertFalse(try crud.prepareJSONL(remote: testRemoteA).text.isEmpty)

        try crud.acknowledgeJSONL(prepared.checkpoint)
        try crud.acknowledgeJSONL(prepared.checkpoint)
        try crud.discardJSONL(prepared.checkpoint)
        XCTAssertEqual(try crud.writeJSONL(remote: testRemoteA), "")
    }

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

        try target.readJSONL(remote: testRemoteA, stream: jsonlInputStream(firstExport))
        let targetPeople = try target.person.select(where: "id = ?", arguments: [.string(personRow.id)])
        XCTAssertEqual(targetPeople.count, 1)
        XCTAssertEqual(targetPeople[0].data.name, "Ada")
        XCTAssertEqual(try scalarInt(targetDB, sql: "SELECT COUNT(*) FROM _sync WHERE remote = ?", arguments: [testRemoteA]), 1)

        let noteLine = "{\"id\":\(jsonString(noteRow.id)),\"atNs\":\(personRow.atNs + 10),\"data\":{\"@type\":\(jsonString(typeURL(NoteTypeName))),\"text\":\"ignored\"}}\n"
        try target.readJSONL(remote: testRemoteA, stream: jsonlInputStream(noteLine))
        XCTAssertEqual(try target.note.select(where: "id = ?", arguments: [.string(noteRow.id)]).count, 0)
        XCTAssertEqual(try scalarInt(targetDB, sql: "SELECT COUNT(*) FROM _sync WHERE object_id = ? AND table_name = ? AND remote = ?", arguments: [noteRow.id, NoteTableName, testRemoteA]), 0)

        _ = try source.person.updateByID(personRow.id, data: makePerson(name: "Ada Updated", age: 38))
        let thirdExport = try source.writeJSONL(remote: testRemoteA)
        XCTAssertEqual(thirdExport.trimmingCharacters(in: .whitespacesAndNewlines).split(separator: "\n").count, 1)
        try target.readJSONL(remote: testRemoteA, stream: jsonlInputStream(thirdExport))
        XCTAssertEqual(try target.person.select(where: "id = ?", arguments: [.string(personRow.id)]).first?.data.name, "Ada Updated")

        let targetUpdatedAtNs = try XCTUnwrap(target.person.select(where: "id = ?", arguments: [.string(personRow.id)]).first?.atNs)
        let invalidByValidateLine = "{\"id\":\(jsonString(personRow.id)),\"atNs\":\(targetUpdatedAtNs + 1),\"data\":{\"@type\":\(jsonString(typeURL(PersonTypeName))),\"name\":\"\",\"age\":\"1\"}}\n"
        try target.readJSONL(remote: testRemoteA, stream: jsonlInputStream(invalidByValidateLine))
        XCTAssertEqual(try target.person.select(where: "id = ?", arguments: [.string(personRow.id)]).first?.data.name, "")

        let localNewer = try target.person.updateByID(personRow.id, data: makePerson(name: "Local Newer", age: 99))
        try target.readJSONL(remote: testRemoteA, stream: jsonlInputStream("{\"id\":\(jsonString(personRow.id)),\"deleted\":true,\"atNs\":\(localNewer.atNs - 1),\"data\":{\"@type\":\(jsonString(typeURL(PersonTypeName)))}}\n"))
        XCTAssertEqual(try target.person.select(where: "id = ?", arguments: [.string(personRow.id)]).first?.data.name, "Local Newer")

        let newerDeleteAtNs = localNewer.atNs + 1
        try target.readJSONL(remote: testRemoteA, stream: jsonlInputStream("{\"id\":\(jsonString(personRow.id)),\"deleted\":true,\"atNs\":\(newerDeleteAtNs),\"data\":{\"@type\":\(jsonString(typeURL(PersonTypeName)))}}\n"))
        XCTAssertEqual(try target.person.select(where: "id = ?", arguments: [.string(personRow.id)]).count, 0)
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

        try target.readJSONL(remote: testRemoteEmpty, stream: jsonlInputStream(firstExport))
        XCTAssertEqual(try target.person.select(where: "id = ?", arguments: [.string(personRow.id)]).count, 1)
        XCTAssertEqual(try scalarInt(sourceDB, sql: "SELECT COUNT(*) FROM _sync WHERE remote = ?", arguments: [testRemoteEmpty]), 0)
        XCTAssertEqual(try scalarInt(targetDB, sql: "SELECT COUNT(*) FROM _sync WHERE remote = ?", arguments: [testRemoteEmpty]), 0)

        let wsFirstExport = try source.writeJSONL(remote: testRemoteWS)
        XCTAssertFalse(wsFirstExport.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
        XCTAssertEqual(try source.writeJSONL(remote: testRemoteWS).trimmingCharacters(in: .whitespacesAndNewlines), "")
        try target.readJSONL(remote: testRemoteWS, stream: jsonlInputStream(wsFirstExport))
        XCTAssertEqual(try scalarInt(sourceDB, sql: "SELECT COUNT(*) FROM _sync WHERE remote = ?", arguments: [testRemoteWS]), 1)
        XCTAssertEqual(try scalarInt(targetDB, sql: "SELECT COUNT(*) FROM _sync WHERE remote = ?", arguments: [testRemoteWS]), 1)
    }

    func testGeneratedJSONLUnknownTypesKeepLatestState() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()

        let firstLine = "{\"id\":\(jsonString(unknownID)),\"atNs\":10,\"data\":{\"@type\":\(jsonString(typeURL(unknownTypeName))),\"payload\":\"old\"}}\n"
        let secondLine = "{\"id\":\(jsonString(unknownID)),\"atNs\":20,\"data\":{\"@type\":\(jsonString(typeURL(unknownTypeName))),\"payload\":\"new\"}}\n"
        try crud.readJSONL(remote: testRemoteA, stream: jsonlInputStream(firstLine + secondLine))

        XCTAssertEqual(try scalarInt(db, sql: "SELECT COUNT(*) FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [unknownTypeName, unknownID]), 1)
        XCTAssertEqual(try scalarInt(db, sql: "SELECT at_ns FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [unknownTypeName, unknownID]), 20)
    }

    func testJSONLRejectsInvalidScalarCoercions() throws {
        let valid = "{\"id\":\(jsonString(unknownID)),\"atNs\":42,\"data\":{\"@type\":\(jsonString(typeURL(unknownTypeName)))}}\n"
        XCTAssertNoThrow(try readJSONL(stream: jsonlInputStream(valid)) { _, _ in })
        XCTAssertThrowsError(try readJSONL(stream: jsonlInputStream(valid.replacingOccurrences(of: "\"atNs\":42", with: "\"deleted\":1,\"atNs\":42"))) { _, _ in })
        XCTAssertThrowsError(try readJSONL(stream: jsonlInputStream(valid.replacingOccurrences(of: "\"atNs\":42", with: "\"atNs\":true"))) { _, _ in })
        XCTAssertThrowsError(try readJSONL(stream: jsonlInputStream(valid.replacingOccurrences(of: "\"atNs\":42", with: "\"atNs\":42.5"))) { _, _ in })
        XCTAssertThrowsError(try readJSONL(stream: jsonlInputStream(valid.replacingOccurrences(of: "\"data\":{\"@type\":\(jsonString(typeURL(unknownTypeName)))}", with: "\"data\":[]"))) { _, _ in })
    }

    func testKnownOmitSyncParkedRecordDoesNotExport() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()
        let dataJSON = String(decoding: try marshalTypeOnlyAnyJSON(typeName: NoteTypeName), as: UTF8.self)
        try db.execute(
            "INSERT INTO _unknown_types (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?)",
            arguments: [NoteTypeName, unknownID, Int64(1), 0, dataJSON]
        )

        XCTAssertEqual(try crud.writeJSONL(remote: ""), "")
        XCTAssertEqual(
            try scalarInt(db, sql: "SELECT COUNT(*) FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [NoteTypeName, unknownID]),
            1
        )
    }

    func testGeneratedInitDrainsUnknownRowsForKnownType() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()

        let personAnyJSON = "{\"@type\":\(jsonString(typeURL(PersonTypeName))),\"name\":\"Recovered\",\"age\":\"44\"}"
        try db.execute("INSERT INTO _unknown_types (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?)", arguments: [PersonTypeName, drainPersonID, Int64(77), 0, personAnyJSON])
        try crud.person.initialize()

        let recovered = try crud.person.select(where: "id = ?", arguments: [.string(drainPersonID)])
        XCTAssertEqual(recovered.count, 1)
        XCTAssertEqual(recovered[0].data.name, "Recovered")
        XCTAssertEqual(recovered[0].data.age, 44)
        XCTAssertEqual(try scalarInt(db, sql: "SELECT COUNT(*) FROM _unknown_types WHERE type_name = ? AND id = ?", arguments: [PersonTypeName, drainPersonID]), 0)
    }
}
