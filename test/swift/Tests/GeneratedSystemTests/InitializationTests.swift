@testable import GeneratedSystem
import CSQLite
import Foundation
import ProprDBSwiftRuntime
import XCTest

private let legacyObjectID = "legacy-id"
private let personName = "Ada"
private let totalChangesSQL = "SELECT total_changes()"

private final class InitializationTrace {
    var statements: [String] = []
}

func recordInitializationSQL(_ db: SQLiteDatabase, action: () throws -> Void) throws -> [String] {
    let trace = InitializationTrace()
    let status = sqlite3_trace_v2(db.sqliteHandle, UInt32(SQLITE_TRACE_STMT), { _, context, _, sql in
        guard let context, let sql else { return 0 }
        let trace = Unmanaged<InitializationTrace>.fromOpaque(context).takeUnretainedValue()
        trace.statements.append(String(cString: sql.assumingMemoryBound(to: CChar.self)))
        return 0
    }, Unmanaged.passUnretained(trace).toOpaque())
    guard status == SQLITE_OK else { throw ProprDBError("install initialization SQL trace: status=\(status)") }
    defer {
        XCTAssertEqual(sqlite3_trace_v2(db.sqliteHandle, 0, nil, nil), SQLITE_OK)
        withExtendedLifetime(trace) {}
    }
    try action()
    return trace.statements
}

final class InitializationTests: XCTestCase {
    func testInitializationAndReadsTrustStoredData() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()
        let row = try crud.person.insert(makePerson(name: personName))
        try db.execute("UPDATE \(quoteSQLiteIdentifier(PersonTableName)) SET id = ?, data = ? WHERE id = ?", arguments: [legacyObjectID, Data(), row.id])
        try db.execute("INSERT INTO _deleted (table_name, id, at_ns) VALUES (?, ?, ?)", arguments: [PersonTableName, legacyObjectID, 1])
        try crud.person.initialize()
        try crud.initialize()
        let stored = try XCTUnwrap(crud.person.select(where: "id = ?", arguments: [.string(legacyObjectID)]).first)
        XCTAssertEqual(stored.id, legacyObjectID)
        XCTAssertEqual(stored.data.name, "")
        XCTAssertThrowsError(try crud.person.insertWithID(legacyObjectID, data: makePerson(name: personName)))
        try db.execute("UPDATE _proprdb_schema SET schema_hash = 'name:string' WHERE table_name = ?", arguments: [PersonTableName])
        try crud.person.initialize()
        try db.execute("UPDATE \(quoteSQLiteIdentifier(PersonTableName)) SET data = ? WHERE id = ?", arguments: [Data([0xff]), legacyObjectID])
        try crud.initialize()
    }

    func testUnchangedInitializationOnlyInspectsMetadata() throws {
        for fullInit in [false, true] {
            let db = try SQLiteDatabase(path: ":memory:")
            let crud = CRUD(db)
            try crud.initialize()
            let before = try scalarInt(db, sql: totalChangesSQL, arguments: [])
            let statements = try recordInitializationSQL(db) {
                if fullInit { try crud.initialize() } else { try crud.person.initialize() }
            }
            XCTAssertEqual(try scalarInt(db, sql: totalChangesSQL, arguments: []), before)
            XCTAssertEqual(statements.filter { $0.hasPrefix("CREATE TABLE IF NOT EXISTS \(_deletedTableName) ") }.count, 1)
            XCTAssertEqual(statements.filter { $0.hasPrefix("SELECT id, at_ns, deleted, data_json FROM \(_unknownTypesTableName) WHERE type_name = ?") }.count, fullInit ? 3 : 1)
            for statement in statements {
                for prefix in ["INSERT ", "UPDATE ", "CREATE INDEX ", "DROP INDEX "] {
                    XCTAssertFalse(statement.hasPrefix(prefix), statement)
                }
                if statement.hasPrefix("SELECT "), !statement.contains("pragma_") {
                    XCTAssertTrue(statement.contains(" WHERE "), statement)
                }
            }
        }
    }
}
