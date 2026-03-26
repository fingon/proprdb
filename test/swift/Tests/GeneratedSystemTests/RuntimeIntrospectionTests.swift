@testable import GeneratedSystem
import ProprDBSwiftRuntime
import XCTest

final class RuntimeIntrospectionTests: XCTestCase {
    func testRTIntrospectTablesUsesDataBlobWhenPresent() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        try ensureCoreTables(db)
        try db.execute("CREATE TABLE IF NOT EXISTS \"thing\" (\"id\" TEXT PRIMARY KEY, \"at_ns\" INTEGER NOT NULL, \"data\" BLOB NOT NULL)")
        try db.execute("INSERT INTO \"thing\" (\"id\", \"at_ns\", \"data\") VALUES ('a', 1, X'0102'), ('b', 2, X''), ('c', 3, X'ffffff')")

        let introspection = try introspectTables(db, descriptors: [
            GeneratedTableDescriptor(tableName: "thing", typeName: "example.Thing", isCore: false, syncEnabled: true),
        ])
        XCTAssertEqual(introspection.count, 1)
        XCTAssertEqual(introspection[0].descriptor.tableName, "thing")
        XCTAssertEqual(introspection[0].objectCount, 3)
        XCTAssertEqual(introspection[0].diskUsageBytes, 5)
    }

    func testRTIntrospectTablesFallbackForNoDataColumn() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        try ensureCoreTables(db)
        try db.execute("INSERT INTO \"_deleted\" (\"table_name\", \"id\", \"at_ns\") VALUES ('person', 'one', 123), ('note', 'two', 7)")

        let introspection = try introspectTables(db, descriptors: [
            GeneratedTableDescriptor(tableName: coreTableDeletedName, typeName: "", isCore: true, syncEnabled: false),
        ])
        XCTAssertEqual(introspection.count, 1)
        XCTAssertEqual(introspection[0].objectCount, 2)
    }

    func testRTIntrospectTablesEmptyTableReturnsZero() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        try db.execute("CREATE TABLE IF NOT EXISTS \"thing\" (\"id\" TEXT PRIMARY KEY, \"at_ns\" INTEGER NOT NULL, \"data\" BLOB NOT NULL)")
        let introspection = try introspectTables(db, descriptors: [
            GeneratedTableDescriptor(tableName: "thing", typeName: "example.Thing", isCore: false, syncEnabled: true),
        ])
        XCTAssertEqual(introspection[0].objectCount, 0)
        XCTAssertEqual(introspection[0].diskUsageBytes, 0)
    }

    func testRTIntrospectTablesMissingTableErrors() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        XCTAssertThrowsError(try introspectTables(db, descriptors: [
            GeneratedTableDescriptor(tableName: "missing_table", typeName: "example.Missing", isCore: false, syncEnabled: true),
        ]))
    }
}
