@testable import GeneratedSystem
import CSQLite
import Foundation
import ProprDBSwiftRuntime
import SwiftProtobuf
import XCTest

private let personIndexPrefix = "idx_" + PersonTableName + "__"
private let choiceIndexPrefix = "idx_" + ChoiceTableName + "__"
private let personNameIndex = personIndexPrefix + "name"
private let personNameAgeIndex = personIndexPrefix + "name_age"
private let stalePersonIndex = personIndexPrefix + "stale"
private let applicationPersonIndex = "application_person_age"
private let obsoleteColumn = "obsolete"
private let obsoleteColumnSQL = "ALTER TABLE \(quoteSQLiteIdentifier(PersonTableName)) ADD COLUMN \(obsoleteColumn) TEXT"
private let staleIndexSQL = "CREATE INDEX \(quoteSQLiteIdentifier(stalePersonIndex)) ON \(quoteSQLiteIdentifier(PersonTableName)) (\(obsoleteColumn))"
private let applicationIndexSQL = "CREATE INDEX \(applicationPersonIndex) ON \(quoteSQLiteIdentifier(PersonTableName)) (age)"
private let staleSchemaSQL = "UPDATE _proprdb_schema SET schema_hash = 'stale' WHERE table_name = ?"
private let schemaSQL = "SELECT schema_hash FROM _proprdb_schema WHERE table_name = ?"
private let columnCountSQL = "SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?"
private let choiceLabelIndex = choiceIndexPrefix + "label"
private let choiceTimeIndex = choiceIndexPrefix + "at_ns"
private let choiceLabelSQL = "CREATE INDEX \(quoteSQLiteIdentifier(choiceLabelIndex)) ON \(quoteSQLiteIdentifier(ChoiceTableName)) (label)"
private let choiceTimeSQL = "CREATE INDEX \(quoteSQLiteIdentifier(choiceTimeIndex)) ON \(quoteSQLiteIdentifier(ChoiceTableName)) (at_ns)"
private let PersonCreateIndexSQL1 = "CREATE INDEX IF NOT EXISTS \"idx_generatedtest_example_person__name\" ON \"generatedtest_example_person\" (\"name\")"
private let PersonCreateIndexSQL2 = "CREATE INDEX IF NOT EXISTS \"idx_generatedtest_example_person__name_age\" ON \"generatedtest_example_person\" (\"name\", \"age\")"
private let testProjectedAge: Int64 = 37
private let testID = "01951d6e-a000-7000-8000-000000000001"

private final class IndexTrace {
    var statements: [String] = []
}

private func recordIndexDDL(_ db: SQLiteDatabase, action: () throws -> Void) throws -> [String] {
    let trace = IndexTrace()
    let context = Unmanaged.passUnretained(trace).toOpaque()
    let status = sqlite3_trace_v2(db.sqliteHandle, UInt32(SQLITE_TRACE_STMT), { _, context, _, sql in
        guard let context, let sql else { return 0 }
        let statement = String(cString: sql.assumingMemoryBound(to: CChar.self))
        if statement.hasPrefix("CREATE INDEX ") || statement.hasPrefix("DROP INDEX ") {
            Unmanaged<IndexTrace>.fromOpaque(context).takeUnretainedValue().statements.append(statement)
        }
        return 0
    }, context)
    guard status == SQLITE_OK else { throw ProprDBError("install index SQL trace: status=\(status)") }
    defer {
        XCTAssertEqual(sqlite3_trace_v2(db.sqliteHandle, 0, nil, nil), SQLITE_OK)
        withExtendedLifetime(trace) {}
    }
    try action()
    return trace.statements
}

private let ChoiceCreateTableSQL = "CREATE TABLE IF NOT EXISTS \"generatedtest_example_choice\" (\"id\" TEXT PRIMARY KEY, \"at_ns\" INTEGER NOT NULL, \"data\" BLOB NOT NULL, \"label\" TEXT)"
private let ChoiceInsertSQL = "INSERT INTO \"generatedtest_example_choice\" (\"id\", \"at_ns\", \"data\", \"label\") VALUES (?, ?, ?, ?)"
private let ChoiceUpsertSQL = "INSERT INTO \"generatedtest_example_choice\" (\"id\", \"at_ns\", \"data\", \"label\") VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET \"at_ns\" = excluded.\"at_ns\", \"data\" = excluded.\"data\", \"label\" = excluded.\"label\""

private let ChoiceGeneratedBinding = GeneratedTableBinding(
	descriptor: GeneratedTableDescriptor(tableName: ChoiceTableName, typeName: ChoiceTypeName, isCore: false, syncEnabled: true, changeListenersEnabled: false, queryStatisticsEnabled: false),
	messageType: Generatedtest_Example_Choice.self,
	insertSQL: ChoiceInsertSQL,
	upsertSQL: ChoiceUpsertSQL,
	createTableSQL: ChoiceCreateTableSQL,
	projectionSchema: ChoiceProjectionSchema,
	projectedColumns: [
		ProjectedColumnDescriptor(name: "label", protoKind: "string", sqliteType: "TEXT", defaultSQL: "''", nullable: true, legacyOneofPresenceRepair: true),
	],
	generatedIndexes: [
	],
	generatedIndexPrefix: choiceIndexPrefix,
	decodeAnyJSON: { try decodeAnyJSON($0, as: Generatedtest_Example_Choice.self) },
	decodeBinary: { try Generatedtest_Example_Choice(serializedBytes: $0) },
	encodeAnyJSON: { message in
		guard let data = message as? Generatedtest_Example_Choice else { throw ProprDBError("expected Generatedtest_Example_Choice") }
		return try marshalAnyJSON(data, typeName: ChoiceTypeName)
	},
	messagesEqual: { left, right in
		guard let left = left as? Generatedtest_Example_Choice, let right = right as? Generatedtest_Example_Choice else { return false }
		return left == right
	},
	projectedValues: { message in
		guard let data = message as? Generatedtest_Example_Choice else { throw ProprDBError("expected Generatedtest_Example_Choice") }
		var values: [SQLiteBindValue] = []
		if case .label = data.selection { values.append(sqliteBindValue(data.label)) } else { values.append(.null) }
		return values
	}
)


private func indexedChoiceBinding() -> GeneratedTableBinding {
    let binding = ChoiceGeneratedBinding
    return GeneratedTableBinding(
        descriptor: binding.descriptor,
        messageType: binding.messageType,
        insertSQL: binding.insertSQL,
        upsertSQL: binding.upsertSQL,
        createTableSQL: binding.createTableSQL,
        projectionSchema: binding.projectionSchema,
        projectedColumns: binding.projectedColumns,
        generatedIndexes: [
            GeneratedIndexDescriptor(name: choiceLabelIndex, createSQL: choiceLabelSQL),
            GeneratedIndexDescriptor(name: choiceTimeIndex, createSQL: choiceTimeSQL),
        ],
        generatedIndexPrefix: binding.generatedIndexPrefix,
        decodeAnyJSON: binding.decodeAnyJSON,
        decodeBinary: binding.decodeBinary,
        encodeAnyJSON: binding.encodeAnyJSON,
        messagesEqual: binding.messagesEqual,
        projectedValues: binding.projectedValues
    )
}

final class IndexReconciliationTests: XCTestCase {
    func testGeneratedIndexReconciliation() throws {
        let cases: [(name: String, setupSQL: [String], fullInit: Bool, expectedDDL: [String])] = [
            ("unchanged table", [], false, []),
            ("unchanged CRUD", [], true, []),
            ("missing index", ["DROP INDEX \(quoteSQLiteIdentifier(personNameIndex))"], false, [PersonCreateIndexSQL1]),
            ("stale index and obsolete column", [obsoleteColumnSQL, staleIndexSQL], false, ["DROP INDEX \(quoteSQLiteIdentifier(stalePersonIndex))"]),
            ("stale index on current column", ["CREATE INDEX \(quoteSQLiteIdentifier(stalePersonIndex)) ON \(quoteSQLiteIdentifier(PersonTableName)) (name)"], false, ["DROP INDEX \(quoteSQLiteIdentifier(stalePersonIndex))"]),
            ("reprojection", [staleSchemaSQL, "UPDATE \(quoteSQLiteIdentifier(PersonTableName)) SET age = 0 WHERE name = 'Ada'"], false, []),
        ]
        for testCase in cases {
            let db = try SQLiteDatabase(path: ":memory:")
            defer { XCTAssertNoThrow(try db.close()) }
            let crud = CRUD(db)
            let initialDDL = try recordIndexDDL(db) { try crud.initialize() }
            XCTAssertTrue(initialDDL.contains(PersonCreateIndexSQL1), testCase.name)
            XCTAssertTrue(initialDDL.contains(PersonCreateIndexSQL2), testCase.name)
            let row = try crud.person.insert(makePerson(name: "Ada", age: testProjectedAge))
            try db.execute(applicationIndexSQL)
            for statement in testCase.setupSQL {
                try db.execute(statement, arguments: statement == staleSchemaSQL ? [PersonTableName] : [])
            }
            let ddl = try recordIndexDDL(db) {
                if testCase.fullInit { try crud.initialize() } else { try crud.person.initialize() }
            }
            XCTAssertEqual(ddl, testCase.expectedDDL, testCase.name)
            let indexes = try tableIndexNamesByName(db: db, tableName: PersonTableName)
            XCTAssertTrue(indexes.contains(personNameIndex), testCase.name)
            XCTAssertTrue(indexes.contains(personNameAgeIndex), testCase.name)
            XCTAssertTrue(indexes.contains(applicationPersonIndex), testCase.name)
            XCTAssertFalse(indexes.contains(stalePersonIndex), testCase.name)
            XCTAssertEqual(try scalarInt(db, sql: columnCountSQL, arguments: [PersonTableName, obsoleteColumn]), 0, testCase.name)
            XCTAssertEqual(try scalarInt(db, sql: "SELECT age FROM \(quoteSQLiteIdentifier(PersonTableName)) WHERE id = ?", arguments: [row.id]), testProjectedAge, testCase.name)
            XCTAssertEqual(try recordIndexDDL(db) { try crud.person.initialize() }, [], testCase.name)
        }
    }

    func testRepairsOnlyAffectedIndex() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        defer { XCTAssertNoThrow(try db.close()) }
        try ensureCoreTables(db)
        try db.execute("CREATE TABLE \(quoteSQLiteIdentifier(ChoiceTableName)) (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL, label TEXT NOT NULL DEFAULT '')")
        try db.execute("INSERT INTO _proprdb_schema (table_name, schema_hash) VALUES (?, ?)", arguments: [ChoiceTableName, "label:string"])
        var choice = Generatedtest_Example_Choice()
        choice.count = 7
        try db.execute("INSERT INTO \(quoteSQLiteIdentifier(ChoiceTableName)) (id, at_ns, data) VALUES (?, ?, ?)", arguments: [testID, Int64(1), try choice.serializedData()])
        try db.execute(choiceLabelSQL)
        try db.execute(choiceTimeSQL)
        let binding = indexedChoiceBinding()
        XCTAssertEqual(try recordIndexDDL(db) { try reconcileGeneratedTable(db, binding: binding) }, ["DROP INDEX \(quoteSQLiteIdentifier(choiceLabelIndex))", choiceLabelSQL])
        XCTAssertEqual(try scalarInt(db, sql: "SELECT label IS NULL FROM \(quoteSQLiteIdentifier(ChoiceTableName)) WHERE id = ?", arguments: [testID]), 1)
        let indexes = try tableIndexNamesByName(db: db, tableName: ChoiceTableName)
        XCTAssertTrue(indexes.contains(choiceLabelIndex))
        XCTAssertTrue(indexes.contains(choiceTimeIndex))
        XCTAssertEqual(try recordIndexDDL(db) { try reconcileGeneratedTable(db, binding: binding) }, [])
    }

    func testFailedReprojectionRollsBackIndexesAndColumns() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        defer { XCTAssertNoThrow(try db.close()) }
        let crud = CRUD(db)
        try crud.initialize()
        for statement in [obsoleteColumnSQL, staleIndexSQL, applicationIndexSQL] { try db.execute(statement) }
        try db.execute("INSERT INTO \(quoteSQLiteIdentifier(PersonTableName)) (id, at_ns, data) VALUES (?, ?, ?)", arguments: [testID, Int64(1), Data([0xff])])
        let indexesBefore = try tableIndexNamesByName(db: db, tableName: PersonTableName)
        let schemaBefore = try scalarString(db, sql: schemaSQL, arguments: [PersonTableName])
        let ddl = try recordIndexDDL(db) { XCTAssertThrowsError(try crud.person.initialize()) }
        XCTAssertEqual(ddl, ["DROP INDEX \(quoteSQLiteIdentifier(stalePersonIndex))"])
        XCTAssertEqual(try tableIndexNamesByName(db: db, tableName: PersonTableName), indexesBefore)
        XCTAssertEqual(try scalarInt(db, sql: columnCountSQL, arguments: [PersonTableName, obsoleteColumn]), 1)
        XCTAssertEqual(try scalarString(db, sql: schemaSQL, arguments: [PersonTableName]), schemaBefore)
    }
}
