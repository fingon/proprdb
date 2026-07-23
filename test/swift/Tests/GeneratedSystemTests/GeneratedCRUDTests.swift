@testable import GeneratedSystem
import ProprDBSwiftRuntime
import XCTest

private let countTombstoneByIDSQL = "SELECT COUNT(*) FROM _deleted WHERE table_name = ? AND id = ?"
private let personNameIndex = "idx_generatedtest_example_person__name"
private let personNameAgeIndex = "idx_generatedtest_example_person__name_age"
private let personStaleIndex = "idx_generatedtest_example_person__stale"

final class GeneratedCRUDTests: XCTestCase {
    func testGeneratedCRUD() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)
        try crud.initialize()

        let indexesAfterInit = try tableIndexNamesByName(db: db, tableName: PersonTableName)
        XCTAssertTrue(indexesAfterInit.contains(personNameIndex))
        XCTAssertTrue(indexesAfterInit.contains(personNameAgeIndex))

        try crud.person.initialize()
        let indexesAfterSecondInit = try tableIndexNamesByName(db: db, tableName: PersonTableName)
        XCTAssertTrue(indexesAfterSecondInit.contains(personNameIndex))
        XCTAssertTrue(indexesAfterSecondInit.contains(personNameAgeIndex))

        try db.execute("CREATE INDEX IF NOT EXISTS \(quoteSQLiteIdentifier(personStaleIndex)) ON \(quoteSQLiteIdentifier(PersonTableName)) (\"name\")")
        XCTAssertTrue(try tableIndexNamesByName(db: db, tableName: PersonTableName).contains(personStaleIndex))

        try crud.person.initialize()
        XCTAssertFalse(try tableIndexNamesByName(db: db, tableName: PersonTableName).contains(personStaleIndex))

        let hiddenTableCount = try scalarInt(db, sql: "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", arguments: ["generatedtest_example_hidden"])
        XCTAssertEqual(hiddenTableCount, 0)

        XCTAssertThrowsError(try crud.person.insert(makePerson(name: "", age: 1)))

        let inserted = try crud.person.insert(makePerson(name: "Ada", age: 37))
        XCTAssertFalse(inserted.id.isEmpty)
        XCTAssertGreaterThan(inserted.atNs, 0)

        let customID = "018f4f3f-6f9f-7a1b-8f55-1234567890ab"
        let insertedWithID = try crud.person.insertWithID(customID, data: makePerson(name: "Grace", age: 30))
        XCTAssertEqual(insertedWithID.id, customID)
        XCTAssertGreaterThan(insertedWithID.atNs, 0)

        XCTAssertThrowsError(try crud.person.insertWithID("", data: makePerson(name: "Empty", age: 1)))
        XCTAssertThrowsError(try crud.person.insertWithID("not-a-uuid", data: makePerson(name: "Bad", age: 1)))

        let selected = try crud.person.select(where: "name = ?", arguments: ["Ada"])
        XCTAssertEqual(selected.count, 1)
        XCTAssertEqual(selected[0].id, inserted.id)

        try crud.person.deleteByID(inserted.id)
        XCTAssertEqual(try scalarInt(db, sql: countTombstoneByIDSQL, arguments: [PersonTableName, inserted.id]), 1)

        let updated = try crud.person.updateByID(inserted.id, data: makePerson(name: "Ada Lovelace", age: 38))
        XCTAssertEqual(updated.id, inserted.id)
        XCTAssertEqual(try scalarInt(db, sql: countTombstoneByIDSQL, arguments: [PersonTableName, inserted.id]), 0)

        XCTAssertThrowsError(try crud.person.updateByID("not-a-uuid", data: makePerson(name: "Nope", age: 10)))

        try db.execute("UPDATE \(quoteSQLiteIdentifier(PersonTableName)) SET \"age\" = 0 WHERE id = ?", arguments: [inserted.id])
        try db.execute("UPDATE _proprdb_schema SET schema_hash = ? WHERE table_name = ?", arguments: ["stale", PersonTableName])
        try crud.person.initialize()

        XCTAssertEqual(try scalarInt(db, sql: "SELECT \"age\" FROM \(quoteSQLiteIdentifier(PersonTableName)) WHERE id = ?", arguments: [inserted.id]), 38)

        let updatedByRow = try crud.person.updateRow(PersonRow(id: inserted.id, atNs: inserted.atNs, data: makePerson(name: "Countess of Lovelace", age: 39)))
        XCTAssertEqual(updatedByRow.id, inserted.id)

        try crud.person.deleteRow(PersonRow(id: inserted.id, atNs: inserted.atNs, data: makePerson()))
        XCTAssertEqual(try scalarInt(db, sql: countTombstoneByIDSQL, arguments: [PersonTableName, inserted.id]), 1)

        let transaction = try db.beginTransaction()
        let txTable = PersonTable(transaction)
        _ = try txTable.insert(makePerson(name: "Tx User", age: 41))
        try transaction.commit()

        let insertedNote = try crud.note.insert(makeNote(text: "Projected note"))
        XCTAssertEqual(try scalarString(db, sql: "SELECT \"text\" FROM \(quoteSQLiteIdentifier(NoteTableName)) WHERE id = ?", arguments: [insertedNote.id]), "Projected note")
    }

    func testGeneratedCRUDTableDescriptors() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(db)

        XCTAssertEqual(
            crud.tableDescriptors(),
            [
                GeneratedTableDescriptor(tableName: PersonTableName, typeName: PersonTypeName, isCore: false, syncEnabled: true, changeListenersEnabled: true),
                GeneratedTableDescriptor(tableName: NoteTableName, typeName: NoteTypeName, isCore: false, syncEnabled: false, changeListenersEnabled: true),
                GeneratedTableDescriptor(tableName: coreTableDeletedName, typeName: "", isCore: true, syncEnabled: false),
                GeneratedTableDescriptor(tableName: coreTableSyncName, typeName: "", isCore: true, syncEnabled: false),
                GeneratedTableDescriptor(tableName: coreTableSchemaStateName, typeName: "", isCore: true, syncEnabled: false),
                GeneratedTableDescriptor(tableName: coreTableUnknownName, typeName: "", isCore: true, syncEnabled: false),
                GeneratedTableDescriptor(tableName: coreTableUnknownSyncName, typeName: "", isCore: true, syncEnabled: false),
                GeneratedTableDescriptor(tableName: coreTableMetadataName, typeName: "", isCore: true, syncEnabled: false),
                GeneratedTableDescriptor(tableName: coreTableExportBatchName, typeName: "", isCore: true, syncEnabled: false),
                GeneratedTableDescriptor(tableName: coreTableExportEntryName, typeName: "", isCore: true, syncEnabled: false),
            ]
        )
    }
}
