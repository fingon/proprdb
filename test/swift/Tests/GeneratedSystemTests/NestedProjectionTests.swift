@testable import GeneratedSystem
import Foundation
import ProprDBSwiftRuntime
import SwiftProtobuf
import XCTest

private let nestedPhotoID = "01951d6e-a000-7000-8000-000000000003"
private let oldPhotoTableSQL = "CREATE TABLE generatedtest_example_photo (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL)"
private let insertOldPhotoSQL = "INSERT INTO generatedtest_example_photo (id, at_ns, data) VALUES (?, ?, ?)"
private let emptyProjectionPredicate = "location_lon IS NULL AND location_lat IS NULL AND exif_create_utc_time_seconds IS NULL AND exif_modify_utc_time_seconds IS NULL AND location_altitude IS NULL AND location_label IS NULL AND selected_location_lon IS NULL AND location_next_lon IS NULL"
private let epochProjectionPredicate = "location_lon = 0 AND location_lat = 0 AND exif_create_utc_time_seconds = 0 AND exif_modify_utc_time_seconds IS NULL"

private func epochPhoto() -> Generatedtest_Example_Photo {
    var photo = Generatedtest_Example_Photo()
    photo.location = Generatedtest_Example_Location()
    photo.exifCreate.utcTime = Google_Protobuf_Timestamp()
    return photo
}

final class NestedProjectionTests: XCTestCase {
    func testPresenceAndUpdates() throws {
        var emptyTimestamp = Generatedtest_Example_Photo()
        emptyTimestamp.exifCreate = Generatedtest_Example_ZonedTimestamp()
        var detailed = epochPhoto()
        detailed.location.altitude = 0
        detailed.location.label = ""
        detailed.location.next = Generatedtest_Example_Location()
        detailed.selectedLocation = Generatedtest_Example_Location()
        let cases = [
            (Generatedtest_Example_Photo(), emptyProjectionPredicate),
            (emptyTimestamp, emptyProjectionPredicate),
            (epochPhoto(), epochProjectionPredicate),
            (detailed, epochProjectionPredicate + " AND location_altitude = 0 AND location_label = '' AND selected_location_lon = 0 AND location_next_lon = 0"),
        ]
        for (data, predicate) in cases {
            let db = try SQLiteDatabase(path: ":memory:")
            let crud = CRUD(db)
            try crud.initialize()
            let row = try crud.photo.insert(data)
            XCTAssertEqual(try crud.photo.select(where: "id = ? AND " + predicate, arguments: [.string(row.id)]).count, 1)
            _ = try crud.photo.updateByID(row.id, data: Generatedtest_Example_Photo())
            XCTAssertEqual(try crud.photo.select(where: "id = ? AND " + emptyProjectionPredicate, arguments: [.string(row.id)]).count, 1)
        }
    }

    func testBackfillPreservesBytesAndSync() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        try ensureCoreTables(db)
        try db.execute(oldPhotoTableSQL)
        var payload = try epochPhoto().serializedData()
        payload.append(contentsOf: [0xa0, 0x06, 0x01])
        try db.execute(insertOldPhotoSQL, arguments: [nestedPhotoID, Int64(7), payload])
        try db.execute("INSERT INTO _sync (object_id, table_name, at_ns, remote) VALUES (?, ?, ?, ?)", arguments: [nestedPhotoID, PhotoTableName, Int64(7), "source"])
        let table = PhotoTable(db)
        try table.initialize()
        try table.initialize()
        XCTAssertEqual(try table.select(where: "id = ? AND " + epochProjectionPredicate, arguments: [.string(nestedPhotoID)]).count, 1)
        let stored = try db.withRows("SELECT data FROM generatedtest_example_photo WHERE id = ?", arguments: [nestedPhotoID]) { rows in
            try XCTUnwrap(rows.next()).data(at: 0)
        }
        XCTAssertEqual(stored, payload)
        XCTAssertEqual(try scalarInt(db, sql: "SELECT at_ns FROM generatedtest_example_photo WHERE id = ?", arguments: [nestedPhotoID]), 7)
        XCTAssertEqual(try scalarInt(db, sql: "SELECT at_ns FROM _sync WHERE object_id = ? AND table_name = ? AND remote = ?", arguments: [nestedPhotoID, PhotoTableName, "source"]), 7)
        XCTAssertTrue(try tableIndexNamesByName(db: db, tableName: PhotoTableName).contains("idx_generatedtest_example_photo__location_lon_location_lat"))
    }

    func testCorruptBackfillRollsBackSchema() throws {
        let db = try SQLiteDatabase(path: ":memory:")
        try ensureCoreTables(db)
        try db.execute(oldPhotoTableSQL)
        try db.execute(insertOldPhotoSQL, arguments: [nestedPhotoID, Int64(7), Data([0xff])])
        XCTAssertThrowsError(try PhotoTable(db).initialize())
        XCTAssertEqual(try scalarInt(db, sql: "SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?", arguments: [PhotoTableName, "location_lon"]), 0)
        XCTAssertEqual(try scalarInt(db, sql: "SELECT COUNT(*) FROM _proprdb_schema WHERE table_name = ?", arguments: [PhotoTableName]), 0)
    }

    func testSyncAndWriteRollback() throws {
        let sourceDB = try SQLiteDatabase(path: ":memory:")
        let targetDB = try SQLiteDatabase(path: ":memory:")
        let source = CRUD(sourceDB)
        let target = CRUD(targetDB)
        try source.initialize()
        try target.initialize()
        let row = try source.photo.insert(epochPhoto())
        try target.readJSONL(remote: "source", text: source.writeJSONL(remote: ""))
        XCTAssertEqual(try target.photo.select(where: "id = ? AND " + epochProjectionPredicate, arguments: [.string(row.id)]).count, 1)
        let transaction = try targetDB.beginTransaction()
        _ = try PhotoTable(transaction).updateByID(row.id, data: Generatedtest_Example_Photo())
        try transaction.rollback()
        XCTAssertEqual(try target.photo.select(where: "id = ? AND " + epochProjectionPredicate, arguments: [.string(row.id)]).count, 1)
        _ = try source.photo.updateByID(row.id, data: Generatedtest_Example_Photo())
        try target.readJSONL(remote: "source", text: source.writeJSONL(remote: ""))
        XCTAssertEqual(try target.photo.select(where: "id = ? AND " + emptyProjectionPredicate, arguments: [.string(row.id)]).count, 1)
    }
    func testSharedJSONLFixtureRoundTrip() throws {
        let fixtureURL = URL(fileURLWithPath: #filePath).deletingLastPathComponent().appendingPathComponent("../../../testdata/nested-photo.jsonl")
        let text = try String(contentsOf: fixtureURL, encoding: .utf8)
        let sourceDB = try SQLiteDatabase(path: ":memory:")
        let targetDB = try SQLiteDatabase(path: ":memory:")
        let source = CRUD(sourceDB)
        let target = CRUD(targetDB)
        try source.initialize()
        try target.initialize()
        try source.readJSONL(remote: "source", text: text)
        let predicate = "id = ? AND location_lon = 0 AND location_lat = 0 AND exif_create_utc_time_seconds = 0 AND exif_modify_utc_time_seconds = 42 AND location_altitude = 0 AND location_label = '' AND selected_location_lon = 0 AND location_next_lon = 0"
        XCTAssertEqual(try source.photo.select(where: predicate, arguments: [.string(nestedPhotoID)]).count, 1)
        try target.readJSONL(remote: "source", text: source.writeJSONL(remote: ""))
        XCTAssertEqual(try target.photo.select(where: predicate, arguments: [.string(nestedPhotoID)]).count, 1)
    }

    func testSourcePathChangesAreRejected() throws {
        for previous in ["location_lon:double:optional", "location_lon:double:optional:path=other.lon"] {
            let db = try SQLiteDatabase(path: ":memory:")
            let table = PhotoTable(db)
            try table.initialize()
            let schema = PhotoProjectionSchema.replacingOccurrences(of: "location_lon:double:optional:path=location.lon", with: previous)
            try db.execute("UPDATE _proprdb_schema SET schema_hash = ? WHERE table_name = ?", arguments: [schema, PhotoTableName])
            XCTAssertThrowsError(try table.initialize())
            XCTAssertEqual(try scalarString(db, sql: "SELECT schema_hash FROM _proprdb_schema WHERE table_name = ?", arguments: [PhotoTableName]), schema)
        }
    }

}
