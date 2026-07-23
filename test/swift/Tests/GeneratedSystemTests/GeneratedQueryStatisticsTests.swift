@testable import GeneratedSystem
import ProprDBSwiftRuntime
import XCTest

final class GeneratedQueryStatisticsTests: XCTestCase {
    func testGeneratedQueryStatistics() throws {
        let database = try SQLiteDatabase(path: ":memory:")
        let crud = CRUD(database)
        try crud.initialize()

        _ = try crud.person.insert(makePerson(name: "Ada", age: 37))
        _ = try crud.person.insert(makePerson(name: "Grace", age: 30))
        _ = try crud.note.insert(makeNote(text: "not measured"))
        try clearQueryStatistics(database)

        _ = try crud.person.select(where: "name = ?", arguments: ["Ada"])
        let initialStatistics = try queryStatistics(database)
        XCTAssertEqual(initialStatistics.count, 1)
        XCTAssertEqual(initialStatistics[0].calls, 1)
        let initialDurationSumNs = initialStatistics[0].durationSumNs
        _ = try crud.person.select(where: "name = ?", arguments: ["Grace"])
        _ = try crud.person.select()
        _ = try crud.note.select(where: "text = ?", arguments: ["not measured"])
        XCTAssertThrowsError(try crud.person.select(where: "missing = ?", arguments: ["ignored"]))

        let statistics = try queryStatistics(database)
        XCTAssertEqual(statistics.count, 2)
        XCTAssertEqual(statistics[0].tableName, PersonTableName)
        XCTAssertEqual(statistics[0].query, "SELECT id, at_ns, data FROM \(quoteSQLiteIdentifier(PersonTableName))")
        XCTAssertEqual(statistics[0].calls, 1)
        XCTAssertGreaterThanOrEqual(statistics[0].durationSumNs, 0)
        XCTAssertEqual(statistics[1].tableName, PersonTableName)
        XCTAssertEqual(statistics[1].query, "SELECT id, at_ns, data FROM \(quoteSQLiteIdentifier(PersonTableName)) WHERE name = ?")
        XCTAssertEqual(statistics[1].calls, 2)
        XCTAssertGreaterThanOrEqual(statistics[1].durationSumNs, initialDurationSumNs)
        XCTAssertFalse(statistics[1].query.contains("Ada"))
        XCTAssertFalse(statistics[1].query.contains("Grace"))

        try clearQueryStatistics(database)
        XCTAssertTrue(try queryStatistics(database).isEmpty)

        let transaction = try database.beginTransaction()
        _ = try PersonTable(transaction).select()
        try transaction.rollback()
        XCTAssertTrue(try queryStatistics(database).isEmpty)

        try database.execute("DROP TABLE \(quoteSQLiteIdentifier(coreTableQueryStatName))")
        XCTAssertThrowsError(try crud.person.select())
    }
}
