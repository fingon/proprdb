@testable import GeneratedSystem
import Foundation
import ProprDBSwiftRuntime
import XCTest

final class InitializationBenchmarkTests: XCTestCase {
    func testInitializationBenchmark() throws {
        guard ProcessInfo.processInfo.environment["PROPRDB_INIT_BENCHMARK"] == "1" else {
            throw XCTSkip("Run make benchmark-init to measure initialization")
        }
        let fixtureURL = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent().deletingLastPathComponent()
            .deletingLastPathComponent().deletingLastPathComponent()
            .appendingPathComponent("testdata/initialization.sql")
        let fixture = try String(contentsOf: fixtureURL, encoding: .utf8)
        let iterations = 10
        for storage in ["memory", "file"] {
            for rowCount in [0, 1000, 10000] {
                let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
                try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
                defer {
                    do { try FileManager.default.removeItem(at: directory) }
                    catch { XCTFail("remove benchmark directory: \(error)") }
                }
                let path = storage == "memory" ? ":memory:" : directory.appendingPathComponent("init.sqlite").path
                let db = try SQLiteDatabase(path: path)
                let crud = CRUD(db)
                try crud.initialize()
                try db.withTransaction { transaction in
                    for statement in fixture.split(separator: ";") {
                        if statement.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty { continue }
                        try transaction.execute(String(statement), arguments: statement.contains("?") ? [rowCount, rowCount] : [])
                    }
                }
                let clock = ContinuousClock()
                let started = clock.now
                for _ in 0..<iterations { try crud.initialize() }
                let duration = started.duration(to: clock.now).components
                let elapsedSec = Double(duration.seconds) + Double(duration.attoseconds) / 1e18
                let statements = try recordInitializationSQL(db) { try crud.initialize() }
                print("init swift storage=\(storage) rows=\(rowCount) ns/op=\(elapsedSec * 1e9 / Double(iterations)) statements/op=\(statements.count)")
                try db.close()
            }
        }
    }
}
