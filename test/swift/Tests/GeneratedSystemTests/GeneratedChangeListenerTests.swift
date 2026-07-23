@testable import GeneratedSystem
import ProprDBSwiftRuntime
import XCTest

final class GeneratedChangeListenerTests: XCTestCase {
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
