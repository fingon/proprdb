import Foundation
import Logging

public let coreTableDirtyKeysDefinitionName = "_proprdb_dirty_key_definitions"
public let coreTableDirtyKeysConsumerName = "_proprdb_dirty_key_consumers"
public let coreTableDirtyKeysEntryName = "_proprdb_dirty_keys"
private let dirtyKeysLogger = Logger(label: "ProprDBSwiftRuntime.DirtyKeys")
private let dirtyKeysTriggerPrefix = "_proprdb_dirty_keys_"
private let dirtyKeysOld = "OLD"
private let dirtyKeysNew = "NEW"
private let dirtyKeysInsert = "INSERT"
private let dirtyKeysUpdate = "UPDATE"
private let dirtyKeysDelete = "DELETE"
private let dirtyKeysEvents = [dirtyKeysInsert, dirtyKeysUpdate, dirtyKeysDelete]

public struct DirtyKeysDescriptor: Sendable {
    public let name: String
    public let watchedColumns: [String]

    public init(name: String, watchedColumns: [String] = []) {
        self.name = name
        self.watchedColumns = watchedColumns
    }
}

public struct DirtyKeysToken: Sendable {
    fileprivate let table: String
    fileprivate let tracker: String
    fileprivate let consumer: String
    fileprivate let registration: String
    fileprivate let revision: Int64
}

public struct DirtyKeysState: Sendable {
    public let dirty: Bool
    public let full: Bool
    public let ids: [String]
    public let token: DirtyKeysToken
}

public struct StaleDirtyKeysError: Error, Sendable {}

func ensureDirtyKeysTables(_ q: any DBTX) throws {
    for statement in [
        "CREATE TABLE IF NOT EXISTS \(coreTableDirtyKeysDefinitionName) (table_name TEXT NOT NULL, tracker_name TEXT NOT NULL, watched_columns TEXT NOT NULL, PRIMARY KEY(table_name, tracker_name))",
        "CREATE TABLE IF NOT EXISTS \(coreTableDirtyKeysConsumerName) (table_name TEXT NOT NULL, tracker_name TEXT NOT NULL, consumer TEXT NOT NULL, registration TEXT NOT NULL, revision INTEGER NOT NULL CHECK(typeof(revision) = 'integer' AND revision >= 0), dirty INTEGER NOT NULL, full INTEGER NOT NULL, PRIMARY KEY(table_name, tracker_name, consumer))",
        "CREATE TABLE IF NOT EXISTS \(coreTableDirtyKeysEntryName) (table_name TEXT NOT NULL, tracker_name TEXT NOT NULL, consumer TEXT NOT NULL, id TEXT NOT NULL, PRIMARY KEY(table_name, tracker_name, consumer, id))",
    ] { try q.execute(statement) }
}

func dirtyKeysSignature(_ trackers: [DirtyKeysDescriptor]) -> String {
    let parts = trackers.sorted { $0.name < $1.name }.map {
        "keys:\($0.name):\($0.watchedColumns.sorted().joined(separator: ","))"
    }
    return parts.isEmpty ? "" : "|" + parts.joined(separator: "|")
}

func dropDirtyKeysTriggers(_ q: any DBTX, table: String) throws {
    for event in dirtyKeysEvents {
        try q.execute("DROP TRIGGER IF EXISTS \(quoteSQLiteIdentifier(dirtyKeysTriggerPrefix + table + "_" + event.lowercased()))")
    }
}

func reconcileDirtyKeys(_ q: any DBTX, binding: GeneratedTableBinding, changed: Bool) throws {
    let table = binding.descriptor.tableName
    if changed {
        try q.execute("UPDATE \(coreTableDirtyKeysConsumerName) SET revision = revision + 1, dirty = 1, full = 1 WHERE table_name = ?", arguments: [table])
        try q.execute("DELETE FROM \(coreTableDirtyKeysDefinitionName) WHERE table_name = ?", arguments: [table])
        for tracker in binding.dirtyKeys {
            try q.execute("INSERT INTO \(coreTableDirtyKeysDefinitionName) (table_name, tracker_name, watched_columns) VALUES (?, ?, ?)", arguments: [table, tracker.name, tracker.watchedColumns.sorted().joined(separator: ",")])
        }
        for target in [coreTableDirtyKeysEntryName, coreTableDirtyKeysConsumerName] {
            try q.execute("DELETE FROM \(target) WHERE table_name = ? AND tracker_name NOT IN (SELECT tracker_name FROM \(coreTableDirtyKeysDefinitionName) WHERE table_name = ?)", arguments: [table, table])
        }
    }
    guard !binding.dirtyKeys.isEmpty else { return }
    for event in dirtyKeysEvents { try q.execute(dirtyKeysTriggerSQL(binding, event: event)) }
}

private func dirtyKeysTriggerSQL(_ binding: GeneratedTableBinding, event: String) -> String {
    let table = binding.descriptor.tableName
    var body = ""
    for tracker in binding.dirtyKeys {
        var predicate = "table_name = \(quoteSQLiteString(table)) AND tracker_name = \(quoteSQLiteString(tracker.name))"
        if event == dirtyKeysUpdate {
            var columns = ["id"] + tracker.watchedColumns
            if tracker.watchedColumns.isEmpty { columns += ["at_ns", "data"] + binding.projectedColumns.map(\.name) }
            let comparisons = columns.map { "OLD.\(quoteSQLiteIdentifier($0)) IS NOT NEW.\(quoteSQLiteIdentifier($0))" }.joined(separator: " OR ")
            predicate += " AND (\(comparisons))"
        }
        body += "UPDATE \(coreTableDirtyKeysConsumerName) SET revision = revision + 1, dirty = 1 WHERE \(predicate);"
        let prefixes = event == dirtyKeysUpdate ? [dirtyKeysOld, dirtyKeysNew] : [event == dirtyKeysDelete ? dirtyKeysOld : dirtyKeysNew]
        for prefix in prefixes {
            body += "INSERT INTO \(coreTableDirtyKeysEntryName) (table_name, tracker_name, consumer, id) SELECT table_name, tracker_name, consumer, \(prefix).id FROM \(coreTableDirtyKeysConsumerName) WHERE \(predicate) ON CONFLICT(table_name, tracker_name, consumer, id) DO NOTHING;"
        }
    }
    return "CREATE TRIGGER IF NOT EXISTS \(quoteSQLiteIdentifier(dirtyKeysTriggerPrefix + table + "_" + event.lowercased())) AFTER \(event) ON \(quoteSQLiteIdentifier(table)) BEGIN \(body) END"
}

public func registerDirtyKeys(_ q: any DBTX, table: String, tracker: String, consumer: String) throws {
    guard !table.isEmpty, !tracker.isEmpty, !consumer.isEmpty else { throw ProprDBError("empty dirty keys identity") }
    let registration = try uuidV7()
    try q.withTransaction { transaction in
        try transaction.withRows("SELECT 1 FROM \(coreTableDirtyKeysDefinitionName) WHERE table_name = ? AND tracker_name = ?", arguments: [table, tracker]) { rows in
            guard try rows.next() != nil else { throw ProprDBError("unknown dirty keys definition") }
        }
        try transaction.execute("INSERT INTO \(coreTableDirtyKeysConsumerName) (table_name, tracker_name, consumer, registration, revision, dirty, full) VALUES (?, ?, ?, ?, 0, 1, 1) ON CONFLICT(table_name, tracker_name, consumer) DO NOTHING", arguments: [table, tracker, consumer, registration])
    }
}

public func readDirtyKeys(_ q: any DBTX, table: String, tracker: String, consumer: String) throws -> DirtyKeysState {
    dirtyKeysLogger.debug("Read dirty keys", metadata: ["table": .string(table), "tracker": .string(tracker), "consumer": .string(consumer)])
    return try q.withTransaction { transaction in
        let metadata = try transaction.withRows("SELECT registration, revision, dirty, full FROM \(coreTableDirtyKeysConsumerName) WHERE table_name = ? AND tracker_name = ? AND consumer = ?", arguments: [table, tracker, consumer]) { rows in
            guard let row = try rows.next() else { throw ProprDBError("unknown dirty keys consumer") }
            return try (row.string(at: 0), row.int64(at: 1), row.int64(at: 2) != 0, row.int64(at: 3) != 0)
        }
        let ids = try transaction.withRows("SELECT id FROM \(coreTableDirtyKeysEntryName) WHERE table_name = ? AND tracker_name = ? AND consumer = ? ORDER BY id", arguments: [table, tracker, consumer]) { rows in
            var ids: [String] = []
            while let row = try rows.next() { ids.append(try row.string(at: 0)) }
            return ids
        }
        return DirtyKeysState(dirty: metadata.2, full: metadata.3, ids: ids, token: DirtyKeysToken(table: table, tracker: tracker, consumer: consumer, registration: metadata.0, revision: metadata.1))
    }
}

public func acknowledgeDirtyKeys(_ q: any DBTX, token: DirtyKeysToken) throws {
    try q.withTransaction { transaction in
        try transaction.withRows("UPDATE \(coreTableDirtyKeysConsumerName) SET revision = revision + 1, dirty = 0, full = 0 WHERE table_name = ? AND tracker_name = ? AND consumer = ? AND registration = ? AND revision = ? RETURNING revision", arguments: [token.table, token.tracker, token.consumer, token.registration, token.revision]) { rows in
            guard try rows.next() != nil else { throw StaleDirtyKeysError() }
        }
        try transaction.execute("DELETE FROM \(coreTableDirtyKeysEntryName) WHERE table_name = ? AND tracker_name = ? AND consumer = ?", arguments: [token.table, token.tracker, token.consumer])
    }
}

public func removeDirtyKeysConsumer(_ q: any DBTX, table: String, tracker: String, consumer: String) throws {
    try q.withTransaction { transaction in
        for target in [coreTableDirtyKeysEntryName, coreTableDirtyKeysConsumerName] {
            try transaction.execute("DELETE FROM \(target) WHERE table_name = ? AND tracker_name = ? AND consumer = ?", arguments: [table, tracker, consumer])
        }
    }
}
