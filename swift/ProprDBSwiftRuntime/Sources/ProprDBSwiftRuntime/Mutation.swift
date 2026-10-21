import Foundation
import Logging

public let coreTableGenerationName = "_proprdb_generations"
public let coreTableDirtyDefinitionName = "_proprdb_dirty_definitions"
public let coreTableDirtyWindowName = "_proprdb_dirty_windows"
private let mutationTriggerPrefix = "_proprdb_mutation_"
private let mutationFormat = "v3|"
private let mutationInsert = "INSERT"
private let mutationUpdate = "UPDATE"
private let mutationDelete = "DELETE"
private let mutationBeforeInsert = "BEFORE_INSERT"
private let mutationBeforeUpdate = "BEFORE_UPDATE"
private let mutationEvents = [mutationInsert, mutationUpdate, mutationDelete, mutationBeforeInsert, mutationBeforeUpdate]
private let mutationLogger = Logger(label: "ProprDBSwiftRuntime.Mutation")

public struct DirtyWindowDescriptor: Sendable {
    public let name: String
    public let startColumn: String
    public let endColumn: String

    public init(name: String, startColumn: String, endColumn: String) {
        self.name = name
        self.startColumn = startColumn
        self.endColumn = endColumn
    }
}

public struct DirtyWindowToken: Sendable {
    fileprivate let table: String
    fileprivate let window: String
    fileprivate let consumer: String
    fileprivate let registration: String
    fileprivate let revision: Int64
}

public struct DirtyWindowState: Sendable {
    public let dirty: Bool
    public let full: Bool
    public let lower: Int64?
    public let upper: Int64?
    public let token: DirtyWindowToken
}

public struct StaleDirtyWindowError: Error, Sendable {}

func ensureMutationTables(_ q: any DBTX) throws {
    let statements = [
        "CREATE TABLE IF NOT EXISTS \(coreTableGenerationName) (table_name TEXT PRIMARY KEY, generation INTEGER NOT NULL CHECK(typeof(generation) = 'integer' AND generation >= 0), signature TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS \(coreTableDirtyDefinitionName) (table_name TEXT NOT NULL, window_name TEXT NOT NULL, start_column TEXT NOT NULL, end_column TEXT NOT NULL, PRIMARY KEY(table_name, window_name))",
        "CREATE TABLE IF NOT EXISTS \(coreTableDirtyWindowName) (table_name TEXT NOT NULL, window_name TEXT NOT NULL, consumer TEXT NOT NULL, registration TEXT NOT NULL, revision INTEGER NOT NULL CHECK(typeof(revision) = 'integer' AND revision >= 0), dirty INTEGER NOT NULL, full INTEGER NOT NULL, lower_bound INTEGER, upper_bound INTEGER, PRIMARY KEY(table_name, window_name, consumer))",
    ]
    for statement in statements { try q.execute(statement) }
    try ensureDirtyKeysTables(q)
}

private func mutationSignature(_ binding: GeneratedTableBinding) -> String {
    let windows = binding.dirtyWindows.sorted { $0.name < $1.name }
    return ([mutationFormat + binding.projectionSchema] + windows.map { "\($0.name):\($0.startColumn):\($0.endColumn)" }).joined(separator: "|") + dirtyKeysSignature(binding.dirtyKeys)
}

private func mutationTriggerName(_ table: String, _ event: String) -> String {
    mutationTriggerPrefix + table + "_" + event.lowercased()
}

func prepareMutationTracking(_ q: any DBTX, binding: GeneratedTableBinding) throws -> Bool {
    let signature = try q.withRows("SELECT signature FROM \(coreTableGenerationName) WHERE table_name = ?", arguments: [binding.descriptor.tableName]) { rows in
        try rows.next()?.string(at: 0)
    }
    let changed = signature != mutationSignature(binding)
    if changed {
        try dropDirtyKeysTriggers(q, table: binding.descriptor.tableName)
        for event in mutationEvents {
            try q.execute("DROP TRIGGER IF EXISTS \(quoteSQLiteIdentifier(mutationTriggerName(binding.descriptor.tableName, event)))")
        }
    }
    return changed
}

func finishMutationTracking(_ q: any DBTX, binding: GeneratedTableBinding, changed: Bool) throws {
    let table = binding.descriptor.tableName
    if changed {
        mutationLogger.debug("Reconcile mutation tracking", metadata: ["table": .string(table), "window_count": .stringConvertible(binding.dirtyWindows.count)])
        try q.execute("INSERT INTO \(coreTableGenerationName) (table_name, generation, signature) VALUES (?, 0, ?) ON CONFLICT(table_name) DO UPDATE SET generation = generation + 1, signature = excluded.signature", arguments: [table, mutationSignature(binding)])
        try q.execute("UPDATE \(coreTableDirtyWindowName) SET revision = revision + 1, dirty = 1, full = 1, lower_bound = NULL, upper_bound = NULL WHERE table_name = ?", arguments: [table])
        try q.execute("DELETE FROM \(coreTableDirtyDefinitionName) WHERE table_name = ?", arguments: [table])
        for window in binding.dirtyWindows {
            try q.execute("INSERT INTO \(coreTableDirtyDefinitionName) (table_name, window_name, start_column, end_column) VALUES (?, ?, ?, ?)", arguments: [table, window.name, window.startColumn, window.endColumn])
        }
        try q.execute("DELETE FROM \(coreTableDirtyWindowName) WHERE table_name = ? AND window_name NOT IN (SELECT window_name FROM \(coreTableDirtyDefinitionName) WHERE table_name = ?)", arguments: [table, table])
    }
    for event in mutationEvents where (event != mutationBeforeInsert && event != mutationBeforeUpdate) || !binding.dirtyWindows.isEmpty {
        try q.execute(mutationTriggerSQL(binding, event: event))
    }
    try reconcileDirtyKeys(q, binding: binding, changed: changed)
}

private func mutationTriggerSQL(_ binding: GeneratedTableBinding, event: String) -> String {
    let table = binding.descriptor.tableName
    let capturesExisting = event == mutationBeforeInsert || event == mutationBeforeUpdate
    var condition = ""
    if event == mutationUpdate || capturesExisting {
        let columns = ["id", "at_ns", "data"] + binding.projectedColumns.map(\.name)
        let prefix = capturesExisting ? "existing" : "OLD"
        let comparisons = columns.map { "\(prefix).\(quoteSQLiteIdentifier($0)) IS NOT NEW.\(quoteSQLiteIdentifier($0))" }.joined(separator: " OR ")
        condition = capturesExisting
            ? " WHEN EXISTS(SELECT 1 FROM \(quoteSQLiteIdentifier(table)) AS existing WHERE existing.id = NEW.id AND (\(comparisons)))"
            : " WHEN " + comparisons
    }
    if event == mutationBeforeUpdate { condition = condition.replacingOccurrences(of: " WHEN ", with: " WHEN NEW.id IS NOT OLD.id AND ") }
    var body = capturesExisting ? "" : "UPDATE \(coreTableGenerationName) SET generation = generation + 1 WHERE table_name = \(quoteSQLiteString(table));"
    for window in binding.dirtyWindows {
        let prefixes = capturesExisting ? ["EXISTING"] : event == mutationUpdate ? ["OLD", "NEW"] : [event == mutationDelete ? "OLD" : "NEW"]
        for prefix in prefixes {
            let start = capturesExisting
                ? "(SELECT \(quoteSQLiteIdentifier(window.startColumn)) FROM \(quoteSQLiteIdentifier(table)) WHERE id = NEW.id)"
                : "\(prefix).\(quoteSQLiteIdentifier(window.startColumn))"
            let end = capturesExisting
                ? "(SELECT \(quoteSQLiteIdentifier(window.endColumn)) FROM \(quoteSQLiteIdentifier(table)) WHERE id = NEW.id)"
                : "\(prefix).\(quoteSQLiteIdentifier(window.endColumn))"
            let invalid = "typeof(\(start)) != 'integer' OR typeof(\(end)) != 'integer' OR \(start) > \(end)"
            body += "UPDATE \(coreTableDirtyWindowName) SET revision = revision + 1, dirty = 1, full = full OR (\(invalid)), lower_bound = CASE WHEN \(invalid) THEN lower_bound WHEN lower_bound IS NULL THEN \(start) ELSE min(lower_bound, \(start)) END, upper_bound = CASE WHEN \(invalid) THEN upper_bound WHEN upper_bound IS NULL THEN \(end) ELSE max(upper_bound, \(end)) END WHERE table_name = \(quoteSQLiteString(table)) AND window_name = \(quoteSQLiteString(window.name));"
        }
    }
    let timing = event == mutationBeforeInsert ? "BEFORE INSERT" : event == mutationBeforeUpdate ? "BEFORE UPDATE" : "AFTER " + event
    return "CREATE TRIGGER IF NOT EXISTS \(quoteSQLiteIdentifier(mutationTriggerName(table, event))) \(timing) ON \(quoteSQLiteIdentifier(table))\(condition) BEGIN \(body) END"
}

public func tableGenerations(_ q: any DBTX, tables: [String]) throws -> [String: Int64] {
    var result: [String: Int64] = [:]
    for table in tables {
        result[table] = try q.withRows("SELECT generation FROM \(coreTableGenerationName) WHERE table_name = ?", arguments: [table]) { rows in
            guard let row = try rows.next() else { throw ProprDBError("unknown generation table \(table)") }
            return try row.int64(at: 0)
        }
    }
    return result
}

public func registerDirtyWindow(_ q: any DBTX, table: String, window: String, consumer: String) throws {
    guard !table.isEmpty, !window.isEmpty, !consumer.isEmpty else { throw ProprDBError("empty dirty window identity") }
    let registration = try uuidV7()
    try q.withTransaction { transaction in
        try transaction.withRows("SELECT 1 FROM \(coreTableDirtyDefinitionName) WHERE table_name = ? AND window_name = ?", arguments: [table, window]) { rows in
            guard try rows.next() != nil else { throw ProprDBError("unknown dirty window definition") }
        }
        try transaction.execute("INSERT INTO \(coreTableDirtyWindowName) (table_name, window_name, consumer, registration, revision, dirty, full) VALUES (?, ?, ?, ?, 0, 1, 1) ON CONFLICT(table_name, window_name, consumer) DO NOTHING", arguments: [table, window, consumer, registration])
    }
}

public func readDirtyWindow(_ q: any DBTX, table: String, window: String, consumer: String) throws -> DirtyWindowState {
    try q.withRows("SELECT registration, revision, dirty, full, lower_bound, upper_bound FROM \(coreTableDirtyWindowName) WHERE table_name = ? AND window_name = ? AND consumer = ?", arguments: [table, window, consumer]) { rows in
        guard let row = try rows.next() else { throw ProprDBError("unknown dirty window consumer") }
        return try DirtyWindowState(dirty: row.int64(at: 2) != 0, full: row.int64(at: 3) != 0, lower: row.optionalInt64(at: 4), upper: row.optionalInt64(at: 5), token: DirtyWindowToken(table: table, window: window, consumer: consumer, registration: row.string(at: 0), revision: row.int64(at: 1)))
    }
}

public func acknowledgeDirtyWindow(_ q: any DBTX, token: DirtyWindowToken) throws {
    try q.withRows("UPDATE \(coreTableDirtyWindowName) SET revision = revision + 1, dirty = 0, full = 0, lower_bound = NULL, upper_bound = NULL WHERE table_name = ? AND window_name = ? AND consumer = ? AND registration = ? AND revision = ? RETURNING revision", arguments: [token.table, token.window, token.consumer, token.registration, token.revision]) { rows in
        guard try rows.next() != nil else { throw StaleDirtyWindowError() }
    }
}

public func removeDirtyWindowConsumer(_ q: any DBTX, table: String, window: String, consumer: String) throws {
    try q.execute("DELETE FROM \(coreTableDirtyWindowName) WHERE table_name = ? AND window_name = ? AND consumer = ?", arguments: [table, window, consumer])
}
