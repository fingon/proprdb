import CSQLite
import Foundation
import Logging
import Security
import SwiftProtobuf

public let coreTableDeletedName = "_deleted"
public let coreTableSyncName = "_sync"
public let coreTableSchemaStateName = "_proprdb_schema"
public let coreTableUnknownName = "_unknown_types"

public let _deletedTableName = quoteSQLiteIdentifier(coreTableDeletedName)
public let _syncTableName = quoteSQLiteIdentifier(coreTableSyncName)
public let _proprdbSchemaTableName = quoteSQLiteIdentifier(coreTableSchemaStateName)
public let _unknownTypesTableName = quoteSQLiteIdentifier(coreTableUnknownName)

private let dataColumnName = "data"
private let logger = Logger(label: "ProprDBSwiftRuntime")
private let sqliteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

public struct ProprDBError: Error, CustomStringConvertible {
    public let message: String

    public init(_ message: String) {
        self.message = message
    }

    public var description: String {
        message
    }
}

public protocol ProprDBValidatable {
    func valid() throws
}

public func validateForWrite<T>(_ value: T) throws {
    if let validatable = value as? any ProprDBValidatable {
        try validatable.valid()
    }
}

public struct JSONLRecord: Equatable {
    public let id: String
    public let deleted: Bool
    public let atNs: Int64
    public let data: Data

    public init(id: String, deleted: Bool, atNs: Int64, data: Data) {
        self.id = id
        self.deleted = deleted
        self.atNs = atNs
        self.data = data
    }
}

public struct GeneratedTableDescriptor: Equatable {
    public let tableName: String
    public let typeName: String
    public let isCore: Bool
    public let syncEnabled: Bool

    public init(tableName: String, typeName: String, isCore: Bool, syncEnabled: Bool) {
        self.tableName = tableName
        self.typeName = typeName
        self.isCore = isCore
        self.syncEnabled = syncEnabled
    }
}

public struct TableIntrospection: Equatable {
    public let descriptor: GeneratedTableDescriptor
    public let objectCount: Int64
    public let diskUsageBytes: Int64

    public init(descriptor: GeneratedTableDescriptor, objectCount: Int64, diskUsageBytes: Int64) {
        self.descriptor = descriptor
        self.objectCount = objectCount
        self.diskUsageBytes = diskUsageBytes
    }
}

public protocol DBTX: AnyObject {
    var sqliteHandle: OpaquePointer? { get }
}

public final class SQLiteDatabase: DBTX {
    public var sqliteHandle: OpaquePointer?

    public init(path: String) throws {
        var handle: OpaquePointer?
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX | SQLITE_OPEN_URI
        let openResult = sqlite3_open_v2(path, &handle, flags, nil)
        guard openResult == SQLITE_OK, let handle else {
            let errorMessage = sqliteErrorMessage(database: handle)
            if let handle {
                sqlite3_close(handle)
            }
            throw ProprDBError("open sqlite database \(path): \(errorMessage)")
        }
        sqlite3_busy_timeout(handle, 5_000)
        sqlite3_extended_result_codes(handle, 1)
        self.sqliteHandle = handle
    }

    deinit {
        if let sqliteHandle {
            sqlite3_close(sqliteHandle)
        }
    }

    public func close() throws {
        guard let sqliteHandle else {
            return
        }
        let result = sqlite3_close(sqliteHandle)
        if result != SQLITE_OK {
            throw ProprDBError("close sqlite database: \(sqliteErrorMessage(database: sqliteHandle))")
        }
        self.sqliteHandle = nil
    }

    public func beginTransaction() throws -> SQLiteTransaction {
        let transaction = SQLiteTransaction(database: self)
        try transaction.begin()
        return transaction
    }
}

public final class SQLiteTransaction: DBTX {
    public var sqliteHandle: OpaquePointer? {
        database.sqliteHandle
    }

    private let database: SQLiteDatabase
    private var finished = false

    fileprivate init(database: SQLiteDatabase) {
        self.database = database
    }

    fileprivate func begin() throws {
        try execute("BEGIN IMMEDIATE TRANSACTION")
    }

    public func commit() throws {
        try execute("COMMIT")
        finished = true
    }

    public func rollback() throws {
        try execute("ROLLBACK")
        finished = true
    }
}

public final class SQLiteRows {
    private var statement: OpaquePointer?
    private var closed = false

    fileprivate init(statement: OpaquePointer) {
        self.statement = statement
    }

    public func next() throws -> SQLiteRow? {
        guard let statement else {
            throw ProprDBError("sqlite rows already closed")
        }
        let result = sqlite3_step(statement)
        switch result {
        case SQLITE_ROW:
            return SQLiteRow(statement: statement)
        case SQLITE_DONE:
            return nil
        default:
            throw ProprDBError("step sqlite rows: \(sqliteErrorMessage(statement: statement))")
        }
    }

    public func close() throws {
        if closed {
            return
        }
        closed = true
        guard let statement else {
            return
        }
        self.statement = nil
        let database = sqlite3_db_handle(statement)
        let result = sqlite3_finalize(statement)
        if result != SQLITE_OK {
            throw ProprDBError("finalize sqlite rows: \(sqliteErrorMessage(database: database))")
        }
    }
}

public struct SQLiteRow {
    fileprivate let statement: OpaquePointer

    public func string(at column: Int32) throws -> String {
        guard let value = sqlite3_column_text(statement, column) else {
            throw ProprDBError("expected text at column \(column)")
        }
        return String(cString: value)
    }

    public func int64(at column: Int32) throws -> Int64 {
        guard sqlite3_column_type(statement, column) != SQLITE_NULL else {
            throw ProprDBError("expected integer at column \(column)")
        }
        return sqlite3_column_int64(statement, column)
    }

    public func data(at column: Int32) throws -> Data {
        guard sqlite3_column_type(statement, column) != SQLITE_NULL else {
            throw ProprDBError("expected blob at column \(column)")
        }
        let bytes = sqlite3_column_blob(statement, column)
        let count = Int(sqlite3_column_bytes(statement, column))
        guard count >= 0 else {
            throw ProprDBError("invalid blob size at column \(column)")
        }
        if count == 0 {
            return Data()
        }
        guard let bytes else {
            throw ProprDBError("missing blob bytes at column \(column)")
        }
        return Data(bytes: bytes, count: count)
    }
}

public extension DBTX {
    func execute(_ sql: String, arguments: [Any?] = []) throws {
        let statement = try prepare(sql)
        defer {
            sqlite3_finalize(statement)
        }
        try bind(arguments, to: statement)
        let result = sqlite3_step(statement)
        if result != SQLITE_DONE {
            throw ProprDBError("execute sqlite statement: \(sqliteErrorMessage(statement: statement))")
        }
    }

    func query(_ sql: String, arguments: [Any?] = []) throws -> SQLiteRows {
        let statement = try prepare(sql)
        do {
            try bind(arguments, to: statement)
            return SQLiteRows(statement: statement)
        } catch {
            sqlite3_finalize(statement)
            throw error
        }
    }

    func withRows<T>(_ sql: String, arguments: [Any?] = [], _ body: (SQLiteRows) throws -> T) throws -> T {
        let rows = try query(sql, arguments: arguments)
        do {
            let result = try body(rows)
            try rows.close()
            return result
        } catch {
            do {
                try rows.close()
            } catch let closeError {
                throw ProprDBError("\(error) (additionally, \(closeError))")
            }
            throw error
        }
    }

    private func prepare(_ sql: String) throws -> OpaquePointer {
        guard let sqliteHandle else {
            throw ProprDBError("nil DBTX")
        }
        var statement: OpaquePointer?
        let prepareResult = sqlite3_prepare_v2(sqliteHandle, sql, -1, &statement, nil)
        guard prepareResult == SQLITE_OK, let statement else {
            throw ProprDBError("prepare sqlite statement: \(sqliteErrorMessage(database: sqliteHandle))")
        }
        return statement
    }
}

public func ensureCoreTables(_ q: any DBTX) throws {
    try q.execute("CREATE TABLE IF NOT EXISTS \(quoteSQLiteIdentifier(coreTableDeletedName)) (table_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, PRIMARY KEY (table_name, id))")
    try q.execute("CREATE TABLE IF NOT EXISTS \(quoteSQLiteIdentifier(coreTableSyncName)) (object_id TEXT NOT NULL, table_name TEXT NOT NULL, at_ns INTEGER NOT NULL, remote TEXT NOT NULL, PRIMARY KEY (object_id, table_name, remote))")
    try q.execute("CREATE TABLE IF NOT EXISTS \(quoteSQLiteIdentifier(coreTableSchemaStateName)) (table_name TEXT PRIMARY KEY, schema_hash TEXT NOT NULL)")
    try q.execute("CREATE TABLE IF NOT EXISTS \(quoteSQLiteIdentifier(coreTableUnknownName)) (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, deleted INTEGER NOT NULL, data_json TEXT NOT NULL, PRIMARY KEY (type_name, id, at_ns))")
}

public func ensureManagedIndexes(_ q: any DBTX, tableName: String, generatedIndexPrefix: String, createIndexSQL: [String], desiredIndexNames: [String]) throws {
    for statement in createIndexSQL {
        try q.execute(statement)
    }
    let desiredIndexes = Set(desiredIndexNames)
    let indexesToDrop = try q.withRows("SELECT name FROM pragma_index_list(\(quoteSQLiteString(tableName)))") { rows in
        var stale: [String] = []
        while let row = try rows.next() {
            let indexName = try row.string(at: 0)
            if indexName.hasPrefix(generatedIndexPrefix) && !desiredIndexes.contains(indexName) {
                stale.append(indexName)
            }
        }
        return stale
    }
    for indexName in indexesToDrop {
        try q.execute("DROP INDEX IF EXISTS \(quoteSQLiteIdentifier(indexName))")
    }
}

public func nowNs() -> Int64 {
    Int64(Date().timeIntervalSince1970 * 1_000_000_000)
}

public func uuidV7() throws -> String {
    var bytes = [UInt8](repeating: 0, count: 16)
    let result = SecRandomCopyBytes(kSecRandomDefault, bytes.count, &bytes)
    if result != errSecSuccess {
        throw ProprDBError("generate random bytes for uuidv7: \(result)")
    }
    let milliseconds = UInt64(Date().timeIntervalSince1970 * 1_000)
    bytes[0] = UInt8((milliseconds >> 40) & 0xff)
    bytes[1] = UInt8((milliseconds >> 32) & 0xff)
    bytes[2] = UInt8((milliseconds >> 24) & 0xff)
    bytes[3] = UInt8((milliseconds >> 16) & 0xff)
    bytes[4] = UInt8((milliseconds >> 8) & 0xff)
    bytes[5] = UInt8(milliseconds & 0xff)
    bytes[6] = (bytes[6] & 0x0f) | 0x70
    bytes[8] = (bytes[8] & 0x3f) | 0x80
    return String(format: "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                  bytes[0], bytes[1], bytes[2], bytes[3],
                  bytes[4], bytes[5], bytes[6], bytes[7],
                  bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15])
}

public func validateUUID(_ id: String) throws {
    let parts = id.split(separator: "-", omittingEmptySubsequences: false)
    let expectedLengths = [8, 4, 4, 4, 12]
    guard parts.count == expectedLengths.count else {
        throw ProprDBError("invalid uuid \(id): expected 5 parts")
    }
    for (index, part) in parts.enumerated() {
        guard part.count == expectedLengths[index] else {
            throw ProprDBError("invalid uuid \(id): unexpected length for part \(index + 1)")
        }
        guard part.unicodeScalars.allSatisfy({ CharacterSet(charactersIn: "0123456789abcdefABCDEF").contains($0) }) else {
            throw ProprDBError("invalid uuid \(id): invalid hexadecimal content")
        }
    }
}

public func typeURL(_ typeName: String) -> String {
    "type.googleapis.com/" + typeName
}

public func typeNameFromURL(_ typeURLValue: String) -> String {
    guard let lastSlash = typeURLValue.lastIndex(of: "/"), lastSlash < typeURLValue.index(before: typeURLValue.endIndex) else {
        return typeURLValue
    }
    return String(typeURLValue[typeURLValue.index(after: lastSlash)...])
}

public func marshalAnyJSON<M: Message>(_ message: M, typeName: String) throws -> Data {
    let object = try jsonObject(from: message.jsonUTF8Data())
    guard var dictionary = object as? [String: Any] else {
        throw ProprDBError("marshal any as json: expected object payload")
    }
    dictionary["@type"] = typeURL(typeName)
    return try JSONSerialization.data(withJSONObject: dictionary, options: [.sortedKeys])
}

public func marshalTypeOnlyAnyJSON(typeName: String) throws -> Data {
    try JSONSerialization.data(withJSONObject: ["@type": typeURL(typeName)], options: [.sortedKeys])
}

public func decodeAnyJSON<M: Message>(_ data: Data, as type: M.Type) throws -> M {
    let object = try jsonObject(from: data)
    guard var dictionary = object as? [String: Any] else {
        throw ProprDBError("unmarshal any json: expected object payload")
    }
    dictionary.removeValue(forKey: "@type")
    let payload = try JSONSerialization.data(withJSONObject: dictionary, options: [.sortedKeys])
    return try M(jsonUTF8Data: payload)
}

public func encodeJSONLRecord(_ record: JSONLRecord) throws -> String {
    var dictionary: [String: Any] = [
        "id": record.id,
        "atNs": record.atNs,
        "data": try jsonObject(from: record.data),
    ]
    if record.deleted {
        dictionary["deleted"] = true
    }
    let data = try JSONSerialization.data(withJSONObject: dictionary, options: [.sortedKeys])
    guard var line = String(data: data, encoding: .utf8) else {
        throw ProprDBError("encode jsonl line as utf8")
    }
    line.append("\n")
    return line
}

public func readJSONL(text: String, visit: (JSONLRecord, Int) throws -> Void) throws {
    let lines = text.split(separator: "\n", omittingEmptySubsequences: false)
    for (lineIndex, rawLine) in lines.enumerated() {
        let lineNumber = lineIndex + 1
        let line = String(rawLine)
        if line.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            continue
        }
        guard let lineData = line.data(using: .utf8) else {
            throw ProprDBError("decode jsonl line \(lineNumber): invalid utf8")
        }
        guard let object = try JSONSerialization.jsonObject(with: lineData) as? [String: Any] else {
            throw ProprDBError("decode jsonl line \(lineNumber): expected object")
        }
        guard let id = object["id"] as? String else {
            throw ProprDBError("decode jsonl line \(lineNumber): missing id")
        }
        let deleted = object["deleted"] as? Bool ?? false
        let atNsValue = object["atNs"]
        let atNs = try decodeInt64(atNsValue, fieldName: "atNs", lineNumber: lineNumber)
        guard let dataObject = object["data"] else {
            throw ProprDBError("decode jsonl line \(lineNumber): missing data")
        }
        let dataJSON = try JSONSerialization.data(withJSONObject: dataObject, options: [.sortedKeys])
        try visit(JSONLRecord(id: id, deleted: deleted, atNs: atNs, data: dataJSON), lineNumber)
    }
}

public func typeNameFromAnyJSON(_ data: Data) throws -> String {
    let object = try jsonObject(from: data)
    guard let dictionary = object as? [String: Any], let typeValue = dictionary["@type"] as? String else {
        throw ProprDBError("empty @type")
    }
    let typeName = typeNameFromURL(typeValue)
    if typeName.isEmpty {
        throw ProprDBError("empty @type")
    }
    return typeName
}

public func unknownInsert(_ q: any DBTX, typeName: String, record: JSONLRecord) throws {
    guard !typeName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
        throw ProprDBError("empty type name")
    }
    try q.execute("INSERT INTO \(_unknownTypesTableName) (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?)", arguments: [typeName, record.id, record.atNs, record.deleted ? 1 : 0, String(decoding: record.data, as: UTF8.self)])
}

public func compactUnknownLatest(_ q: any DBTX) throws {
    let sql = """
    DELETE FROM \(_unknownTypesTableName) WHERE rowid NOT IN (
    SELECT MAX(kept.rowid)
    FROM \(_unknownTypesTableName) kept
    JOIN (
        SELECT type_name, id, MAX(at_ns) AS max_at_ns
        FROM \(_unknownTypesTableName)
        GROUP BY type_name, id
    ) latest
    ON latest.type_name = kept.type_name AND latest.id = kept.id AND latest.max_at_ns = kept.at_ns
    GROUP BY kept.type_name, kept.id
    )
    """
    try q.execute(sql)
}

public func replayUnknownByType(_ q: any DBTX, typeName: String, apply: (JSONLRecord) throws -> Void) throws {
    guard !typeName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
        throw ProprDBError("empty type name")
    }
    try compactUnknownLatest(q)
    let replayRows = try q.withRows("SELECT id, at_ns, deleted, data_json FROM \(_unknownTypesTableName) WHERE type_name = ? ORDER BY at_ns ASC, id ASC, rowid ASC", arguments: [typeName]) { rows in
        var buffered: [JSONLRecord] = []
        while let row = try rows.next() {
            let id = try row.string(at: 0)
            let atNs = try row.int64(at: 1)
            let deleted = try row.int64(at: 2) != 0
            let dataJSON = try row.string(at: 3)
            buffered.append(JSONLRecord(id: id, deleted: deleted, atNs: atNs, data: Data(dataJSON.utf8)))
        }
        return buffered
    }
    for record in replayRows {
        try apply(record)
        try q.execute("DELETE FROM \(_unknownTypesTableName) WHERE type_name = ? AND id = ?", arguments: [typeName, record.id])
    }
}

public func syncNeedsSend(_ q: any DBTX, objectID: String, tableName: String, remote: String, atNs: Int64) throws -> Bool {
    if remote.isEmpty {
        return true
    }
    let syncedAtNs = try q.withRows("SELECT at_ns FROM \(_syncTableName) WHERE object_id = ? AND table_name = ? AND remote = ?", arguments: [objectID, tableName, remote]) { rows in
        try rows.next()?.int64(at: 0)
    }
    guard let syncedAtNs else {
        return true
    }
    return syncedAtNs < atNs
}

public func syncUpsert(_ q: any DBTX, objectID: String, tableName: String, remote: String, atNs: Int64) throws {
    if remote.isEmpty {
        return
    }
    try q.execute("INSERT INTO \(_syncTableName) (object_id, table_name, at_ns, remote) VALUES (?, ?, ?, ?) ON CONFLICT(object_id, table_name, remote) DO UPDATE SET at_ns = CASE WHEN excluded.at_ns > at_ns THEN excluded.at_ns ELSE at_ns END", arguments: [objectID, tableName, atNs, remote])
}

public func localMaxAtNs(_ q: any DBTX, tableName: String, objectID: String) throws -> Int64 {
    let quotedTableName = quoteSQLiteIdentifier(tableName)
    let rowAtNs = try q.withRows("SELECT at_ns FROM \(quotedTableName) WHERE id = ?", arguments: [objectID]) { rows in
        try rows.next()?.int64(at: 0)
    }
    let tombstoneAtNs = try q.withRows("SELECT at_ns FROM \(_deletedTableName) WHERE table_name = ? AND id = ?", arguments: [tableName, objectID]) { rows in
        try rows.next()?.int64(at: 0)
    }
    return max(rowAtNs ?? -1, tombstoneAtNs ?? -1)
}

public func introspectTables(_ q: any DBTX, descriptors: [GeneratedTableDescriptor]) throws -> [TableIntrospection] {
    var result: [TableIntrospection] = []
    for descriptor in descriptors {
        result.append(TableIntrospection(
            descriptor: descriptor,
            objectCount: try tableObjectCount(q, tableName: descriptor.tableName),
            diskUsageBytes: try tableDiskUsageBytes(q, tableName: descriptor.tableName)
        ))
    }
    return result
}

public func logIgnoredUnsyncedJSONLRecord(typeName: String, id: String, remote: String, lineNumber: Int) {
    logger.error("ignoring unsynced jsonl record", metadata: [
        "type": .string(typeName),
        "id": .string(id),
        "remote": .string(remote),
        "line": .stringConvertible(lineNumber),
    ])
}

public func quoteSQLiteIdentifier(_ value: String) -> String {
    "\"\(value.replacingOccurrences(of: "\"", with: "\"\""))\""
}

private func sqliteErrorMessage(statement: OpaquePointer?) -> String {
    if let database = sqlite3_db_handle(statement), let cString = sqlite3_errmsg(database) {
        return String(cString: cString)
    }
    return "unknown sqlite error"
}

private func sqliteErrorMessage(database: OpaquePointer?) -> String {
    if let database, let cString = sqlite3_errmsg(database) {
        return String(cString: cString)
    }
    return "unknown sqlite error"
}

private func bind(_ arguments: [Any?], to statement: OpaquePointer) throws {
    if sqlite3_bind_parameter_count(statement) != Int32(arguments.count) {
        throw ProprDBError("bind sqlite statement: expected \(sqlite3_bind_parameter_count(statement)) arguments, got \(arguments.count)")
    }
    for (index, value) in arguments.enumerated() {
        let parameterIndex = Int32(index + 1)
        switch value {
        case nil:
            if sqlite3_bind_null(statement, parameterIndex) != SQLITE_OK {
                throw ProprDBError("bind sqlite null: \(sqliteErrorMessage(statement: statement))")
            }
        case let value as String:
            if sqlite3_bind_text(statement, parameterIndex, value, -1, sqliteTransient) != SQLITE_OK {
                throw ProprDBError("bind sqlite text: \(sqliteErrorMessage(statement: statement))")
            }
        case let value as Int:
            if sqlite3_bind_int64(statement, parameterIndex, Int64(value)) != SQLITE_OK {
                throw ProprDBError("bind sqlite integer: \(sqliteErrorMessage(statement: statement))")
            }
        case let value as Int64:
            if sqlite3_bind_int64(statement, parameterIndex, value) != SQLITE_OK {
                throw ProprDBError("bind sqlite integer: \(sqliteErrorMessage(statement: statement))")
            }
        case let value as Bool:
            if sqlite3_bind_int64(statement, parameterIndex, value ? 1 : 0) != SQLITE_OK {
                throw ProprDBError("bind sqlite bool: \(sqliteErrorMessage(statement: statement))")
            }
        case let value as Double:
            if sqlite3_bind_double(statement, parameterIndex, value) != SQLITE_OK {
                throw ProprDBError("bind sqlite double: \(sqliteErrorMessage(statement: statement))")
            }
        case let value as Float:
            if sqlite3_bind_double(statement, parameterIndex, Double(value)) != SQLITE_OK {
                throw ProprDBError("bind sqlite float: \(sqliteErrorMessage(statement: statement))")
            }
        case let value as Data:
            try value.withUnsafeBytes { buffer in
                let pointer = buffer.baseAddress?.assumingMemoryBound(to: UInt8.self)
                let result = sqlite3_bind_blob(statement, parameterIndex, pointer, Int32(buffer.count), sqliteTransient)
                if result != SQLITE_OK {
                    throw ProprDBError("bind sqlite blob: \(sqliteErrorMessage(statement: statement))")
                }
            }
        default:
            throw ProprDBError("unsupported sqlite bind value type \(String(describing: value))")
        }
    }
}

private func jsonObject(from data: Data) throws -> Any {
    try JSONSerialization.jsonObject(with: data)
}

private func decodeInt64(_ value: Any?, fieldName: String, lineNumber: Int) throws -> Int64 {
    switch value {
    case let number as NSNumber:
        return number.int64Value
    case let string as String:
        guard let parsed = Int64(string) else {
            throw ProprDBError("decode jsonl line \(lineNumber): invalid \(fieldName)")
        }
        return parsed
    default:
        throw ProprDBError("decode jsonl line \(lineNumber): missing \(fieldName)")
    }
}

private func tableObjectCount(_ q: any DBTX, tableName: String) throws -> Int64 {
    let quotedTableName = quoteSQLiteIdentifier(tableName)
    let objectCount = try q.withRows("SELECT COUNT(*) FROM \(quotedTableName)") { rows in
        guard let row = try rows.next() else {
            return Int64(0)
        }
        return try row.int64(at: 0)
    }
    return objectCount
}

private func tableDiskUsageBytes(_ q: any DBTX, tableName: String) throws -> Int64 {
    let columnNames = try tableColumnNames(q, tableName: tableName)
    let quotedTableName = quoteSQLiteIdentifier(tableName)
    let query: String
    if columnNames.contains(dataColumnName) {
        query = "SELECT COALESCE(SUM(LENGTH(\(quoteSQLiteIdentifier(dataColumnName)))), 0) FROM \(quotedTableName)"
    } else {
        query = "SELECT COALESCE(SUM(\(estimatedRowPayloadBytesSQL(columnNames: columnNames))), 0) FROM \(quotedTableName)"
    }
    return try q.withRows(query) { rows in
        guard let row = try rows.next() else {
            return Int64(0)
        }
        return try row.int64(at: 0)
    }
}

private func tableColumnNames(_ q: any DBTX, tableName: String) throws -> [String] {
    try q.withRows("PRAGMA table_info(\(quoteSQLiteIdentifier(tableName)))") { rows in
        var names: [String] = []
        while let row = try rows.next() {
            names.append(try row.string(at: 1))
        }
        return names
    }
}

private func estimatedRowPayloadBytesSQL(columnNames: [String]) -> String {
    if columnNames.isEmpty {
        return "0"
    }
    return columnNames.map { "COALESCE(LENGTH(CAST(\(quoteSQLiteIdentifier($0)) AS BLOB)), 0)" }.joined(separator: " + ")
}

public func quoteSQLiteString(_ value: String) -> String {
    "'\(value.replacingOccurrences(of: "'", with: "''"))'"
}
