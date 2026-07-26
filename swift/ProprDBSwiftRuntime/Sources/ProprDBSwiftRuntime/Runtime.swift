import CSQLite
import Foundation
import Logging
import Security
import SwiftProtobuf

public let coreTableDeletedName = "_deleted"
public let coreTableSyncName = "_sync"
public let coreTableSchemaStateName = "_proprdb_schema"
public let coreTableUnknownName = "_unknown_types"
public let coreTableUnknownSyncName = "_unknown_sync"
public let coreTableMetadataName = "_proprdb_metadata"
public let coreTableExportBatchName = "_export_batches"
public let coreTableExportEntryName = "_export_batch_entries"
public let coreTableQueryStatName = "_querystat"

public let _deletedTableName = quoteSQLiteIdentifier(coreTableDeletedName)
public let _syncTableName = quoteSQLiteIdentifier(coreTableSyncName)
public let _proprdbSchemaTableName = quoteSQLiteIdentifier(coreTableSchemaStateName)
public let _unknownTypesTableName = quoteSQLiteIdentifier(coreTableUnknownName)
public let _unknownSyncTableName = quoteSQLiteIdentifier(coreTableUnknownSyncName)
public let _metadataTableName = quoteSQLiteIdentifier(coreTableMetadataName)
public let _exportBatchesTableName = quoteSQLiteIdentifier(coreTableExportBatchName)
public let _exportBatchEntriesTableName = quoteSQLiteIdentifier(coreTableExportEntryName)
public let _queryStatTableName = quoteSQLiteIdentifier(coreTableQueryStatName)

private let dataColumnName = "data"
private let logger = Logger(label: "ProprDBSwiftRuntime")
private let sqliteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
private let nanosecondsPerSecond: Int64 = 1_000_000_000
private let attosecondsPerNanosecond: Int64 = 1_000_000_000
private let upsertQueryStatisticSQL = """
INSERT INTO \(_queryStatTableName) (table_name, query, calls, duration_sum_ns)
VALUES (?, ?, 1, ?)
ON CONFLICT(table_name, query) DO UPDATE SET
calls = calls + excluded.calls,
duration_sum_ns = duration_sum_ns + excluded.duration_sum_ns
"""
private let selectQueryStatisticsSQL = """
SELECT table_name, query, calls, duration_sum_ns
FROM \(_queryStatTableName)
ORDER BY table_name, query
"""
private let clearQueryStatisticsSQL = "DELETE FROM \(_queryStatTableName)"

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

public struct JSONLRecord: Equatable, Sendable {
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

public struct JSONLCheckpoint: Codable, Equatable, Sendable {
    public let version: Int
    public let databaseId: String
    public let batchId: String

    public func serialized() throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        return String(decoding: try encoder.encode(self), as: UTF8.self)
    }

    public static func parse(_ text: String) throws -> Self {
        guard let data = text.data(using: .utf8) else {
            throw ProprDBError("checkpoint is not utf8")
        }
        let checkpoint = try JSONDecoder().decode(Self.self, from: data)
        guard checkpoint.version == 1, !checkpoint.databaseId.isEmpty, !checkpoint.batchId.isEmpty else {
            throw ProprDBError("invalid jsonl checkpoint")
        }
        return checkpoint
    }
}

public struct PreparedJSONLExport: Equatable, Sendable {
    public let text: String
    public let checkpoint: JSONLCheckpoint
}

public struct ConflictError: Error, Equatable, Sendable, CustomStringConvertible {
    public let typeName: String
    public let id: String
    public let atNs: Int64
    public let localDeleted: Bool
    public let remoteDeleted: Bool

    public var description: String {
        "conflicting state type=\(typeName) id=\(id) at_ns=\(atNs) local_deleted=\(localDeleted) remote_deleted=\(remoteDeleted)"
    }
}

public enum TableChange<Value: Sendable>: Sendable {
    case upsert(id: String, atNs: Int64, data: Value)
    case delete(id: String, atNs: Int64)
}

public struct GeneratedTableDescriptor: Equatable, Sendable {
    public let tableName: String
    public let typeName: String
    public let isCore: Bool
    public let syncEnabled: Bool
    public let changeListenersEnabled: Bool
    public let queryStatisticsEnabled: Bool

    public init(
        tableName: String,
        typeName: String,
        isCore: Bool,
        syncEnabled: Bool,
        changeListenersEnabled: Bool = false,
        queryStatisticsEnabled: Bool = false
    ) {
        self.tableName = tableName
        self.typeName = typeName
        self.isCore = isCore
        self.syncEnabled = syncEnabled
        self.changeListenersEnabled = changeListenersEnabled
        self.queryStatisticsEnabled = queryStatisticsEnabled
    }
}

public enum SQLiteBindValue: Sendable, ExpressibleByStringLiteral, ExpressibleByIntegerLiteral, ExpressibleByFloatLiteral, ExpressibleByBooleanLiteral {
    case null
    case string(String)
    case int64(Int64)
    case double(Double)
    case bool(Bool)
    case data(Data)

    public init(stringLiteral value: String) {
        self = .string(value)
    }

    public init(integerLiteral value: Int64) {
        self = .int64(value)
    }

    public init(floatLiteral value: Double) {
        self = .double(value)
    }

    public init(booleanLiteral value: Bool) {
        self = .bool(value)
    }

    fileprivate var sqliteValue: Any? {
        switch self {
        case .null: nil
        case let .string(value): value
        case let .int64(value): value
        case let .double(value): value
        case let .bool(value): value
        case let .data(value): value
        }
    }
}

public func sqliteBindValue(_ value: String) -> SQLiteBindValue { .string(value) }
public func sqliteBindValue(_ value: Bool) -> SQLiteBindValue { .bool(value) }
public func sqliteBindValue(_ value: Int32) -> SQLiteBindValue { .int64(Int64(value)) }
public func sqliteBindValue(_ value: Int64) -> SQLiteBindValue { .int64(value) }
public func sqliteBindValue(_ value: UInt32) -> SQLiteBindValue { .int64(Int64(value)) }
public func sqliteBindValue(_ value: UInt64) -> SQLiteBindValue { .int64(Int64(bitPattern: value)) }
public func sqliteBindValue(_ value: Float) -> SQLiteBindValue { .double(Double(value)) }
public func sqliteBindValue(_ value: Double) -> SQLiteBindValue { .double(value) }
public func sqliteBindValue(_ value: Data) -> SQLiteBindValue { .data(value) }
public func sqliteBindValue<T: Enum>(_ value: T) -> SQLiteBindValue { .int64(Int64(value.rawValue)) }

public struct GeneratedTableBinding: @unchecked Sendable {
    public let descriptor: GeneratedTableDescriptor
    public let messageType: any Message.Type
    public let insertSQL: String
    public let upsertSQL: String
    public let createTableSQL: String
    public let projectionSchema: String
    public let projectedColumns: [ProjectedColumnDescriptor]
    public let generatedIndexes: [GeneratedIndexDescriptor]
    public let generatedIndexPrefix: String
    public let decodeAnyJSON: @Sendable (Data) throws -> any Message
    public let decodeBinary: @Sendable (Data) throws -> any Message
    public let encodeAnyJSON: @Sendable (any Message) throws -> Data
    public let messagesEqual: @Sendable (any Message, any Message) -> Bool
    public let projectedValues: @Sendable (any Message) throws -> [SQLiteBindValue]

    public init(
        descriptor: GeneratedTableDescriptor,
        messageType: any Message.Type,
        insertSQL: String,
        upsertSQL: String,
        createTableSQL: String,
        projectionSchema: String,
        projectedColumns: [ProjectedColumnDescriptor],
        generatedIndexes: [GeneratedIndexDescriptor],
        generatedIndexPrefix: String,
        decodeAnyJSON: @escaping @Sendable (Data) throws -> any Message,
        decodeBinary: @escaping @Sendable (Data) throws -> any Message,
        encodeAnyJSON: @escaping @Sendable (any Message) throws -> Data,
        messagesEqual: @escaping @Sendable (any Message, any Message) -> Bool,
        projectedValues: @escaping @Sendable (any Message) throws -> [SQLiteBindValue]
    ) {
        self.descriptor = descriptor
        self.messageType = messageType
        self.insertSQL = insertSQL
        self.upsertSQL = upsertSQL
        self.createTableSQL = createTableSQL
        self.projectionSchema = projectionSchema
        self.projectedColumns = projectedColumns
        self.generatedIndexes = generatedIndexes
        self.generatedIndexPrefix = generatedIndexPrefix
        self.decodeAnyJSON = decodeAnyJSON
        self.decodeBinary = decodeBinary
        self.encodeAnyJSON = encodeAnyJSON
        self.messagesEqual = messagesEqual
        self.projectedValues = projectedValues
    }
}

public struct ProjectedColumnDescriptor: Sendable {
    public let name: String
    public let protoKind: String
    public let sqliteType: String
    public let defaultSQL: String
    public let nullable: Bool
    public let legacyOneofPresenceRepair: Bool

    public init(name: String, protoKind: String, sqliteType: String, defaultSQL: String, nullable: Bool, legacyOneofPresenceRepair: Bool) {
        self.name = name
        self.protoKind = protoKind
        self.sqliteType = sqliteType
        self.defaultSQL = defaultSQL
        self.nullable = nullable
        self.legacyOneofPresenceRepair = legacyOneofPresenceRepair
    }
}

public struct GeneratedIndexDescriptor: Sendable {
    public let name: String
    public let createSQL: String

    public init(name: String, createSQL: String) {
        self.name = name
        self.createSQL = createSQL
    }
}

public func coreTableDescriptors() -> [GeneratedTableDescriptor] {
    [
        GeneratedTableDescriptor(tableName: coreTableDeletedName, typeName: "", isCore: true, syncEnabled: false),
        GeneratedTableDescriptor(tableName: coreTableSyncName, typeName: "", isCore: true, syncEnabled: false),
        GeneratedTableDescriptor(tableName: coreTableSchemaStateName, typeName: "", isCore: true, syncEnabled: false),
        GeneratedTableDescriptor(tableName: coreTableUnknownName, typeName: "", isCore: true, syncEnabled: false),
        GeneratedTableDescriptor(tableName: coreTableUnknownSyncName, typeName: "", isCore: true, syncEnabled: false),
        GeneratedTableDescriptor(tableName: coreTableMetadataName, typeName: "", isCore: true, syncEnabled: false),
        GeneratedTableDescriptor(tableName: coreTableExportBatchName, typeName: "", isCore: true, syncEnabled: false),
        GeneratedTableDescriptor(tableName: coreTableExportEntryName, typeName: "", isCore: true, syncEnabled: false),
        GeneratedTableDescriptor(tableName: coreTableQueryStatName, typeName: "", isCore: true, syncEnabled: false),
    ]
}

public struct TableIntrospection: Equatable {
    public let descriptor: GeneratedTableDescriptor
    public let objectCount: Int64
    public let payloadBytes: Int64

    public init(descriptor: GeneratedTableDescriptor, objectCount: Int64, payloadBytes: Int64) {
        self.descriptor = descriptor
        self.objectCount = objectCount
        self.payloadBytes = payloadBytes
    }
}

public struct QueryStatistic: Equatable, Sendable {
    public let tableName: String
    public let query: String
    public let calls: Int64
    public let durationSumNs: Int64
}

public protocol DBTX: AnyObject {
    var sqliteHandle: OpaquePointer? { get }
    func withTransaction<T>(_ body: (any DBTX) throws -> T) throws -> T
}

private struct GeneratedTableChange: Sendable {
    let tableName: String
    let id: String
    let atNs: Int64
    let deleted: Bool
    let message: (any Message)?
}

private struct TableChangeSubscriber: Sendable {
    let yield: @Sendable (GeneratedTableChange) -> Void
    let finish: @Sendable () -> Void
}

private final class TableChangeBroker: @unchecked Sendable {
    private let lock = NSLock()
    private var subscribers: [String: [UUID: TableChangeSubscriber]] = [:]
    private var finished = false

    func add(tableName: String, id: UUID, subscriber: TableChangeSubscriber) {
        lock.lock()
        if finished {
            lock.unlock()
            subscriber.finish()
            return
        }
        subscribers[tableName, default: [:]][id] = subscriber
        lock.unlock()
    }

    func remove(tableName: String, id: UUID) {
        lock.lock()
        subscribers[tableName]?[id] = nil
        if subscribers[tableName]?.isEmpty == true {
            subscribers[tableName] = nil
        }
        lock.unlock()
    }

    func hasSubscribers(tableName: String) -> Bool {
        lock.lock()
        let result = subscribers[tableName]?.isEmpty == false
        lock.unlock()
        return result
    }

    func publish(_ changes: [GeneratedTableChange]) {
        for change in changes {
            lock.lock()
            let currentSubscribers = subscribers[change.tableName].map { Array($0.values) } ?? []
            lock.unlock()
            for subscriber in currentSubscribers {
                subscriber.yield(change)
            }
        }
    }

    func finish() {
        lock.lock()
        guard !finished else {
            lock.unlock()
            return
        }
        finished = true
        let currentSubscribers = subscribers.values.flatMap { Array($0.values) }
        subscribers.removeAll()
        lock.unlock()
        for subscriber in currentSubscribers {
            subscriber.finish()
        }
    }
}

private protocol TableChangeBrokerProvider: AnyObject {
    var tableChangeBroker: TableChangeBroker { get }
}

private final class TableChangeCollector {
    var changes: [GeneratedTableChange] = []
}

private final class ChangeTrackingDBTX: DBTX, TableChangeBrokerProvider {
    let inner: any DBTX
    let tableChangeBroker: TableChangeBroker
    private let collector: TableChangeCollector?

    var sqliteHandle: OpaquePointer? {
        inner.sqliteHandle
    }

    init(_ inner: any DBTX, broker: TableChangeBroker, collector: TableChangeCollector? = nil) {
        self.inner = inner
        tableChangeBroker = broker
        self.collector = collector
    }

    func withTransaction<T>(_ body: (any DBTX) throws -> T) throws -> T {
        let childCollector = TableChangeCollector()
        let result = try inner.withTransaction { transaction in
            try body(ChangeTrackingDBTX(transaction, broker: tableChangeBroker, collector: childCollector))
        }
        if let collector {
            collector.changes.append(contentsOf: childCollector.changes)
        } else {
            tableChangeBroker.publish(childCollector.changes)
        }
        return result
    }

    func queue(_ change: GeneratedTableChange) {
        if let collector {
            collector.changes.append(change)
        } else {
            tableChangeBroker.publish([change])
        }
    }
}

public func withChangeListeners(_ q: any DBTX) -> any DBTX {
    if let tracked = q as? ChangeTrackingDBTX {
        return tracked
    }
    if let provider = q as? any TableChangeBrokerProvider {
        return ChangeTrackingDBTX(q, broker: provider.tableChangeBroker)
    }
    return ChangeTrackingDBTX(q, broker: TableChangeBroker())
}

public func tableChanges<Value: Message>(
    _ q: any DBTX,
    tableName: String,
    as _: Value.Type
) -> AsyncStream<TableChange<Value>> {
    guard let tracked = withChangeListeners(q) as? ChangeTrackingDBTX else {
        preconditionFailure("change listener wrapper has unexpected type")
    }
    let broker = tracked.tableChangeBroker
    return AsyncStream(bufferingPolicy: .unbounded) { continuation in
        let id = UUID()
        continuation.onTermination = { @Sendable _ in
            broker.remove(tableName: tableName, id: id)
        }
        broker.add(
            tableName: tableName,
            id: id,
            subscriber: TableChangeSubscriber(
                yield: { change in
                    if change.deleted {
                        continuation.yield(.delete(id: change.id, atNs: change.atNs))
                        return
                    }
                    guard let data = change.message as? Value else {
                        preconditionFailure("change listener type mismatch for table \(change.tableName)")
                    }
                    continuation.yield(.upsert(id: change.id, atNs: change.atNs, data: data))
                },
                finish: {
                    continuation.finish()
                }
            )
        )
    }
}

public final class SQLiteDatabase: DBTX {
    public var sqliteHandle: OpaquePointer?
    fileprivate let tableChangeBroker = TableChangeBroker()

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
        guard sqlite3_busy_timeout(handle, 5_000) == SQLITE_OK else {
            let message = sqliteErrorMessage(database: handle)
            _ = sqlite3_close(handle)
            throw ProprDBError("configure sqlite busy timeout: \(message)")
        }
        guard sqlite3_extended_result_codes(handle, 1) == SQLITE_OK else {
            let message = sqliteErrorMessage(database: handle)
            _ = sqlite3_close(handle)
            throw ProprDBError("configure sqlite extended result codes: \(message)")
        }
        self.sqliteHandle = handle
    }

    deinit {
        if let sqliteHandle {
            let result = sqlite3_close(sqliteHandle)
            if result != SQLITE_OK {
                logger.error("fallback sqlite close failed", metadata: ["result": .stringConvertible(result)])
            }
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
        tableChangeBroker.finish()
    }

    public func beginTransaction() throws -> SQLiteTransaction {
        let transaction = SQLiteTransaction(database: self)
        try transaction.begin()
        return transaction
    }

    public func withTransaction<T>(_ body: (any DBTX) throws -> T) throws -> T {
        let transaction = try beginTransaction()
        do {
            let result = try body(transaction)
            try transaction.commit()
            return result
        } catch {
            do {
                try transaction.rollback()
            } catch let rollbackError {
                throw ProprDBError("\(error) (additionally, rollback transaction: \(rollbackError))")
            }
            throw error
        }
    }
}

public actor ProprDBActor {
    private let database: SQLiteDatabase

    public init(path: String) throws {
        database = try SQLiteDatabase(path: path)
    }

    public func withDatabase<T: Sendable>(_ body: (SQLiteDatabase) throws -> T) throws -> T {
        try body(database)
    }

    public func close() throws {
        try database.close()
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
        guard !finished else {
            throw ProprDBError("transaction already finished")
        }
        try execute("COMMIT")
        finished = true
    }

    public func rollback() throws {
        guard !finished else {
            throw ProprDBError("transaction already finished")
        }
        try execute("ROLLBACK")
        finished = true
    }

    public func withTransaction<T>(_ body: (any DBTX) throws -> T) throws -> T {
        let savepoint = "proprdb_\(UUID().uuidString.replacingOccurrences(of: "-", with: ""))"
        try execute("SAVEPOINT \(savepoint)")
        do {
            let result = try body(self)
            try execute("RELEASE \(savepoint)")
            return result
        } catch {
            do {
                try execute("ROLLBACK TO \(savepoint)")
                try execute("RELEASE \(savepoint)")
            } catch let rollbackError {
                throw ProprDBError("\(error) (additionally, rollback savepoint: \(rollbackError))")
            }
            throw error
        }
    }

    deinit {
        if !finished {
            do {
                try rollback()
            } catch {
                logger.error("fallback sqlite rollback failed", metadata: ["error": .string(String(describing: error))])
            }
        }
    }
}

extension SQLiteDatabase: TableChangeBrokerProvider {}

extension SQLiteTransaction: TableChangeBrokerProvider {
    fileprivate var tableChangeBroker: TableChangeBroker {
        database.tableChangeBroker
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
            var values: [SQLiteValue] = []
            for column in 0 ..< sqlite3_column_count(statement) {
                values.append(try SQLiteValue(statement: statement, column: column))
            }
            return SQLiteRow(values: values)
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

    deinit {
        if !closed {
            do {
                try close()
            } catch {
                logger.error("fallback sqlite rows finalize failed", metadata: ["error": .string(String(describing: error))])
            }
        }
    }
}

public struct SQLiteRow {
    fileprivate let values: [SQLiteValue]

    public func string(at column: Int32) throws -> String {
        guard case let .text(value) = try value(at: column) else {
            throw ProprDBError("expected text at column \(column)")
        }
        return value
    }

    public func optionalString(at column: Int32) throws -> String? {
        switch try value(at: column) {
        case .null:
            return nil
        case let .text(value):
            return value
        default:
            throw ProprDBError("expected nullable text at column \(column)")
        }
    }

    public func int64(at column: Int32) throws -> Int64 {
        guard case let .integer(value) = try value(at: column) else {
            throw ProprDBError("expected integer at column \(column)")
        }
        return value
    }

    public func data(at column: Int32) throws -> Data {
        guard case let .blob(value) = try value(at: column) else {
            throw ProprDBError("expected blob at column \(column)")
        }
        return value
    }

    private func value(at column: Int32) throws -> SQLiteValue {
        let index = Int(column)
        guard values.indices.contains(index) else {
            throw ProprDBError("sqlite column \(column) out of range")
        }
        return values[index]
    }
}

private enum SQLiteValue {
    case null
    case integer(Int64)
    case real(Double)
    case text(String)
    case blob(Data)

    init(statement: OpaquePointer, column: Int32) throws {
        switch sqlite3_column_type(statement, column) {
        case SQLITE_NULL:
            self = .null
        case SQLITE_INTEGER:
            self = .integer(sqlite3_column_int64(statement, column))
        case SQLITE_FLOAT:
            self = .real(sqlite3_column_double(statement, column))
        case SQLITE_TEXT:
            let count = Int(sqlite3_column_bytes(statement, column))
            guard count >= 0, let pointer = sqlite3_column_text(statement, column) else {
                throw ProprDBError("read sqlite text at column \(column)")
            }
            let data = Data(bytes: pointer, count: count)
            guard let value = String(data: data, encoding: .utf8) else {
                throw ProprDBError("invalid utf8 text at column \(column)")
            }
            self = .text(value)
        case SQLITE_BLOB:
            let count = Int(sqlite3_column_bytes(statement, column))
            if count == 0 {
                self = .blob(Data())
            } else if let pointer = sqlite3_column_blob(statement, column) {
                self = .blob(Data(bytes: pointer, count: count))
            } else {
                throw ProprDBError("read sqlite blob at column \(column)")
            }
        default:
            throw ProprDBError("unsupported sqlite type at column \(column)")
        }
    }
}

public extension DBTX {
    func execute(_ sql: String, arguments: [Any?] = []) throws {
        let statement = try prepare(sql)
        do {
            try bind(arguments, to: statement)
            let result = sqlite3_step(statement)
            if result != SQLITE_DONE {
                throw ProprDBError("execute sqlite statement: \(sqliteErrorMessage(statement: statement))")
            }
            let finalizeResult = sqlite3_finalize(statement)
            if finalizeResult != SQLITE_OK {
                throw ProprDBError("finalize sqlite statement: result=\(finalizeResult)")
            }
        } catch {
            let finalizeResult = sqlite3_finalize(statement)
            if finalizeResult != SQLITE_OK {
                throw ProprDBError("\(error) (additionally, finalize sqlite statement: result=\(finalizeResult))")
            }
            throw error
        }
    }

    func query(_ sql: String, arguments: [Any?] = []) throws -> SQLiteRows {
        let statement = try prepare(sql)
        do {
            try bind(arguments, to: statement)
            return SQLiteRows(statement: statement)
        } catch {
            let finalizeResult = sqlite3_finalize(statement)
            if finalizeResult != SQLITE_OK {
                throw ProprDBError("\(error) (additionally, finalize sqlite query: result=\(finalizeResult))")
            }
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

    func withRows<T>(
        _ sql: String,
        bindValues: [SQLiteBindValue],
        _ body: (SQLiteRows) throws -> T
    ) throws -> T {
        try withRows(sql, arguments: bindValues.map(\.sqliteValue), body)
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

public func measureQuery<T>(
    _ q: any DBTX,
    tableName: String,
    query: String,
    body: () throws -> T
) throws -> T {
    guard !tableName.isEmpty else {
        throw ProprDBError("empty table name")
    }
    guard !query.isEmpty else {
        throw ProprDBError("empty query")
    }
    let clock = ContinuousClock()
    let startedAt = clock.now
    let result = try body()
    let durationSumNs = try durationNanoseconds(startedAt.duration(to: clock.now))
    try q.execute(upsertQueryStatisticSQL, arguments: [tableName, query, durationSumNs])
    return result
}

public func queryStatistics(_ q: any DBTX) throws -> [QueryStatistic] {
    try q.withRows(selectQueryStatisticsSQL) { rows in
        var statistics: [QueryStatistic] = []
        while let row = try rows.next() {
            statistics.append(QueryStatistic(
                tableName: try row.string(at: 0),
                query: try row.string(at: 1),
                calls: try row.int64(at: 2),
                durationSumNs: try row.int64(at: 3)
            ))
        }
        return statistics
    }
}

public func clearQueryStatistics(_ q: any DBTX) throws {
    try q.execute(clearQueryStatisticsSQL)
}

private func durationNanoseconds(_ duration: Duration) throws -> Int64 {
    let components = duration.components
    let (secondsNs, secondsOverflow) = components.seconds.multipliedReportingOverflow(by: nanosecondsPerSecond)
    guard !secondsOverflow else {
        throw ProprDBError("query duration exceeds nanosecond range")
    }
    let fractionalNs = components.attoseconds / attosecondsPerNanosecond
    let (durationNs, durationOverflow) = secondsNs.addingReportingOverflow(fractionalNs)
    guard !durationOverflow, durationNs >= 0 else {
        throw ProprDBError("query duration is outside nanosecond range")
    }
    return durationNs
}

public func ensureCoreTables(_ q: any DBTX) throws {
    try q.withTransaction { transaction in
        try transaction.execute("CREATE TABLE IF NOT EXISTS \(_deletedTableName) (table_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, PRIMARY KEY (table_name, id))")
        try transaction.execute("CREATE TABLE IF NOT EXISTS \(_syncTableName) (object_id TEXT NOT NULL, table_name TEXT NOT NULL, at_ns INTEGER NOT NULL, remote TEXT NOT NULL, PRIMARY KEY (object_id, table_name, remote))")
        try transaction.execute("CREATE TABLE IF NOT EXISTS \(_proprdbSchemaTableName) (table_name TEXT PRIMARY KEY, schema_hash TEXT NOT NULL)")
        try transaction.execute("CREATE TABLE IF NOT EXISTS \(_metadataTableName) (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        try transaction.execute("CREATE TABLE IF NOT EXISTS \(_unknownSyncTableName) (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, remote TEXT NOT NULL, PRIMARY KEY (type_name, id, remote))")
        try transaction.execute("CREATE TABLE IF NOT EXISTS \(_exportBatchesTableName) (batch_id TEXT PRIMARY KEY, database_id TEXT NOT NULL, remote TEXT NOT NULL, complete INTEGER NOT NULL DEFAULT 0)")
        try transaction.execute("CREATE TABLE IF NOT EXISTS \(_exportBatchEntriesTableName) (batch_id TEXT NOT NULL, sequence INTEGER NOT NULL, table_name TEXT NOT NULL, object_id TEXT NOT NULL, at_ns INTEGER NOT NULL, record_json BLOB, PRIMARY KEY (batch_id, sequence), FOREIGN KEY (batch_id) REFERENCES \(_exportBatchesTableName)(batch_id) ON DELETE CASCADE)")
        try transaction.execute("CREATE TABLE IF NOT EXISTS \(_queryStatTableName) (table_name TEXT NOT NULL, query TEXT NOT NULL, calls INTEGER NOT NULL, duration_sum_ns INTEGER NOT NULL, PRIMARY KEY (table_name, query))")
        try ensureLatestUnknownSchema(transaction)
        let databaseID = try transaction.withRows("SELECT value FROM \(_metadataTableName) WHERE key = ?", arguments: ["database_id"]) { rows in
            try rows.next()?.string(at: 0)
        }
        if databaseID == nil {
            try transaction.execute("INSERT INTO \(_metadataTableName) (key, value) VALUES (?, ?)", arguments: ["database_id", try uuidV7()])
        }
    }
}

private func ensureLatestUnknownSchema(_ q: any DBTX) throws {
    let createSQL = try q.withRows("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", arguments: [coreTableUnknownName]) { rows in
        try rows.next()?.string(at: 0)
    }
    guard let createSQL else {
        try q.execute("CREATE TABLE \(_unknownTypesTableName) (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, deleted INTEGER NOT NULL, data_json TEXT NOT NULL, PRIMARY KEY (type_name, id))")
        return
    }
    let normalizedSQL = createSQL.lowercased().replacingOccurrences(of: " ", with: "")
    if normalizedSQL.contains("primarykey(type_name,id)") {
        return
    }
    let replacementTableName = "_unknown_types_replacement"
    let replacementTable = quoteSQLiteIdentifier(replacementTableName)
    try q.execute("DROP TABLE IF EXISTS \(replacementTable)")
    try q.execute("CREATE TABLE \(replacementTable) (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, deleted INTEGER NOT NULL, data_json TEXT NOT NULL, PRIMARY KEY (type_name, id))")
    try q.execute("""
    INSERT INTO \(replacementTable) (type_name, id, at_ns, deleted, data_json)
    SELECT old.type_name, old.id, old.at_ns, old.deleted, old.data_json
    FROM \(_unknownTypesTableName) old
    WHERE old.rowid = (
        SELECT candidate.rowid FROM \(_unknownTypesTableName) candidate
        WHERE candidate.type_name = old.type_name AND candidate.id = old.id
        ORDER BY candidate.at_ns DESC, candidate.rowid DESC LIMIT 1
    )
    """)
    try q.execute("DROP TABLE \(_unknownTypesTableName)")
    try q.execute("ALTER TABLE \(replacementTable) RENAME TO \(coreTableUnknownName)")
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

private struct GeneratedTableColumn {
    let name: String
    let sqliteType: String
    let notNull: Bool
    let defaultSQL: String?
}

public func reconcileGeneratedTable(_ q: any DBTX, binding: GeneratedTableBinding) throws {
    try q.withTransaction { transaction in
        try transaction.execute(binding.createTableSQL)
        let columns = try transaction.withRows("PRAGMA table_info(\(quoteSQLiteIdentifier(binding.descriptor.tableName)))") { rows in
            var result: [GeneratedTableColumn] = []
            while let row = try rows.next() {
                result.append(GeneratedTableColumn(
                    name: try row.string(at: 1),
                    sqliteType: try row.string(at: 2),
                    notNull: try row.int64(at: 3) != 0,
                    defaultSQL: try row.optionalString(at: 4)
                ))
            }
            return result
        }
        let columnsByName = Dictionary(uniqueKeysWithValues: columns.map { ($0.name, $0) })
        var expectedNames: Set<String> = ["id", "at_ns", "data"]
        var repairNames: Set<String> = []
        for projected in binding.projectedColumns {
            expectedNames.insert(projected.name)
            guard let column = columnsByName[projected.name] else {
                continue
            }
            guard column.sqliteType.caseInsensitiveCompare(projected.sqliteType) == .orderedSame else {
                throw ProprDBError("projection column \(binding.descriptor.tableName).\(projected.name) has SQLite type \(column.sqliteType), expected \(projected.sqliteType)")
            }
            let actualNullable = !column.notNull
            if actualNullable != projected.nullable {
                if projected.nullable, projected.legacyOneofPresenceRepair, !actualNullable {
                    repairNames.insert(projected.name)
                } else {
                    throw ProprDBError("projection column \(binding.descriptor.tableName).\(projected.name) nullable=\(actualNullable), expected \(projected.nullable)")
                }
            }
            if !projected.nullable, normalizeSQLiteDefault(column.defaultSQL) != normalizeSQLiteDefault(projected.defaultSQL) {
                throw ProprDBError("projection column \(binding.descriptor.tableName).\(projected.name) has default \(column.defaultSQL ?? "NULL"), expected \(projected.defaultSQL)")
            }
        }
        let currentSchema = try transaction.withRows("SELECT schema_hash FROM \(_proprdbSchemaTableName) WHERE table_name = ?", arguments: [binding.descriptor.tableName]) { rows in
            try rows.next()?.string(at: 0)
        }
        if let currentSchema {
            try validateProjectionEvolution(tableName: binding.descriptor.tableName, previousSchema: currentSchema, columns: binding.projectedColumns)
        }
        try ensureManagedIndexes(transaction, tableName: binding.descriptor.tableName, generatedIndexPrefix: binding.generatedIndexPrefix, createIndexSQL: [], desiredIndexNames: [])
        let columnsToDrop = columns.filter { !expectedNames.contains($0.name) || repairNames.contains($0.name) }
        if !columnsToDrop.isEmpty {
            try requireDropColumnSQLite(transaction)
        }
        var changed = false
        for column in columnsToDrop {
            do {
                try transaction.execute("ALTER TABLE \(quoteSQLiteIdentifier(binding.descriptor.tableName)) DROP COLUMN \(quoteSQLiteIdentifier(column.name))")
            } catch {
                throw ProprDBError("drop projection column \(binding.descriptor.tableName).\(column.name): \(error)")
            }
            changed = true
        }
        for projected in binding.projectedColumns where columnsByName[projected.name] == nil || repairNames.contains(projected.name) {
            try transaction.execute("ALTER TABLE \(quoteSQLiteIdentifier(binding.descriptor.tableName)) ADD COLUMN \(projectedColumnSQL(projected))")
            changed = true
        }
        if changed || currentSchema != binding.projectionSchema {
            try reprojectGeneratedTable(transaction, binding: binding)
        }
        try ensureManagedIndexes(
            transaction,
            tableName: binding.descriptor.tableName,
            generatedIndexPrefix: binding.generatedIndexPrefix,
            createIndexSQL: binding.generatedIndexes.map(\.createSQL),
            desiredIndexNames: binding.generatedIndexes.map(\.name)
        )
        try transaction.execute(
            "INSERT INTO \(_proprdbSchemaTableName) (table_name, schema_hash) VALUES (?, ?) ON CONFLICT(table_name) DO UPDATE SET schema_hash = excluded.schema_hash",
            arguments: [binding.descriptor.tableName, binding.projectionSchema]
        )
    }
}

public func auditObjectIDs(_ q: any DBTX, bindings: [GeneratedTableBinding]) throws {
    var targets = [
        _deletedTableName: "id",
        _syncTableName: "object_id",
        _unknownTypesTableName: "id",
        _unknownSyncTableName: "id",
        _exportBatchEntriesTableName: "object_id",
    ]
    for binding in bindings {
        targets[binding.descriptor.tableName] = "id"
    }
    for (tableName, columnName) in targets {
        let exists = try q.withRows("SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?", arguments: [tableName]) { rows in
            try rows.next()?.int64(at: 0) ?? 0
        }
        guard exists > 0 else {
            continue
        }
        try q.withRows("SELECT \(quoteSQLiteIdentifier(columnName)) FROM \(quoteSQLiteIdentifier(tableName))") { rows in
            while let row = try rows.next() {
                let id = try row.string(at: 0)
                do {
                    try validateUUIDV7(id)
                } catch {
                    throw ProprDBError("invalid stored object ID table=\(tableName) id=\(id): \(error)")
                }
            }
        }
    }
}

private func projectedColumnSQL(_ column: ProjectedColumnDescriptor) -> String {
    let statement = "\(quoteSQLiteIdentifier(column.name)) \(column.sqliteType)"
    return column.nullable ? statement : "\(statement) NOT NULL DEFAULT \(column.defaultSQL)"
}

private func normalizeSQLiteDefault(_ value: String?) -> String {
    value?.trimmingCharacters(in: .whitespacesAndNewlines).uppercased() ?? ""
}

private func validateProjectionEvolution(tableName: String, previousSchema: String, columns: [ProjectedColumnDescriptor]) throws {
    var previous: [String: (kind: String, nullable: Bool)] = [:]
    for entry in previousSchema.split(separator: ";") {
        let parts = entry.split(separator: ":", omittingEmptySubsequences: false).map(String.init)
        guard parts.count >= 2 else {
            continue
        }
        previous[parts[0]] = (parts[1], parts.count == 3 && parts[2] == "optional")
    }
    for column in columns {
        guard let old = previous[column.name] else {
            continue
        }
        guard old.kind == column.protoKind else {
            throw ProprDBError("projection column \(tableName).\(column.name) changed protobuf kind from \(old.kind) to \(column.protoKind)")
        }
        if old.nullable != column.nullable,
           !(column.legacyOneofPresenceRepair && !old.nullable && column.nullable)
        {
            throw ProprDBError("projection column \(tableName).\(column.name) changed presence semantics")
        }
    }
}

private func requireDropColumnSQLite(_ q: any DBTX) throws {
    let version = try q.withRows("SELECT sqlite_version()") { rows in
        guard let row = try rows.next() else {
            throw ProprDBError("read SQLite version")
        }
        return try row.string(at: 0)
    }
    let components = version.split(separator: ".").compactMap { Int($0) }
    guard components.count >= 2, components[0] > 3 || (components[0] == 3 && components[1] >= 35) else {
        throw ProprDBError("projection removal requires SQLite 3.35 or newer, found \(version)")
    }
}

private func reprojectGeneratedTable(_ q: any DBTX, binding: GeneratedTableBinding) throws {
    guard !binding.projectedColumns.isEmpty else {
        return
    }
    let bufferedRows = try q.withRows("SELECT id, data FROM \(quoteSQLiteIdentifier(binding.descriptor.tableName))") { rows in
        var result: [(String, Data)] = []
        while let row = try rows.next() {
            result.append((try row.string(at: 0), try row.data(at: 1)))
        }
        return result
    }
    let assignments = binding.projectedColumns.map { "\(quoteSQLiteIdentifier($0.name)) = ?" }.joined(separator: ", ")
    let updateSQL = "UPDATE \(quoteSQLiteIdentifier(binding.descriptor.tableName)) SET \(assignments) WHERE id = ?"
    for (id, bytes) in bufferedRows {
        let message = try binding.decodeBinary(bytes)
        var arguments = try binding.projectedValues(message).map(\.sqliteValue)
        arguments.append(id)
        try q.execute(updateSQL, arguments: arguments)
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

public func validateUUIDV7(_ id: String) throws {
    let characters = Array(id)
    guard characters.count == 36 else {
        throw ProprDBError("invalid uuidv7 \(id): expected 36 characters")
    }
    let hyphenIndexes = Set([8, 13, 18, 23])
    let hexadecimal = CharacterSet(charactersIn: "0123456789abcdef")
    for (index, character) in characters.enumerated() {
        if hyphenIndexes.contains(index) {
            guard character == "-" else {
                throw ProprDBError("invalid uuidv7 \(id): expected hyphen at character \(index + 1)")
            }
        } else {
            guard character.unicodeScalars.allSatisfy(hexadecimal.contains) else {
                throw ProprDBError("invalid uuidv7 \(id): expected canonical lowercase hexadecimal")
            }
        }
    }
    guard characters[14] == "7" else {
        throw ProprDBError("invalid uuidv7 \(id): version is not 7")
    }
    guard "89ab".contains(characters[19]) else {
        throw ProprDBError("invalid uuidv7 \(id): invalid RFC variant")
    }
}

public func validateUUID(_ id: String) throws {
    try validateUUIDV7(id)
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
    var anyMessage = try Google_Protobuf_Any(message: message)
    anyMessage.typeURL = typeURL(typeName)
    return try anyMessage.jsonUTF8Data()
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

private let jsonlReadBufferBytes = 64 * 1_024

private struct JSONLStreamLineReader {
    let stream: InputStream
    private var readBuffer = [UInt8](repeating: 0, count: jsonlReadBufferBytes)
    private var readBufferIndex = 0
    private var readBufferCount = 0
    private var lineData = Data()
    private var reachedEnd = false

    init(stream: InputStream) {
        self.stream = stream
    }

    mutating func nextLine() throws -> Data? {
        while true {
            if readBufferIndex < readBufferCount {
                let remaining = readBufferIndex..<readBufferCount
                if let newlineIndex = readBuffer[remaining].firstIndex(of: UInt8(ascii: "\n")) {
                    lineData.append(contentsOf: readBuffer[readBufferIndex..<newlineIndex])
                    readBufferIndex = newlineIndex + 1
                    let result = lineData
                    lineData.removeAll(keepingCapacity: true)
                    return result
                }
                lineData.append(contentsOf: readBuffer[remaining])
                readBufferIndex = readBufferCount
            }
            if reachedEnd {
                guard !lineData.isEmpty else {
                    return nil
                }
                let result = lineData
                lineData.removeAll(keepingCapacity: true)
                return result
            }
            let readBytes = stream.read(&readBuffer, maxLength: readBuffer.count)
            switch readBytes {
            case let count where count > 0:
                readBufferIndex = 0
                readBufferCount = count
            case 0:
                reachedEnd = true
            default:
                throw stream.streamError ?? ProprDBError("read jsonl stream")
            }
        }
    }
}

public func readJSONL(stream: InputStream, visit: (JSONLRecord, Int) throws -> Void) throws {
    guard stream.streamStatus == .notOpen else {
        throw ProprDBError("jsonl input stream must be unopened")
    }
    stream.open()
    defer { stream.close() }
    var reader = JSONLStreamLineReader(stream: stream)
    var lineNumber = 0
    while true {
        let rawLine: Data
        do {
            guard let nextLine = try reader.nextLine() else {
                return
            }
            rawLine = nextLine
        } catch {
            throw ProprDBError("read jsonl line \(lineNumber + 1): \(error)")
        }
        lineNumber += 1
        guard let line = String(data: rawLine, encoding: .utf8) else {
            throw ProprDBError("decode jsonl line \(lineNumber): invalid utf8")
        }
        if line.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            continue
        }
        guard let object = try JSONSerialization.jsonObject(with: rawLine) as? [String: Any] else {
            throw ProprDBError("decode jsonl line \(lineNumber): expected object")
        }
        guard let id = object["id"] as? String else {
            throw ProprDBError("decode jsonl line \(lineNumber): missing id")
        }
        let deleted: Bool
        if let deletedValue = object["deleted"] {
            guard CFGetTypeID(deletedValue as CFTypeRef) == CFBooleanGetTypeID() else {
                throw ProprDBError("decode jsonl line \(lineNumber): deleted must be a boolean")
            }
            deleted = (deletedValue as! NSNumber).boolValue
        } else {
            deleted = false
        }
        let atNsValue = object["atNs"]
        let atNs = try decodeInt64(atNsValue, fieldName: "atNs", lineNumber: lineNumber)
        guard let dataObject = object["data"] as? [String: Any] else {
            throw ProprDBError("decode jsonl line \(lineNumber): data must be an object")
        }
        let dataJSON = try JSONSerialization.data(withJSONObject: dataObject, options: [.sortedKeys])
        _ = try typeNameFromAnyJSON(dataJSON)
        try validateUUIDV7(id)
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
    try validateUUIDV7(record.id)
    let canonicalData = try JSONSerialization.data(withJSONObject: jsonObject(from: record.data), options: [.sortedKeys])
    let dataJSON = String(decoding: canonicalData, as: UTF8.self)
    let existing = try q.withRows("SELECT at_ns, deleted, data_json FROM \(_unknownTypesTableName) WHERE type_name = ? AND id = ?", arguments: [typeName, record.id]) { rows -> (Int64, Bool, String)? in
        guard let row = try rows.next() else { return nil }
        return (try row.int64(at: 0), try row.int64(at: 1) != 0, try row.string(at: 2))
    }
    if let existing, existing.0 == record.atNs {
        if existing.1 == record.deleted, existing.2 == dataJSON {
            return
        }
        throw ConflictError(typeName: typeName, id: record.id, atNs: record.atNs, localDeleted: existing.1, remoteDeleted: record.deleted)
    }
    try q.execute("INSERT INTO \(_unknownTypesTableName) (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?) ON CONFLICT(type_name, id) DO UPDATE SET at_ns = excluded.at_ns, deleted = excluded.deleted, data_json = excluded.data_json WHERE excluded.at_ns > at_ns", arguments: [typeName, record.id, record.atNs, record.deleted ? 1 : 0, dataJSON])
}

public func replayUnknownByType(_ q: any DBTX, typeName: String, apply: (JSONLRecord) throws -> Void) throws {
    guard !typeName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
        throw ProprDBError("empty type name")
    }
    let replayRows = try q.withRows("SELECT id, at_ns, deleted, data_json FROM \(_unknownTypesTableName) WHERE type_name = ? ORDER BY at_ns ASC, id ASC", arguments: [typeName]) { rows in
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

public func nextObjectAtNs(_ q: any DBTX, tableName: String, objectID: String) throws -> Int64 {
    let currentAtNs = try localMaxAtNs(q, tableName: tableName, objectID: objectID)
    let wallAtNs = nowNs()
    if currentAtNs >= wallAtNs {
        guard currentAtNs < Int64.max else {
            throw ProprDBError("object timestamp exhausted Int64")
        }
        return currentAtNs + 1
    }
    return wallAtNs
}

private func bindingArguments(_ binding: GeneratedTableBinding, id: String, atNs: Int64, message: any Message) throws -> [Any?] {
    var arguments: [Any?] = [id, atNs, try message.serializedData()]
    arguments.append(contentsOf: try binding.projectedValues(message).map(\.sqliteValue))
    return arguments
}

private func queueGeneratedTableChange(
    _ q: any DBTX,
    binding: GeneratedTableBinding,
    id: String,
    atNs: Int64,
    deleted: Bool,
    message: (any Message)?
) {
    guard
        binding.descriptor.changeListenersEnabled,
        let tracked = q as? ChangeTrackingDBTX,
        tracked.tableChangeBroker.hasSubscribers(tableName: binding.descriptor.tableName)
    else {
        return
    }
    tracked.queue(GeneratedTableChange(
        tableName: binding.descriptor.tableName,
        id: id,
        atNs: atNs,
        deleted: deleted,
        message: message
    ))
}

public func writeLocalObject(
    _ q: any DBTX,
    binding: GeneratedTableBinding,
    id: String,
    message: any Message,
    insert: Bool
) throws -> Int64 {
    try validateUUIDV7(id)
    return try q.withTransaction { transaction in
        let atNs = try nextObjectAtNs(transaction, tableName: binding.descriptor.tableName, objectID: id)
        let arguments = try bindingArguments(binding, id: id, atNs: atNs, message: message)
        try transaction.execute("DELETE FROM \(_deletedTableName) WHERE table_name = ? AND id = ?", arguments: [binding.descriptor.tableName, id])
        try transaction.execute(insert ? binding.insertSQL : binding.upsertSQL, arguments: arguments)
        queueGeneratedTableChange(transaction, binding: binding, id: id, atNs: atNs, deleted: false, message: message)
        return atNs
    }
}

public func deleteLocalObject(_ q: any DBTX, binding: GeneratedTableBinding, id: String) throws {
    try validateUUIDV7(id)
    try q.withTransaction { transaction in
        let atNs = try nextObjectAtNs(transaction, tableName: binding.descriptor.tableName, objectID: id)
        try applyBoundDeletion(transaction, binding: binding, id: id, atNs: atNs)
        queueGeneratedTableChange(transaction, binding: binding, id: id, atNs: atNs, deleted: true, message: nil)
    }
}

private func decodedBindingMessage(_ binding: GeneratedTableBinding, data: Data) throws -> any Message {
    try binding.decodeAnyJSON(data)
}

public func applyIncomingObject(_ q: any DBTX, binding: GeneratedTableBinding, record: JSONLRecord) throws {
    try validateUUIDV7(record.id)
    let localAtNs = try localMaxAtNs(q, tableName: binding.descriptor.tableName, objectID: record.id)
    if record.atNs < localAtNs {
        return
    }
    if record.deleted {
        if record.atNs == localAtNs {
            let tombstoneExists = try q.withRows("SELECT 1 FROM \(_deletedTableName) WHERE table_name = ? AND id = ? AND at_ns = ?", arguments: [binding.descriptor.tableName, record.id, record.atNs]) { rows in
                try rows.next() != nil
            }
            if tombstoneExists {
                return
            }
            throw ConflictError(typeName: binding.descriptor.typeName, id: record.id, atNs: record.atNs, localDeleted: false, remoteDeleted: true)
        }
        try applyBoundDeletion(q, binding: binding, id: record.id, atNs: record.atNs)
        queueGeneratedTableChange(q, binding: binding, id: record.id, atNs: record.atNs, deleted: true, message: nil)
        return
    }
    let message = try decodedBindingMessage(binding, data: record.data)
    if record.atNs == localAtNs {
        let localData = try q.withRows("SELECT data FROM \(quoteSQLiteIdentifier(binding.descriptor.tableName)) WHERE id = ? AND at_ns = ?", arguments: [record.id, record.atNs]) { rows in
            try rows.next()?.data(at: 0)
        }
        guard let localData else {
            throw ConflictError(typeName: binding.descriptor.typeName, id: record.id, atNs: record.atNs, localDeleted: true, remoteDeleted: false)
        }
        let localMessage = try binding.decodeBinary(localData)
        if binding.messagesEqual(localMessage, message) {
            return
        }
        throw ConflictError(typeName: binding.descriptor.typeName, id: record.id, atNs: record.atNs, localDeleted: false, remoteDeleted: false)
    }
    try q.execute("DELETE FROM \(_deletedTableName) WHERE table_name = ? AND id = ?", arguments: [binding.descriptor.tableName, record.id])
    try q.execute(binding.upsertSQL, arguments: try bindingArguments(binding, id: record.id, atNs: record.atNs, message: message))
    queueGeneratedTableChange(q, binding: binding, id: record.id, atNs: record.atNs, deleted: false, message: message)
}

private func applyTombstone(_ q: any DBTX, tableName: String, id: String, atNs: Int64) throws {
    try q.execute("INSERT INTO \(_deletedTableName) (table_name, id, at_ns) VALUES (?, ?, ?) ON CONFLICT(table_name, id) DO UPDATE SET at_ns = excluded.at_ns", arguments: [tableName, id, atNs])
    try q.execute("DELETE FROM \(quoteSQLiteIdentifier(tableName)) WHERE id = ?", arguments: [id])
}

private func applyBoundDeletion(_ q: any DBTX, binding: GeneratedTableBinding, id: String, atNs: Int64) throws {
    if binding.descriptor.syncEnabled {
        try applyTombstone(q, tableName: binding.descriptor.tableName, id: id, atNs: atNs)
        return
    }
    try q.execute("DELETE FROM \(quoteSQLiteIdentifier(binding.descriptor.tableName)) WHERE id = ?", arguments: [id])
}

public func readBoundJSONL(_ q: any DBTX, bindings: [GeneratedTableBinding], remote: String, stream: InputStream) throws {
    let bindingsByType = Dictionary(uniqueKeysWithValues: bindings.map { ($0.descriptor.typeName, $0) })
    try readJSONL(stream: stream) { record, lineNumber in
        guard !record.id.isEmpty, !record.data.isEmpty else {
            throw ProprDBError("jsonl line \(lineNumber) has empty id or data")
        }
        let typeName = try typeNameFromAnyJSON(record.data)
        if let binding = bindingsByType[typeName], !binding.descriptor.syncEnabled {
            logIgnoredUnsyncedJSONLRecord(typeName: typeName, id: record.id, remote: remote, lineNumber: lineNumber)
            return
        }
        try q.withTransaction { transaction in
            if let binding = bindingsByType[typeName] {
                try applyIncomingObject(transaction, binding: binding, record: record)
                try syncUpsert(transaction, objectID: record.id, tableName: binding.descriptor.tableName, remote: remote, atNs: record.atNs)
            } else {
                try unknownInsert(transaction, typeName: typeName, record: record)
                if !remote.isEmpty {
                    try transaction.execute("INSERT INTO \(_unknownSyncTableName) (type_name, id, at_ns, remote) VALUES (?, ?, ?, ?) ON CONFLICT(type_name, id, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)", arguments: [typeName, record.id, record.atNs, remote])
                }
            }
        }
    }
}

public func drainBoundUnknown(_ q: any DBTX, bindings: [GeneratedTableBinding]) throws {
    for binding in bindings where binding.descriptor.syncEnabled {
        let records = try q.withRows("SELECT id, at_ns, deleted, data_json FROM \(_unknownTypesTableName) WHERE type_name = ? ORDER BY id", arguments: [binding.descriptor.typeName]) { rows in
            var result: [JSONLRecord] = []
            while let row = try rows.next() {
                result.append(JSONLRecord(
                    id: try row.string(at: 0),
                    deleted: try row.int64(at: 2) != 0,
                    atNs: try row.int64(at: 1),
                    data: Data((try row.string(at: 3)).utf8)
                ))
            }
            return result
        }
        for record in records {
            try q.withTransaction { transaction in
                try applyIncomingObject(transaction, binding: binding, record: record)
                try transaction.execute("INSERT INTO \(_syncTableName) (object_id, table_name, at_ns, remote) SELECT id, ?, at_ns, remote FROM \(_unknownSyncTableName) WHERE type_name = ? AND id = ? ON CONFLICT(object_id, table_name, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)", arguments: [binding.descriptor.tableName, binding.descriptor.typeName, record.id])
                try transaction.execute("DELETE FROM \(_unknownSyncTableName) WHERE type_name = ? AND id = ?", arguments: [binding.descriptor.typeName, record.id])
                try transaction.execute("DELETE FROM \(_unknownTypesTableName) WHERE type_name = ? AND id = ?", arguments: [binding.descriptor.typeName, record.id])
            }
        }
    }
}

private func databaseID(_ q: any DBTX) throws -> String {
    try q.withRows("SELECT value FROM \(_metadataTableName) WHERE key = ?", arguments: ["database_id"]) { rows in
        guard let value = try rows.next()?.string(at: 0) else {
            throw ProprDBError("missing database id")
        }
        return value
    }
}

public func prepareBoundJSONL(_ q: any DBTX, bindings: [GeneratedTableBinding], remote: String) throws -> PreparedJSONLExport {
    try ensureCoreTables(q)
    let knownTypeNames = Set(bindings.map(\.descriptor.typeName))
    let checkpoint = try q.withTransaction { transaction in
        let checkpoint = JSONLCheckpoint(version: 1, databaseId: try databaseID(transaction), batchId: try uuidV7())
        try transaction.execute("INSERT INTO \(_exportBatchesTableName) (batch_id, database_id, remote, complete) VALUES (?, ?, ?, 0)", arguments: [checkpoint.batchId, checkpoint.databaseId, remote])
        var sequence = 0
        for binding in bindings where binding.descriptor.syncEnabled {
            let tableName = binding.descriptor.tableName
            let rows = try transaction.withRows("""
            SELECT row.id, row.at_ns, row.data FROM \(quoteSQLiteIdentifier(tableName)) row
            LEFT JOIN \(_syncTableName) sync_row
            ON sync_row.object_id = row.id AND sync_row.table_name = ? AND sync_row.remote = ?
            WHERE ? = '' OR sync_row.at_ns IS NULL OR sync_row.at_ns < row.at_ns ORDER BY row.id
            """, arguments: [tableName, remote, remote]) { rows in
                var result: [(String, Int64, Data)] = []
                while let row = try rows.next() {
                    result.append((try row.string(at: 0), try row.int64(at: 1), try row.data(at: 2)))
                }
                return result
            }
            for (id, atNs, data) in rows {
                let message = try binding.decodeBinary(data)
                let record = JSONLRecord(id: id, deleted: false, atNs: atNs, data: try binding.encodeAnyJSON(message))
                try stageJSONLRecord(transaction, checkpoint: checkpoint, sequence: sequence, tableName: tableName, record: record)
                sequence += 1
            }
        }
        let bindingsByTable = Dictionary(uniqueKeysWithValues: bindings.filter { $0.descriptor.syncEnabled }.map { ($0.descriptor.tableName, $0) })
        let tombstones = try transaction.withRows("SELECT table_name, id, at_ns FROM \(_deletedTableName) ORDER BY table_name, id") { rows in
            var result: [(String, String, Int64)] = []
            while let row = try rows.next() {
                result.append((try row.string(at: 0), try row.string(at: 1), try row.int64(at: 2)))
            }
            return result
        }
        for (tableName, id, atNs) in tombstones {
            guard let binding = bindingsByTable[tableName], try syncNeedsSend(transaction, objectID: id, tableName: tableName, remote: remote, atNs: atNs) else {
                continue
            }
            let record = JSONLRecord(id: id, deleted: true, atNs: atNs, data: try marshalTypeOnlyAnyJSON(typeName: binding.descriptor.typeName))
            try stageJSONLRecord(transaction, checkpoint: checkpoint, sequence: sequence, tableName: tableName, record: record)
            sequence += 1
        }
        let unknownRows = try transaction.withRows("SELECT type_name, id, at_ns, deleted, data_json FROM \(_unknownTypesTableName) ORDER BY type_name, id") { rows in
            var result: [(String, JSONLRecord)] = []
            while let row = try rows.next() {
                let typeName = try row.string(at: 0)
                let record = JSONLRecord(id: try row.string(at: 1), deleted: try row.int64(at: 3) != 0, atNs: try row.int64(at: 2), data: Data((try row.string(at: 4)).utf8))
                result.append((typeName, record))
            }
            return result
        }
        for (typeName, record) in unknownRows {
            if knownTypeNames.contains(typeName) {
                continue
            }
            if try remote.isEmpty || unknownSyncNeedsSend(transaction, typeName: typeName, record: record, remote: remote) {
                try stageJSONLRecord(transaction, checkpoint: checkpoint, sequence: sequence, tableName: "@unknown:\(typeName)", record: record)
                sequence += 1
            }
        }
        try transaction.execute("UPDATE \(_exportBatchesTableName) SET complete = 1 WHERE batch_id = ?", arguments: [checkpoint.batchId])
        return checkpoint
    }
    let text = try q.withRows("SELECT record_json FROM \(_exportBatchEntriesTableName) WHERE batch_id = ? ORDER BY sequence", arguments: [checkpoint.batchId]) { rows in
        var output = ""
        while let row = try rows.next() {
            output += String(decoding: try row.data(at: 0), as: UTF8.self)
        }
        return output
    }
    return PreparedJSONLExport(text: text, checkpoint: checkpoint)
}

private func stageJSONLRecord(_ q: any DBTX, checkpoint: JSONLCheckpoint, sequence: Int, tableName: String, record: JSONLRecord) throws {
    try q.execute("INSERT INTO \(_exportBatchEntriesTableName) (batch_id, sequence, table_name, object_id, at_ns, record_json) VALUES (?, ?, ?, ?, ?, ?)", arguments: [checkpoint.batchId, sequence, tableName, record.id, record.atNs, Data(try encodeJSONLRecord(record).utf8)])
}

private func unknownSyncNeedsSend(_ q: any DBTX, typeName: String, record: JSONLRecord, remote: String) throws -> Bool {
    let atNs = try q.withRows("SELECT at_ns FROM \(_unknownSyncTableName) WHERE type_name = ? AND id = ? AND remote = ?", arguments: [typeName, record.id, remote]) { rows in
        try rows.next()?.int64(at: 0)
    }
    return atNs == nil || atNs! < record.atNs
}

public func acknowledgeBoundJSONL(_ q: any DBTX, checkpoint: JSONLCheckpoint) throws {
    try q.withTransaction { transaction in
        guard checkpoint.version == 1, checkpoint.databaseId == (try databaseID(transaction)) else {
            throw ProprDBError("export checkpoint belongs to a different database")
        }
        let batch = try transaction.withRows("SELECT remote, complete FROM \(_exportBatchesTableName) WHERE batch_id = ?", arguments: [checkpoint.batchId]) { rows -> (String, Int64)? in
            guard let row = try rows.next() else { return nil }
            return (try row.string(at: 0), try row.int64(at: 1))
        }
        guard let (remote, complete) = batch else { return }
        guard complete == 1 else { throw ProprDBError("cannot acknowledge incomplete export batch") }
        if !remote.isEmpty {
            try transaction.execute("INSERT INTO \(_syncTableName) (object_id, table_name, at_ns, remote) SELECT object_id, table_name, at_ns, ? FROM \(_exportBatchEntriesTableName) WHERE batch_id = ? AND table_name NOT LIKE '@unknown:%' ON CONFLICT(object_id, table_name, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)", arguments: [remote, checkpoint.batchId])
            try transaction.execute("INSERT INTO \(_unknownSyncTableName) (type_name, id, at_ns, remote) SELECT substr(table_name, 10), object_id, at_ns, ? FROM \(_exportBatchEntriesTableName) WHERE batch_id = ? AND table_name LIKE '@unknown:%' ON CONFLICT(type_name, id, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)", arguments: [remote, checkpoint.batchId])
        }
        try deleteJSONLBatch(transaction, batchID: checkpoint.batchId)
    }
}

public func discardBoundJSONL(_ q: any DBTX, checkpoint: JSONLCheckpoint) throws {
    guard checkpoint.version == 1, checkpoint.databaseId == (try databaseID(q)) else {
        throw ProprDBError("export checkpoint belongs to a different database")
    }
    try deleteJSONLBatch(q, batchID: checkpoint.batchId)
}

private func deleteJSONLBatch(_ q: any DBTX, batchID: String) throws {
    try q.execute("DELETE FROM \(_exportBatchEntriesTableName) WHERE batch_id = ?", arguments: [batchID])
    try q.execute("DELETE FROM \(_exportBatchesTableName) WHERE batch_id = ?", arguments: [batchID])
}

public func introspectTables(_ q: any DBTX, descriptors: [GeneratedTableDescriptor]) throws -> [TableIntrospection] {
    var result: [TableIntrospection] = []
    for descriptor in descriptors {
        result.append(TableIntrospection(
            descriptor: descriptor,
            objectCount: try tableObjectCount(q, tableName: descriptor.tableName),
            payloadBytes: try tablePayloadBytes(q, tableName: descriptor.tableName)
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
            try value.utf8CString.withUnsafeBufferPointer { buffer in
                let result = sqlite3_bind_text(statement, parameterIndex, buffer.baseAddress, Int32(buffer.count - 1), sqliteTransient)
                if result != SQLITE_OK {
                    throw ProprDBError("bind sqlite text: \(sqliteErrorMessage(statement: statement))")
                }
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
            if value.isEmpty {
                if sqlite3_bind_zeroblob(statement, parameterIndex, 0) != SQLITE_OK {
                    throw ProprDBError("bind empty sqlite blob: \(sqliteErrorMessage(statement: statement))")
                }
                continue
            }
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
        guard CFGetTypeID(number) != CFBooleanGetTypeID() else {
            throw ProprDBError("decode jsonl line \(lineNumber): invalid \(fieldName)")
        }
        let type = String(cString: number.objCType)
        guard ["s", "i", "l", "q"].contains(type),
              let parsed = Int64(number.stringValue),
              String(parsed) == number.stringValue
        else {
            throw ProprDBError("decode jsonl line \(lineNumber): invalid \(fieldName)")
        }
        return parsed
    case let string as String:
        guard let parsed = Int64(string), String(parsed) == string else {
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

private func tablePayloadBytes(_ q: any DBTX, tableName: String) throws -> Int64 {
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
