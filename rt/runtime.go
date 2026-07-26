package proprdbrt

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	CoreTableDeletedName     = "_deleted"
	CoreTableSyncName        = "_sync"
	CoreTableSchemaStateName = "_proprdb_schema"
	CoreTableUnknownName     = "_unknown_types"
	CoreTableUnknownSyncName = "_unknown_sync"
	CoreTableMetadataName    = "_proprdb_metadata"
	CoreTableExportBatchName = "_export_batches"
	CoreTableExportEntryName = "_export_batch_entries"
	CoreTableQueryStatName   = "_querystat"
	dataColumnName           = "data"
	DefaultBatchRecords      = 256
	metadataDatabaseIDKey    = "database_id"
	unknownBatchTablePrefix  = "@unknown:"
)

type DBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	WithTransaction(context.Context, func(DBTX) error) error
}

type DBAdapter struct {
	db *sql.DB
}

type ConnAdapter struct {
	conn *sql.Conn
}

type TxAdapter struct {
	tx *sql.Tx
}

var savepointSequence atomic.Uint64

func WrapDB(db *sql.DB) DBTX {
	return &DBAdapter{db: db}
}

func WrapConn(conn *sql.Conn) DBTX {
	return &ConnAdapter{conn: conn}
}

func WrapTx(tx *sql.Tx) DBTX {
	return &TxAdapter{tx: tx}
}

func (a *DBAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.db.ExecContext(ctx, query, args...)
}

func (a *DBAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.db.QueryContext(ctx, query, args...)
}

func (a *DBAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return a.db.QueryRowContext(ctx, query, args...)
}

func (a *DBAdapter) WithTransaction(ctx context.Context, body func(DBTX) error) error {
	if a == nil || a.db == nil {
		return errors.New("nil database adapter")
	}
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	return finishSQLTransaction(tx, body(&TxAdapter{tx: tx}))
}

func (a *ConnAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.conn.ExecContext(ctx, query, args...)
}

func (a *ConnAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.conn.QueryContext(ctx, query, args...)
}

func (a *ConnAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return a.conn.QueryRowContext(ctx, query, args...)
}

func (a *ConnAdapter) WithTransaction(ctx context.Context, body func(DBTX) error) error {
	if a == nil || a.conn == nil {
		return errors.New("nil connection adapter")
	}
	tx, err := a.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	return finishSQLTransaction(tx, body(&TxAdapter{tx: tx}))
}

func (a *TxAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.tx.ExecContext(ctx, query, args...)
}

func (a *TxAdapter) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return a.tx.QueryContext(ctx, query, args...)
}

func (a *TxAdapter) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return a.tx.QueryRowContext(ctx, query, args...)
}

func (a *TxAdapter) WithTransaction(ctx context.Context, body func(DBTX) error) error {
	if a == nil || a.tx == nil {
		return errors.New("nil transaction adapter")
	}
	savepoint := "proprdb_" + strconv.FormatUint(savepointSequence.Add(1), 10)
	if _, err := a.tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return fmt.Errorf("create savepoint: %w", err)
	}
	bodyErr := body(a)
	if bodyErr != nil {
		if _, rollbackErr := a.tx.ExecContext(ctx, "ROLLBACK TO "+savepoint); rollbackErr != nil {
			return fmt.Errorf("%w (additionally, rollback savepoint: %v)", bodyErr, rollbackErr)
		}
		if _, releaseErr := a.tx.ExecContext(ctx, "RELEASE "+savepoint); releaseErr != nil {
			return fmt.Errorf("%w (additionally, release savepoint: %v)", bodyErr, releaseErr)
		}
		return bodyErr
	}
	if _, err := a.tx.ExecContext(ctx, "RELEASE "+savepoint); err != nil {
		return fmt.Errorf("release savepoint: %w", err)
	}
	return nil
}

func finishSQLTransaction(tx *sql.Tx, bodyErr error) error {
	if bodyErr != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("%w (additionally, rollback transaction: %v)", bodyErr, rollbackErr)
		}
		return bodyErr
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

type JSONLRecord struct {
	ID      string          `json:"id"`
	Deleted bool            `json:"deleted,omitempty"`
	AtNs    int64           `json:"atNs"`
	Data    json.RawMessage `json:"data"`
}

type ConflictError struct {
	TypeName      string
	ID            string
	AtNs          int64
	LocalDeleted  bool
	RemoteDeleted bool
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflicting state type=%s id=%s at_ns=%d local_deleted=%t remote_deleted=%t", e.TypeName, e.ID, e.AtNs, e.LocalDeleted, e.RemoteDeleted)
}

type JSONLCheckpoint struct {
	Version    int    `json:"version"`
	DatabaseID string `json:"databaseId"`
	BatchID    string `json:"batchId"`
}

type UnknownExportRecord struct {
	TypeName string
	Record   JSONLRecord
}

func UnknownExportRecordsContext(ctx context.Context, q DBTX, remote string) (records []UnknownExportRecord, err error) {
	query := `SELECT unknown_row.type_name, unknown_row.id, unknown_row.at_ns, unknown_row.deleted, unknown_row.data_json
FROM ` + CoreTableUnknownName + ` unknown_row
LEFT JOIN ` + CoreTableUnknownSyncName + ` sync_row
ON sync_row.type_name = unknown_row.type_name AND sync_row.id = unknown_row.id AND sync_row.remote = ?
WHERE ? = '' OR sync_row.at_ns IS NULL OR sync_row.at_ns < unknown_row.at_ns
ORDER BY unknown_row.type_name, unknown_row.id`
	rows, err := q.QueryContext(ctx, query, remote, remote)
	if err != nil {
		return nil, fmt.Errorf("select unknown export rows: %w", err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil && err == nil {
			err = fmt.Errorf("close unknown export rows: %w", closeErr)
		}
	}()
	records = make([]UnknownExportRecord, 0)
	for rows.Next() {
		var typeName string
		var id string
		var atNs int64
		var deleted int
		var dataJSON string
		if err := rows.Scan(&typeName, &id, &atNs, &deleted, &dataJSON); err != nil {
			return nil, fmt.Errorf("scan unknown export row: %w", err)
		}
		records = append(records, UnknownExportRecord{
			TypeName: typeName,
			Record:   JSONLRecord{ID: id, Deleted: deleted != 0, AtNs: atNs, Data: json.RawMessage(dataJSON)},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unknown export rows: %w", err)
	}
	return records, nil
}

func UnknownBatchTableName(typeName string) string {
	return unknownBatchTablePrefix + typeName
}

func BeginJSONLBatchContext(ctx context.Context, q DBTX, remote string) (JSONLCheckpoint, error) {
	if err := EnsureCoreTablesContext(ctx, q); err != nil {
		return JSONLCheckpoint{}, err
	}
	databaseID, err := databaseIDContext(ctx, q)
	if err != nil {
		return JSONLCheckpoint{}, err
	}
	batchID, err := UUIDv7()
	if err != nil {
		return JSONLCheckpoint{}, err
	}
	if _, err := q.ExecContext(ctx, `INSERT INTO `+CoreTableExportBatchName+` (batch_id, database_id, remote, complete) VALUES (?, ?, ?, 0)`, batchID, databaseID, remote); err != nil {
		return JSONLCheckpoint{}, fmt.Errorf("create export batch: %w", err)
	}
	return JSONLCheckpoint{Version: 1, DatabaseID: databaseID, BatchID: batchID}, nil
}

func AppendJSONLBatchEntryContext(ctx context.Context, q DBTX, checkpoint JSONLCheckpoint, sequence int, tableName, objectID string, atNs int64) error {
	_, err := q.ExecContext(ctx, `INSERT INTO `+CoreTableExportEntryName+` (batch_id, sequence, table_name, object_id, at_ns) VALUES (?, ?, ?, ?, ?)`, checkpoint.BatchID, sequence, tableName, objectID, atNs)
	if err != nil {
		return fmt.Errorf("append export batch entry: %w", err)
	}
	return nil
}

func CompleteJSONLBatchContext(ctx context.Context, q DBTX, checkpoint JSONLCheckpoint) error {
	result, err := q.ExecContext(ctx, `UPDATE `+CoreTableExportBatchName+` SET complete = 1 WHERE batch_id = ? AND database_id = ?`, checkpoint.BatchID, checkpoint.DatabaseID)
	if err != nil {
		return fmt.Errorf("complete export batch: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read completed export batch count: %w", err)
	}
	if affected != 1 {
		return errors.New("export batch checkpoint does not match database")
	}
	return nil
}

func AcknowledgeJSONLContext(ctx context.Context, q DBTX, checkpoint JSONLCheckpoint) error {
	return q.WithTransaction(ctx, func(tx DBTX) error {
		databaseID, err := databaseIDContext(ctx, tx)
		if err != nil {
			return err
		}
		if checkpoint.Version != 1 || checkpoint.DatabaseID != databaseID {
			return errors.New("export checkpoint belongs to a different database")
		}
		var remote string
		var complete int
		if err := tx.QueryRowContext(ctx, `SELECT remote, complete FROM `+CoreTableExportBatchName+` WHERE batch_id = ?`, checkpoint.BatchID).Scan(&remote, &complete); errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return fmt.Errorf("read export batch: %w", err)
		}
		if complete != 1 {
			return errors.New("cannot acknowledge incomplete export batch")
		}
		if remote != "" {
			query := `INSERT INTO ` + CoreTableSyncName + ` (object_id, table_name, at_ns, remote)
SELECT object_id, table_name, at_ns, ? FROM ` + CoreTableExportEntryName + ` WHERE batch_id = ?
AND table_name NOT LIKE '@unknown:%'
ON CONFLICT(object_id, table_name, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)`
			if _, err := tx.ExecContext(ctx, query, remote, checkpoint.BatchID); err != nil {
				return fmt.Errorf("acknowledge export entries: %w", err)
			}
			unknownQuery := `INSERT INTO ` + CoreTableUnknownSyncName + ` (type_name, id, at_ns, remote)
SELECT substr(table_name, 10), object_id, at_ns, ? FROM ` + CoreTableExportEntryName + ` WHERE batch_id = ?
AND table_name LIKE '@unknown:%'
ON CONFLICT(type_name, id, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)`
			if _, err := tx.ExecContext(ctx, unknownQuery, remote, checkpoint.BatchID); err != nil {
				return fmt.Errorf("acknowledge unknown export entries: %w", err)
			}
		}
		return deleteJSONLBatchContext(ctx, tx, checkpoint.BatchID)
	})
}

func DiscardJSONLContext(ctx context.Context, q DBTX, checkpoint JSONLCheckpoint) error {
	databaseID, err := databaseIDContext(ctx, q)
	if err != nil {
		return err
	}
	if checkpoint.Version != 1 || checkpoint.DatabaseID != databaseID {
		return errors.New("export checkpoint belongs to a different database")
	}
	return deleteJSONLBatchContext(ctx, q, checkpoint.BatchID)
}

func deleteJSONLBatchContext(ctx context.Context, q DBTX, batchID string) error {
	if _, err := q.ExecContext(ctx, `DELETE FROM `+CoreTableExportEntryName+` WHERE batch_id = ?`, batchID); err != nil {
		return fmt.Errorf("delete export batch entries: %w", err)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM `+CoreTableExportBatchName+` WHERE batch_id = ?`, batchID); err != nil {
		return fmt.Errorf("delete export batch: %w", err)
	}
	return nil
}

func databaseIDContext(ctx context.Context, q DBTX) (string, error) {
	var databaseID string
	if err := q.QueryRowContext(ctx, `SELECT value FROM `+CoreTableMetadataName+` WHERE key = ?`, metadataDatabaseIDKey).Scan(&databaseID); err != nil {
		return "", fmt.Errorf("read database id: %w", err)
	}
	return databaseID, nil
}

func (c JSONLCheckpoint) MarshalText() ([]byte, error) {
	type checkpointWire JSONLCheckpoint
	data, err := json.Marshal(checkpointWire(c))
	if err != nil {
		return nil, fmt.Errorf("marshal jsonl checkpoint: %w", err)
	}
	return data, nil
}

func (c *JSONLCheckpoint) UnmarshalText(data []byte) error {
	type checkpointWire JSONLCheckpoint
	var checkpoint checkpointWire
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return fmt.Errorf("unmarshal jsonl checkpoint: %w", err)
	}
	*c = JSONLCheckpoint(checkpoint)
	if c.Version != 1 || c.DatabaseID == "" || c.BatchID == "" {
		return errors.New("invalid jsonl checkpoint")
	}
	return nil
}

var decimalInt64Pattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

func (r *JSONLRecord) UnmarshalJSON(data []byte) error {
	var wire struct {
		ID      string          `json:"id"`
		Deleted json.RawMessage `json:"deleted,omitempty"`
		AtNs    json.RawMessage `json:"atNs"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	atNsText := string(wire.AtNs)
	if len(wire.AtNs) > 0 && wire.AtNs[0] == '"' {
		var value string
		if err := json.Unmarshal(wire.AtNs, &value); err != nil {
			return fmt.Errorf("decode atNs string: %w", err)
		}
		atNsText = value
	}
	if !decimalInt64Pattern.MatchString(atNsText) {
		return errors.New("atNs must be a signed decimal integer")
	}
	atNs, err := strconv.ParseInt(atNsText, 10, 64)
	if err != nil {
		return fmt.Errorf("decode atNs: %w", err)
	}
	deleted := false
	if len(wire.Deleted) > 0 {
		if string(wire.Deleted) != "true" && string(wire.Deleted) != "false" {
			return errors.New("deleted must be a boolean")
		}
		deleted = string(wire.Deleted) == "true"
	}
	if err := ValidateUUIDv7(wire.ID); err != nil {
		return fmt.Errorf("decode id: %w", err)
	}
	if len(wire.Data) == 0 || wire.Data[0] != '{' {
		return errors.New("data must be an object")
	}
	if _, err := TypeNameFromAnyJSON(wire.Data); err != nil {
		return fmt.Errorf("decode data: %w", err)
	}
	r.ID = wire.ID
	r.Deleted = deleted
	r.AtNs = atNs
	r.Data = wire.Data
	return nil
}

type GeneratedTableDescriptor struct {
	TableName              string
	TypeName               string
	IsCore                 bool
	SyncEnabled            bool
	ChangeListenersEnabled bool
	QueryStatisticsEnabled bool
}

type GeneratedTableBinding struct {
	Descriptor        GeneratedTableDescriptor
	NewMessage        func() proto.Message
	InsertSQL         string
	UpsertSQL         string
	CreateTableSQL    string
	ProjectionSchema  string
	ProjectedColumns  []ProjectedColumnDescriptor
	GeneratedIndexes  []GeneratedIndexDescriptor
	GeneratedIndexKey string
	ProjectedValues   func(proto.Message) ([]any, error)
}

type ProjectedColumnDescriptor struct {
	Name                      string
	ProtoKind                 string
	SQLiteType                string
	DefaultSQL                string
	Nullable                  bool
	LegacyOneofPresenceRepair bool
}

type GeneratedIndexDescriptor struct {
	Name      string
	CreateSQL string
}

func CoreTableDescriptors() []GeneratedTableDescriptor {
	return []GeneratedTableDescriptor{
		{TableName: CoreTableDeletedName, IsCore: true},
		{TableName: CoreTableSyncName, IsCore: true},
		{TableName: CoreTableSchemaStateName, IsCore: true},
		{TableName: CoreTableUnknownName, IsCore: true},
		{TableName: CoreTableUnknownSyncName, IsCore: true},
		{TableName: CoreTableMetadataName, IsCore: true},
		{TableName: CoreTableExportBatchName, IsCore: true},
		{TableName: CoreTableExportEntryName, IsCore: true},
		{TableName: CoreTableQueryStatName, IsCore: true},
	}
}

type TableIntrospection struct {
	Descriptor   GeneratedTableDescriptor
	ObjectCount  int64
	PayloadBytes int64
}

func EnsureCoreTables(q DBTX) error {
	return EnsureCoreTablesContext(context.Background(), q)
}

func EnsureCoreTablesContext(ctx context.Context, q DBTX) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	return q.WithTransaction(ctx, func(tx DBTX) error {
		statements := []struct {
			name string
			sql  string
		}{
			{"_deleted", `CREATE TABLE IF NOT EXISTS ` + CoreTableDeletedName + ` (table_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, PRIMARY KEY (table_name, id))`},
			{"_sync", `CREATE TABLE IF NOT EXISTS ` + CoreTableSyncName + ` (object_id TEXT NOT NULL, table_name TEXT NOT NULL, at_ns INTEGER NOT NULL, remote TEXT NOT NULL, PRIMARY KEY (object_id, table_name, remote))`},
			{"_proprdb_schema", `CREATE TABLE IF NOT EXISTS ` + CoreTableSchemaStateName + ` (table_name TEXT PRIMARY KEY, schema_hash TEXT NOT NULL)`},
			{"_proprdb_metadata", `CREATE TABLE IF NOT EXISTS ` + CoreTableMetadataName + ` (key TEXT PRIMARY KEY, value TEXT NOT NULL)`},
			{"_unknown_sync", `CREATE TABLE IF NOT EXISTS ` + CoreTableUnknownSyncName + ` (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, remote TEXT NOT NULL, PRIMARY KEY (type_name, id, remote))`},
			{"_export_batches", `CREATE TABLE IF NOT EXISTS ` + CoreTableExportBatchName + ` (batch_id TEXT PRIMARY KEY, database_id TEXT NOT NULL, remote TEXT NOT NULL, complete INTEGER NOT NULL DEFAULT 0)`},
			{"_export_batch_entries", `CREATE TABLE IF NOT EXISTS ` + CoreTableExportEntryName + ` (batch_id TEXT NOT NULL, sequence INTEGER NOT NULL, table_name TEXT NOT NULL, object_id TEXT NOT NULL, at_ns INTEGER NOT NULL, record_json BLOB, PRIMARY KEY (batch_id, sequence), FOREIGN KEY (batch_id) REFERENCES ` + CoreTableExportBatchName + `(batch_id) ON DELETE CASCADE)`},
			{"_querystat", `CREATE TABLE IF NOT EXISTS ` + CoreTableQueryStatName + ` (table_name TEXT NOT NULL, query TEXT NOT NULL, calls INTEGER NOT NULL, duration_sum_ns INTEGER NOT NULL, PRIMARY KEY (table_name, query))`},
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement.sql); err != nil {
				return fmt.Errorf("create %s table: %w", statement.name, err)
			}
		}
		if err := ensureLatestUnknownSchema(ctx, tx); err != nil {
			return err
		}
		var databaseID string
		err := tx.QueryRowContext(ctx, `SELECT value FROM `+CoreTableMetadataName+` WHERE key = ?`, metadataDatabaseIDKey).Scan(&databaseID)
		if errors.Is(err, sql.ErrNoRows) {
			generatedID, idErr := UUIDv7()
			if idErr != nil {
				return idErr
			}
			if _, insertErr := tx.ExecContext(ctx, `INSERT INTO `+CoreTableMetadataName+` (key, value) VALUES (?, ?)`, metadataDatabaseIDKey, generatedID); insertErr != nil {
				return fmt.Errorf("insert database id: %w", insertErr)
			}
		} else if err != nil {
			return fmt.Errorf("read database id: %w", err)
		}
		return nil
	})
}

func ensureLatestUnknownSchema(ctx context.Context, q DBTX) error {
	var createSQL string
	err := q.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`, CoreTableUnknownName).Scan(&createSQL)
	if errors.Is(err, sql.ErrNoRows) {
		_, createErr := q.ExecContext(ctx, `CREATE TABLE `+CoreTableUnknownName+` (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, deleted INTEGER NOT NULL, data_json TEXT NOT NULL, PRIMARY KEY (type_name, id))`)
		if createErr != nil {
			return fmt.Errorf("create _unknown_types table: %w", createErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect _unknown_types schema: %w", err)
	}
	normalizedSQL := strings.ToLower(strings.ReplaceAll(createSQL, " ", ""))
	if strings.Contains(normalizedSQL, "primarykey(type_name,id)") {
		return nil
	}
	const replacementTable = "_unknown_types_replacement"
	if _, err := q.ExecContext(ctx, `DROP TABLE IF EXISTS `+replacementTable); err != nil {
		return fmt.Errorf("drop stale unknown replacement: %w", err)
	}
	if _, err := q.ExecContext(ctx, `CREATE TABLE `+replacementTable+` (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, deleted INTEGER NOT NULL, data_json TEXT NOT NULL, PRIMARY KEY (type_name, id))`); err != nil {
		return fmt.Errorf("create unknown replacement: %w", err)
	}
	copySQL := `INSERT INTO ` + replacementTable + ` (type_name, id, at_ns, deleted, data_json)
SELECT old.type_name, old.id, old.at_ns, old.deleted, old.data_json
FROM ` + CoreTableUnknownName + ` old
WHERE old.rowid = (
	SELECT candidate.rowid FROM ` + CoreTableUnknownName + ` candidate
	WHERE candidate.type_name = old.type_name AND candidate.id = old.id
	ORDER BY candidate.at_ns DESC, candidate.rowid DESC LIMIT 1
)`
	if _, err := q.ExecContext(ctx, copySQL); err != nil {
		return fmt.Errorf("copy latest unknown rows: %w", err)
	}
	if _, err := q.ExecContext(ctx, `DROP TABLE `+CoreTableUnknownName); err != nil {
		return fmt.Errorf("drop legacy unknown table: %w", err)
	}
	if _, err := q.ExecContext(ctx, `ALTER TABLE `+replacementTable+` RENAME TO `+CoreTableUnknownName); err != nil {
		return fmt.Errorf("rename unknown replacement: %w", err)
	}
	return nil
}

func EnsureManagedIndexes(q DBTX, tableName, generatedIndexPrefix string, createIndexSQL, desiredIndexNames []string) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	ctx := context.Background()
	for _, createSQL := range createIndexSQL {
		if _, err := q.ExecContext(ctx, createSQL); err != nil {
			return fmt.Errorf("create index for %s: %w", tableName, err)
		}
	}
	indexRows, err := q.QueryContext(ctx, `SELECT name FROM pragma_index_list("`+tableName+`")`)
	if err != nil {
		return fmt.Errorf("read indexes for %s: %w", tableName, err)
	}
	desiredIndexes := make(map[string]bool, len(desiredIndexNames))
	for _, indexName := range desiredIndexNames {
		desiredIndexes[indexName] = true
	}
	staleGeneratedIndexes := make([]string, 0)
	for indexRows.Next() {
		var indexName string
		if err := indexRows.Scan(&indexName); err != nil {
			if closeErr := CloseRows(indexRows, "index metadata"); closeErr != nil {
				return fmt.Errorf("scan index row: %w (additionally, %v)", err, closeErr)
			}
			return fmt.Errorf("scan index row: %w", err)
		}
		if !strings.HasPrefix(indexName, generatedIndexPrefix) {
			continue
		}
		if desiredIndexes[indexName] {
			continue
		}
		staleGeneratedIndexes = append(staleGeneratedIndexes, indexName)
	}
	if err := indexRows.Err(); err != nil {
		if closeErr := CloseRows(indexRows, "index metadata"); closeErr != nil {
			return fmt.Errorf("iterate index rows for %s: %w (additionally, %v)", tableName, err, closeErr)
		}
		return fmt.Errorf("iterate index rows for %s: %w", tableName, err)
	}
	if err := CloseRows(indexRows, "index metadata"); err != nil {
		return err
	}
	for _, indexName := range staleGeneratedIndexes {
		dropSQL := `DROP INDEX IF EXISTS "` + strings.ReplaceAll(indexName, `"`, `""`) + `"`
		if _, err := q.ExecContext(ctx, dropSQL); err != nil {
			return fmt.Errorf("drop stale index %s for %s: %w", indexName, tableName, err)
		}
	}
	return nil
}

func ReconcileGeneratedTableContext(ctx context.Context, q DBTX, binding GeneratedTableBinding) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	if binding.Descriptor.TableName == "" || binding.CreateTableSQL == "" {
		return errors.New("invalid generated table schema binding")
	}
	return q.WithTransaction(ctx, func(tx DBTX) error {
		if _, err := tx.ExecContext(ctx, binding.CreateTableSQL); err != nil {
			return fmt.Errorf("create table %s: %w", binding.Descriptor.TableName, err)
		}
		columns, err := generatedTableColumnsContext(ctx, tx, binding.Descriptor.TableName)
		if err != nil {
			return err
		}
		expectedNames := map[string]bool{"id": true, "at_ns": true, "data": true}
		columnsByName := make(map[string]generatedTableColumn, len(columns))
		for _, column := range columns {
			columnsByName[column.name] = column
		}
		repairNames := make(map[string]bool)
		for _, projected := range binding.ProjectedColumns {
			expectedNames[projected.Name] = true
			column, ok := columnsByName[projected.Name]
			if !ok {
				continue
			}
			if !strings.EqualFold(column.sqliteType, projected.SQLiteType) {
				return fmt.Errorf("projection column %s.%s has SQLite type %s, expected %s", binding.Descriptor.TableName, projected.Name, column.sqliteType, projected.SQLiteType)
			}
			actualNullable := column.notNull == 0
			if actualNullable != projected.Nullable {
				if projected.Nullable && projected.LegacyOneofPresenceRepair && !actualNullable {
					repairNames[projected.Name] = true
					continue
				}
				return fmt.Errorf("projection column %s.%s nullable=%t, expected %t", binding.Descriptor.TableName, projected.Name, actualNullable, projected.Nullable)
			}
			if !projected.Nullable && normalizeSQLiteDefault(column.defaultValue) != normalizeSQLiteDefault(projected.DefaultSQL) {
				return fmt.Errorf("projection column %s.%s has default %v, expected %s", binding.Descriptor.TableName, projected.Name, column.defaultValue, projected.DefaultSQL)
			}
		}
		createIndexSQL := make([]string, 0, len(binding.GeneratedIndexes))
		desiredIndexNames := make([]string, 0, len(binding.GeneratedIndexes))
		for _, index := range binding.GeneratedIndexes {
			createIndexSQL = append(createIndexSQL, index.CreateSQL)
			desiredIndexNames = append(desiredIndexNames, index.Name)
		}
		if err := EnsureManagedIndexes(tx, binding.Descriptor.TableName, binding.GeneratedIndexKey, nil, nil); err != nil {
			return err
		}
		changed := false
		for _, column := range columns {
			if expectedNames[column.name] && !repairNames[column.name] {
				continue
			}
			if _, err := tx.ExecContext(ctx, `ALTER TABLE `+quoteSQLiteIdentifier(binding.Descriptor.TableName)+` DROP COLUMN `+quoteSQLiteIdentifier(column.name)); err != nil {
				return fmt.Errorf("drop projection column %s.%s: %w", binding.Descriptor.TableName, column.name, err)
			}
			changed = true
		}
		for _, projected := range binding.ProjectedColumns {
			if _, exists := columnsByName[projected.Name]; exists && !repairNames[projected.Name] {
				continue
			}
			if _, err := tx.ExecContext(ctx, `ALTER TABLE `+quoteSQLiteIdentifier(binding.Descriptor.TableName)+` ADD COLUMN `+projectedColumnSQL(projected)); err != nil {
				return fmt.Errorf("add projection column %s.%s: %w", binding.Descriptor.TableName, projected.Name, err)
			}
			changed = true
		}
		var currentSchema string
		schemaErr := tx.QueryRowContext(ctx, `SELECT schema_hash FROM `+CoreTableSchemaStateName+` WHERE table_name = ?`, binding.Descriptor.TableName).Scan(&currentSchema)
		if schemaErr != nil && !errors.Is(schemaErr, sql.ErrNoRows) {
			return fmt.Errorf("read projection schema for %s: %w", binding.Descriptor.TableName, schemaErr)
		}
		if schemaErr == nil {
			if err := validateProjectionEvolution(binding.Descriptor.TableName, currentSchema, binding.ProjectedColumns); err != nil {
				return err
			}
		}
		if changed || currentSchema != binding.ProjectionSchema {
			if err := reprojectGeneratedTableContext(ctx, tx, binding); err != nil {
				return err
			}
		}
		if err := EnsureManagedIndexes(tx, binding.Descriptor.TableName, binding.GeneratedIndexKey, createIndexSQL, desiredIndexNames); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+CoreTableSchemaStateName+` (table_name, schema_hash) VALUES (?, ?) ON CONFLICT(table_name) DO UPDATE SET schema_hash = excluded.schema_hash`, binding.Descriptor.TableName, binding.ProjectionSchema); err != nil {
			return fmt.Errorf("write projection schema for %s: %w", binding.Descriptor.TableName, err)
		}
		return nil
	})
}

func AuditObjectIDsContext(ctx context.Context, q DBTX, bindings []GeneratedTableBinding) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	targets := map[string]string{
		CoreTableDeletedName:     "id",
		CoreTableSyncName:        "object_id",
		CoreTableUnknownName:     "id",
		CoreTableUnknownSyncName: "id",
		CoreTableExportEntryName: "object_id",
	}
	for _, binding := range bindings {
		targets[binding.Descriptor.TableName] = "id"
	}
	for tableName, columnName := range targets {
		if err := auditObjectIDTableContext(ctx, q, tableName, columnName); err != nil {
			return err
		}
	}
	return nil
}

func auditObjectIDTableContext(ctx context.Context, q DBTX, tableName, columnName string) (err error) {
	var exists int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = ?`, tableName).Scan(&exists); err != nil {
		return fmt.Errorf("inspect object ID table %s: %w", tableName, err)
	}
	if exists == 0 {
		return nil
	}
	rows, err := q.QueryContext(ctx, `SELECT `+quoteSQLiteIdentifier(columnName)+` FROM `+quoteSQLiteIdentifier(tableName))
	if err != nil {
		return fmt.Errorf("read object IDs from %s: %w", tableName, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close object IDs from %s: %w", tableName, closeErr)
		}
	}()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scan object ID from %s: %w", tableName, err)
		}
		if err := ValidateUUIDv7(id); err != nil {
			return fmt.Errorf("invalid stored object ID table=%s id=%s: %w", tableName, id, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate object IDs from %s: %w", tableName, err)
	}
	return nil
}

type projectionSignatureField struct {
	kind     string
	nullable bool
}

func validateProjectionEvolution(tableName, previousSchema string, currentColumns []ProjectedColumnDescriptor) error {
	previous := make(map[string]projectionSignatureField)
	for entry := range strings.SplitSeq(previousSchema, ";") {
		parts := strings.Split(entry, ":")
		if len(parts) < 2 {
			continue
		}
		previous[parts[0]] = projectionSignatureField{kind: parts[1], nullable: len(parts) == 3 && parts[2] == "optional"}
	}
	for _, current := range currentColumns {
		old, ok := previous[current.Name]
		if !ok {
			continue
		}
		if old.kind != current.ProtoKind {
			return fmt.Errorf("projection column %s.%s changed protobuf kind from %s to %s", tableName, current.Name, old.kind, current.ProtoKind)
		}
		if old.nullable == current.Nullable {
			continue
		}
		if current.LegacyOneofPresenceRepair && !old.nullable && current.Nullable {
			continue
		}
		return fmt.Errorf("projection column %s.%s changed presence semantics", tableName, current.Name)
	}
	return nil
}

type generatedTableColumn struct {
	name         string
	sqliteType   string
	notNull      int
	defaultValue any
}

func generatedTableColumnsContext(ctx context.Context, q DBTX, tableName string) (columns []generatedTableColumn, err error) {
	rows, err := q.QueryContext(ctx, `PRAGMA table_info(`+quoteSQLiteIdentifier(tableName)+`)`)
	if err != nil {
		return nil, fmt.Errorf("read columns for %s: %w", tableName, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close columns for %s: %w", tableName, closeErr)
		}
	}()
	for rows.Next() {
		var cid int
		var column generatedTableColumn
		var primaryKey int
		if err := rows.Scan(&cid, &column.name, &column.sqliteType, &column.notNull, &column.defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("scan columns for %s: %w", tableName, err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns for %s: %w", tableName, err)
	}
	return columns, nil
}

func projectedColumnSQL(column ProjectedColumnDescriptor) string {
	statement := quoteSQLiteIdentifier(column.Name) + " " + column.SQLiteType
	if column.Nullable {
		return statement
	}
	return statement + " NOT NULL DEFAULT " + column.DefaultSQL
}

func normalizeSQLiteDefault(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(strings.ToUpper(fmt.Sprint(value)))
}

func reprojectGeneratedTableContext(ctx context.Context, q DBTX, binding GeneratedTableBinding) (err error) {
	if len(binding.ProjectedColumns) == 0 {
		return nil
	}
	rows, err := q.QueryContext(ctx, `SELECT id, data FROM `+quoteSQLiteIdentifier(binding.Descriptor.TableName))
	if err != nil {
		return fmt.Errorf("query rows for reprojection from %s: %w", binding.Descriptor.TableName, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close reprojection rows from %s: %w", binding.Descriptor.TableName, closeErr)
		}
	}()
	type reprojectRow struct {
		id   string
		data []byte
	}
	buffer := make([]reprojectRow, 0)
	for rows.Next() {
		var row reprojectRow
		if err := rows.Scan(&row.id, &row.data); err != nil {
			return fmt.Errorf("scan reprojection row from %s: %w", binding.Descriptor.TableName, err)
		}
		buffer = append(buffer, row)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate reprojection rows from %s: %w", binding.Descriptor.TableName, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close reprojection rows from %s: %w", binding.Descriptor.TableName, err)
	}
	assignments := make([]string, 0, len(binding.ProjectedColumns))
	for _, column := range binding.ProjectedColumns {
		assignments = append(assignments, quoteSQLiteIdentifier(column.Name)+" = ?")
	}
	updateSQL := `UPDATE ` + quoteSQLiteIdentifier(binding.Descriptor.TableName) + ` SET ` + strings.Join(assignments, ", ") + ` WHERE id = ?`
	for _, row := range buffer {
		message := binding.NewMessage()
		if message == nil {
			return fmt.Errorf("binding for %s created nil message", binding.Descriptor.TypeName)
		}
		if err := proto.Unmarshal(row.data, message); err != nil {
			return fmt.Errorf("unmarshal reprojection row %s/%s: %w", binding.Descriptor.TableName, row.id, err)
		}
		values, err := binding.ProjectedValues(message)
		if err != nil {
			return fmt.Errorf("project row %s/%s: %w", binding.Descriptor.TableName, row.id, err)
		}
		values = append(values, row.id)
		if _, err := q.ExecContext(ctx, updateSQL, values...); err != nil {
			return fmt.Errorf("reproject row %s/%s: %w", binding.Descriptor.TableName, row.id, err)
		}
	}
	return nil
}

func CloseRows(rows *sql.Rows, operation string) error {
	if rows == nil {
		return nil
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close %s rows: %w", operation, err)
	}
	return nil
}

func NowNs() int64 {
	return time.Now().UnixNano()
}

func UUIDv7() (string, error) {
	var uuidBytes [16]byte
	if _, err := rand.Read(uuidBytes[:]); err != nil {
		return "", fmt.Errorf("generate random bytes for uuidv7: %w", err)
	}
	milliseconds := uint64(time.Now().UnixMilli())
	uuidBytes[0] = byte(milliseconds >> 40)
	uuidBytes[1] = byte(milliseconds >> 32)
	uuidBytes[2] = byte(milliseconds >> 24)
	uuidBytes[3] = byte(milliseconds >> 16)
	uuidBytes[4] = byte(milliseconds >> 8)
	uuidBytes[5] = byte(milliseconds)
	uuidBytes[6] = (uuidBytes[6] & 0x0f) | 0x70
	uuidBytes[8] = (uuidBytes[8] & 0x3f) | 0x80
	segment1 := binary.BigEndian.Uint32(uuidBytes[0:4])
	segment2 := binary.BigEndian.Uint16(uuidBytes[4:6])
	segment3 := binary.BigEndian.Uint16(uuidBytes[6:8])
	segment4 := binary.BigEndian.Uint16(uuidBytes[8:10])
	segment5High := binary.BigEndian.Uint16(uuidBytes[10:12])
	segment5Low := binary.BigEndian.Uint32(uuidBytes[12:16])
	segment5 := (uint64(segment5High) << 32) | uint64(segment5Low)
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", segment1, segment2, segment3, segment4, segment5), nil
}

func ValidateUUIDv7(id string) error {
	if len(id) != 36 {
		return fmt.Errorf("invalid uuidv7 %q: expected 36 characters", id)
	}
	for index, character := range id {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if character != '-' {
				return fmt.Errorf("invalid uuidv7 %q: expected hyphen at character %d", id, index+1)
			}
			continue
		}
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return fmt.Errorf("invalid uuidv7 %q: expected canonical lowercase hexadecimal", id)
		}
	}
	if id[14] != '7' {
		return fmt.Errorf("invalid uuidv7 %q: version is not 7", id)
	}
	if !strings.ContainsRune("89ab", rune(id[19])) {
		return fmt.Errorf("invalid uuidv7 %q: invalid RFC variant", id)
	}
	return nil
}

func ValidateUUID(id string) error {
	return ValidateUUIDv7(id)
}

func TypeURL(typeName string) string {
	return "type.googleapis.com/" + typeName
}

func TypeNameFromURL(typeURL string) string {
	if typeURL == "" {
		return ""
	}
	lastSlash := strings.LastIndex(typeURL, "/")
	if lastSlash == -1 || lastSlash == len(typeURL)-1 {
		return typeURL
	}
	return typeURL[lastSlash+1:]
}

func MarshalAnyJSON(message proto.Message) (json.RawMessage, error) {
	anyMessage, err := anypb.New(message)
	if err != nil {
		return nil, fmt.Errorf("marshal any wrapper: %w", err)
	}
	dataJSON, err := protojson.Marshal(anyMessage)
	if err != nil {
		return nil, fmt.Errorf("marshal any as json: %w", err)
	}
	return json.RawMessage(dataJSON), nil
}

func MarshalTypeOnlyAnyJSON(typeName string) (json.RawMessage, error) {
	anyMessage := &anypb.Any{TypeUrl: TypeURL(typeName)}
	dataJSON, err := protojson.Marshal(anyMessage)
	if err != nil {
		return nil, fmt.Errorf("marshal type-only any as json: %w", err)
	}
	return json.RawMessage(dataJSON), nil
}

func ReadJSONL(r io.Reader, visit func(JSONLRecord, int) error) error {
	reader := bufio.NewReader(r)
	lineNumber := 0
	for {
		lineNumber++
		line, readErr := reader.ReadString('\n')
		if len(strings.TrimSpace(line)) == 0 {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			if readErr != nil {
				return fmt.Errorf("read jsonl line %d: %w", lineNumber, readErr)
			}
			continue
		}
		var record JSONLRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return fmt.Errorf("decode jsonl line %d: %w", lineNumber, err)
		}
		if err := visit(record, lineNumber); err != nil {
			return err
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("read jsonl line %d: %w", lineNumber, readErr)
		}
	}
}

type anyTypeEnvelope struct {
	Type string `json:"@type"`
}

func TypeNameFromAnyJSON(data json.RawMessage) (string, error) {
	envelope := anyTypeEnvelope{}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", fmt.Errorf("unmarshal any json: %w", err)
	}
	typeName := TypeNameFromURL(envelope.Type)
	if typeName == "" {
		return "", errors.New("empty @type")
	}
	return typeName, nil
}

func UnknownInsert(q DBTX, typeName string, record JSONLRecord) error {
	return UnknownInsertContext(context.Background(), q, typeName, record)
}

func UnknownInsertContext(ctx context.Context, q DBTX, typeName string, record JSONLRecord) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	if strings.TrimSpace(typeName) == "" {
		return errors.New("empty type name")
	}
	deletedInt := 0
	if record.Deleted {
		deletedInt = 1
	}
	canonicalData, err := canonicalJSON(record.Data)
	if err != nil {
		return fmt.Errorf("canonicalize unknown row for %s/%s: %w", typeName, record.ID, err)
	}
	var localAtNs int64
	var localDeleted int
	var localData string
	selectErr := q.QueryRowContext(ctx, `SELECT at_ns, deleted, data_json FROM `+CoreTableUnknownName+` WHERE type_name = ? AND id = ?`, typeName, record.ID).Scan(&localAtNs, &localDeleted, &localData)
	if selectErr == nil && localAtNs == record.AtNs {
		if localDeleted == deletedInt && localData == string(canonicalData) {
			return nil
		}
		return &ConflictError{TypeName: typeName, ID: record.ID, AtNs: record.AtNs, LocalDeleted: localDeleted != 0, RemoteDeleted: record.Deleted}
	}
	if selectErr != nil && !errors.Is(selectErr, sql.ErrNoRows) {
		return fmt.Errorf("read unknown row for %s/%s: %w", typeName, record.ID, selectErr)
	}
	upsertUnknownSQL := `INSERT INTO ` + CoreTableUnknownName + ` (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(type_name, id) DO UPDATE SET at_ns = excluded.at_ns, deleted = excluded.deleted, data_json = excluded.data_json
WHERE excluded.at_ns > at_ns`
	if _, err := q.ExecContext(ctx, upsertUnknownSQL, typeName, record.ID, record.AtNs, deletedInt, string(canonicalData)); err != nil {
		return fmt.Errorf("upsert unknown row for %s/%s/%d: %w", typeName, record.ID, record.AtNs, err)
	}
	return nil
}

func CompactUnknownLatest(q DBTX) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	return nil
}

func canonicalJSON(data json.RawMessage) ([]byte, error) {
	var value any
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

func ReplayUnknownByType(q DBTX, typeName string, apply func(JSONLRecord) error) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	if apply == nil {
		return errors.New("nil apply")
	}
	if strings.TrimSpace(typeName) == "" {
		return errors.New("empty type name")
	}
	if err := CompactUnknownLatest(q); err != nil {
		return err
	}
	ctx := context.Background()
	selectUnknownSQL := `SELECT id, at_ns, deleted, data_json FROM ` + CoreTableUnknownName + ` WHERE type_name = ? ORDER BY at_ns ASC, id ASC, rowid ASC`
	rows, err := q.QueryContext(ctx, selectUnknownSQL, typeName)
	if err != nil {
		return fmt.Errorf("select unknown rows for %s: %w", typeName, err)
	}
	type unknownReplayRow struct {
		id          string
		atNs        int64
		deletedInt  int
		dataJSONStr string
	}
	replayRows := make([]unknownReplayRow, 0)
	for rows.Next() {
		row := unknownReplayRow{}
		if err := rows.Scan(&row.id, &row.atNs, &row.deletedInt, &row.dataJSONStr); err != nil {
			if closeErr := CloseRows(rows, "unknown rows"); closeErr != nil {
				return fmt.Errorf("scan unknown row for %s: %w (additionally, %v)", typeName, err, closeErr)
			}
			return fmt.Errorf("scan unknown row for %s: %w", typeName, err)
		}
		replayRows = append(replayRows, row)
	}
	if err := rows.Err(); err != nil {
		if closeErr := CloseRows(rows, "unknown rows"); closeErr != nil {
			return fmt.Errorf("iterate unknown rows for %s: %w (additionally, %v)", typeName, err, closeErr)
		}
		return fmt.Errorf("iterate unknown rows for %s: %w", typeName, err)
	}
	if err := CloseRows(rows, "unknown rows"); err != nil {
		return err
	}
	for _, row := range replayRows {
		record := JSONLRecord{
			ID:      row.id,
			Deleted: row.deletedInt != 0,
			AtNs:    row.atNs,
			Data:    json.RawMessage(row.dataJSONStr),
		}
		if err := apply(record); err != nil {
			return fmt.Errorf("apply unknown row for %s/%s: %w", typeName, row.id, err)
		}
		deleteUnknownRowsSQL := `DELETE FROM ` + CoreTableUnknownName + ` WHERE type_name = ? AND id = ?`
		if _, err := q.ExecContext(ctx, deleteUnknownRowsSQL, typeName, row.id); err != nil {
			return fmt.Errorf("delete unknown rows for %s/%s: %w", typeName, row.id, err)
		}
	}
	return nil
}

func TransferUnknownSyncContext(ctx context.Context, q DBTX, typeName, tableName, objectID string) error {
	query := `INSERT INTO ` + CoreTableSyncName + ` (object_id, table_name, at_ns, remote)
SELECT id, ?, at_ns, remote FROM ` + CoreTableUnknownSyncName + ` WHERE type_name = ? AND id = ?
ON CONFLICT(object_id, table_name, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)`
	if _, err := q.ExecContext(ctx, query, tableName, typeName, objectID); err != nil {
		return fmt.Errorf("transfer unknown sync state for %s/%s: %w", typeName, objectID, err)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM `+CoreTableUnknownSyncName+` WHERE type_name = ? AND id = ?`, typeName, objectID); err != nil {
		return fmt.Errorf("delete transferred unknown sync state for %s/%s: %w", typeName, objectID, err)
	}
	return nil
}

func SyncNeedsSend(q DBTX, objectID, tableName, remote string, atNs int64) (bool, error) {
	return SyncNeedsSendContext(context.Background(), q, objectID, tableName, remote, atNs)
}

func SyncNeedsSendContext(ctx context.Context, q DBTX, objectID, tableName, remote string, atNs int64) (bool, error) {
	if remote == "" {
		return true, nil
	}
	var syncedAtNs int64
	selectSyncSQL := `SELECT at_ns FROM ` + CoreTableSyncName + ` WHERE object_id = ? AND table_name = ? AND remote = ?`
	err := q.QueryRowContext(ctx, selectSyncSQL, objectID, tableName, remote).Scan(&syncedAtNs)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("select sync row for %s/%s/%s: %w", tableName, objectID, remote, err)
	}
	return syncedAtNs < atNs, nil
}

func SyncUpsert(q DBTX, objectID, tableName, remote string, atNs int64) error {
	return SyncUpsertContext(context.Background(), q, objectID, tableName, remote, atNs)
}

func SyncUpsertContext(ctx context.Context, q DBTX, objectID, tableName, remote string, atNs int64) error {
	if remote == "" {
		return nil
	}
	upsertSyncSQL := `INSERT INTO ` + CoreTableSyncName + ` (object_id, table_name, at_ns, remote) VALUES (?, ?, ?, ?) ON CONFLICT(object_id, table_name, remote) DO UPDATE SET at_ns = CASE WHEN excluded.at_ns > at_ns THEN excluded.at_ns ELSE at_ns END`
	if _, err := q.ExecContext(ctx, upsertSyncSQL, objectID, tableName, atNs, remote); err != nil {
		return fmt.Errorf("upsert sync row for %s/%s/%s: %w", tableName, objectID, remote, err)
	}
	return nil
}

func LocalMaxAtNs(q DBTX, tableName, objectID string) (int64, error) {
	return LocalMaxAtNsContext(context.Background(), q, tableName, objectID)
}

func LocalMaxAtNsContext(ctx context.Context, q DBTX, tableName, objectID string) (int64, error) {
	maxAtNs := int64(-1)
	var rowAtNs int64
	rowErr := q.QueryRowContext(ctx, `SELECT at_ns FROM "`+tableName+`" WHERE id = ?`, objectID).Scan(&rowAtNs)
	if rowErr != nil && !errors.Is(rowErr, sql.ErrNoRows) {
		return 0, fmt.Errorf("select row timestamp for %s/%s: %w", tableName, objectID, rowErr)
	}
	if rowErr == nil && rowAtNs > maxAtNs {
		maxAtNs = rowAtNs
	}
	var tombstoneAtNs int64
	selectTombstoneSQL := `SELECT at_ns FROM ` + CoreTableDeletedName + ` WHERE table_name = ? AND id = ?`
	tombstoneErr := q.QueryRowContext(ctx, selectTombstoneSQL, tableName, objectID).Scan(&tombstoneAtNs)
	if tombstoneErr != nil && !errors.Is(tombstoneErr, sql.ErrNoRows) {
		return 0, fmt.Errorf("select tombstone timestamp for %s/%s: %w", tableName, objectID, tombstoneErr)
	}
	if tombstoneErr == nil && tombstoneAtNs > maxAtNs {
		maxAtNs = tombstoneAtNs
	}
	return maxAtNs, nil
}

func NextObjectAtNsContext(ctx context.Context, q DBTX, tableName, objectID string) (int64, error) {
	currentAtNs, err := LocalMaxAtNsContext(ctx, q, tableName, objectID)
	if err != nil {
		return 0, err
	}
	wallAtNs := NowNs()
	if currentAtNs >= wallAtNs {
		if currentAtNs == int64(^uint64(0)>>1) {
			return 0, errors.New("object timestamp exhausted int64")
		}
		return currentAtNs + 1, nil
	}
	return wallAtNs, nil
}

func UnknownSyncNeedsSendContext(ctx context.Context, q DBTX, typeName, objectID, remote string, atNs int64) (bool, error) {
	if remote == "" {
		return true, nil
	}
	var syncedAtNs int64
	err := q.QueryRowContext(ctx, `SELECT at_ns FROM `+CoreTableUnknownSyncName+` WHERE type_name = ? AND id = ? AND remote = ?`, typeName, objectID, remote).Scan(&syncedAtNs)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read unknown sync state for %s/%s/%s: %w", typeName, objectID, remote, err)
	}
	return syncedAtNs < atNs, nil
}

func UnknownSyncUpsertContext(ctx context.Context, q DBTX, typeName, objectID, remote string, atNs int64) error {
	if remote == "" {
		return nil
	}
	query := `INSERT INTO ` + CoreTableUnknownSyncName + ` (type_name, id, at_ns, remote) VALUES (?, ?, ?, ?)
ON CONFLICT(type_name, id, remote) DO UPDATE SET at_ns = max(at_ns, excluded.at_ns)`
	if _, err := q.ExecContext(ctx, query, typeName, objectID, atNs, remote); err != nil {
		return fmt.Errorf("upsert unknown sync state for %s/%s/%s: %w", typeName, objectID, remote, err)
	}
	return nil
}

func IntrospectTables(q DBTX, descriptors []GeneratedTableDescriptor) ([]TableIntrospection, error) {
	if q == nil {
		return nil, errors.New("nil DBTX")
	}
	introspectionRows := make([]TableIntrospection, 0, len(descriptors))
	for _, descriptor := range descriptors {
		objectCount, err := tableObjectCount(q, descriptor.TableName)
		if err != nil {
			return nil, err
		}
		payloadBytes, err := tablePayloadBytes(q, descriptor.TableName)
		if err != nil {
			return nil, err
		}
		introspectionRows = append(introspectionRows, TableIntrospection{
			Descriptor:   descriptor,
			ObjectCount:  objectCount,
			PayloadBytes: payloadBytes,
		})
	}
	return introspectionRows, nil
}

func tableObjectCount(q DBTX, tableName string) (int64, error) {
	ctx := context.Background()
	var objectCount int64
	tableNameIdentifier := quoteSQLiteIdentifier(tableName)
	query := `SELECT COUNT(*) FROM ` + tableNameIdentifier
	if err := q.QueryRowContext(ctx, query).Scan(&objectCount); err != nil {
		return 0, fmt.Errorf("count objects for table %s: %w", tableName, err)
	}
	return objectCount, nil
}

func tablePayloadBytes(q DBTX, tableName string) (int64, error) {
	ctx := context.Background()
	columnNames, err := tableColumnNames(q, tableName)
	if err != nil {
		return 0, err
	}
	tableNameIdentifier := quoteSQLiteIdentifier(tableName)
	var payloadBytes int64
	var query string
	if containsColumn(columnNames, dataColumnName) {
		query = `SELECT COALESCE(SUM(LENGTH(` + quoteSQLiteIdentifier(dataColumnName) + `)), 0) FROM ` + tableNameIdentifier
	} else {
		query = `SELECT COALESCE(SUM(` + estimatedRowPayloadBytesSQL(columnNames) + `), 0) FROM ` + tableNameIdentifier
	}
	if err := q.QueryRowContext(ctx, query).Scan(&payloadBytes); err != nil {
		return 0, fmt.Errorf("read payload size for table %s: %w", tableName, err)
	}
	return payloadBytes, nil
}

func tableColumnNames(q DBTX, tableName string) ([]string, error) {
	ctx := context.Background()
	query := `PRAGMA table_info(` + quoteSQLiteIdentifier(tableName) + `)`
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read columns for table %s: %w", tableName, err)
	}
	columnNames := make([]string, 0)
	for rows.Next() {
		var cid int
		var name string
		var colType string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &pk); err != nil {
			if closeErr := CloseRows(rows, "table columns"); closeErr != nil {
				return nil, fmt.Errorf("scan table column for %s: %w (additionally, %v)", tableName, err, closeErr)
			}
			return nil, fmt.Errorf("scan table column for %s: %w", tableName, err)
		}
		columnNames = append(columnNames, name)
	}
	if err := rows.Err(); err != nil {
		if closeErr := CloseRows(rows, "table columns"); closeErr != nil {
			return nil, fmt.Errorf("iterate table columns for %s: %w (additionally, %v)", tableName, err, closeErr)
		}
		return nil, fmt.Errorf("iterate table columns for %s: %w", tableName, err)
	}
	if err := CloseRows(rows, "table columns"); err != nil {
		return nil, err
	}
	return columnNames, nil
}

func containsColumn(columnNames []string, targetColumn string) bool {
	for _, columnName := range columnNames {
		if columnName == targetColumn {
			return true
		}
	}
	return false
}

func estimatedRowPayloadBytesSQL(columnNames []string) string {
	if len(columnNames) == 0 {
		return "0"
	}
	estimatedColumns := make([]string, 0, len(columnNames))
	for _, columnName := range columnNames {
		quotedColumnName := quoteSQLiteIdentifier(columnName)
		estimatedColumns = append(estimatedColumns, `COALESCE(LENGTH(CAST(`+quotedColumnName+` AS BLOB)), 0)`)
	}
	return strings.Join(estimatedColumns, " + ")
}

func quoteSQLiteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
