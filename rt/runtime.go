package proprdbrt

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
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
	dataColumnName           = "data"
)

type DBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type JSONLRecord struct {
	ID      string          `json:"id"`
	Deleted bool            `json:"deleted,omitempty"`
	AtNs    int64           `json:"atNs"`
	Data    json.RawMessage `json:"data"`
}

type GeneratedTableDescriptor struct {
	TableName   string
	TypeName    string
	IsCore      bool
	SyncEnabled bool
}

type TableIntrospection struct {
	Descriptor     GeneratedTableDescriptor
	ObjectCount    int64
	DiskUsageBytes int64
}

func EnsureCoreTables(q DBTX) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	ctx := context.Background()
	createDeletedTableSQL := `CREATE TABLE IF NOT EXISTS ` + CoreTableDeletedName + ` (table_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, PRIMARY KEY (table_name, id))`
	if _, err := q.ExecContext(ctx, createDeletedTableSQL); err != nil {
		return fmt.Errorf("create _deleted table: %w", err)
	}
	createSyncTableSQL := `CREATE TABLE IF NOT EXISTS ` + CoreTableSyncName + ` (object_id TEXT NOT NULL, table_name TEXT NOT NULL, at_ns INTEGER NOT NULL, remote TEXT NOT NULL, PRIMARY KEY (object_id, table_name, remote))`
	if _, err := q.ExecContext(ctx, createSyncTableSQL); err != nil {
		return fmt.Errorf("create _sync table: %w", err)
	}
	createSchemaStateTableSQL := `CREATE TABLE IF NOT EXISTS ` + CoreTableSchemaStateName + ` (table_name TEXT PRIMARY KEY, schema_hash TEXT NOT NULL)`
	if _, err := q.ExecContext(ctx, createSchemaStateTableSQL); err != nil {
		return fmt.Errorf("create _proprdb_schema table: %w", err)
	}
	createUnknownTableSQL := `CREATE TABLE IF NOT EXISTS ` + CoreTableUnknownName + ` (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, deleted INTEGER NOT NULL, data_json TEXT NOT NULL, PRIMARY KEY (type_name, id, at_ns))`
	if _, err := q.ExecContext(ctx, createUnknownTableSQL); err != nil {
		return fmt.Errorf("create _unknown_types table: %w", err)
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

func ValidateUUID(id string) error {
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		return fmt.Errorf("invalid uuid %q: expected 5 parts", id)
	}
	lengths := []int{8, 4, 4, 4, 12}
	for index, part := range parts {
		if len(part) != lengths[index] {
			return fmt.Errorf("invalid uuid %q: unexpected length for part %d", id, index+1)
		}
		if _, err := hex.DecodeString(part); err != nil {
			return fmt.Errorf("invalid uuid %q: %w", id, err)
		}
	}
	return nil
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
	decoder := json.NewDecoder(r)
	lineNumber := 0
	for {
		lineNumber++
		var record JSONLRecord
		if err := decoder.Decode(&record); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("decode jsonl line %d: %w", lineNumber, err)
		}
		if err := visit(record, lineNumber); err != nil {
			return err
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
	if q == nil {
		return errors.New("nil DBTX")
	}
	if strings.TrimSpace(typeName) == "" {
		return errors.New("empty type name")
	}
	ctx := context.Background()
	deletedInt := 0
	if record.Deleted {
		deletedInt = 1
	}
	upsertUnknownSQL := `INSERT INTO ` + CoreTableUnknownName + ` (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?)`
	if _, err := q.ExecContext(ctx, upsertUnknownSQL, typeName, record.ID, record.AtNs, deletedInt, string(record.Data)); err != nil {
		return fmt.Errorf("insert unknown row for %s/%s/%d: %w", typeName, record.ID, record.AtNs, err)
	}
	return nil
}

func CompactUnknownLatest(q DBTX) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	ctx := context.Background()
	compactSQL := `DELETE FROM ` + CoreTableUnknownName + ` WHERE rowid NOT IN (
SELECT MAX(kept.rowid)
FROM ` + CoreTableUnknownName + ` kept
JOIN (
	SELECT type_name, id, MAX(at_ns) AS max_at_ns
	FROM ` + CoreTableUnknownName + `
	GROUP BY type_name, id
) latest
ON latest.type_name = kept.type_name AND latest.id = kept.id AND latest.max_at_ns = kept.at_ns
GROUP BY kept.type_name, kept.id
)`
	if _, err := q.ExecContext(ctx, compactSQL); err != nil {
		return fmt.Errorf("compact unknown rows: %w", err)
	}
	return nil
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

func SyncNeedsSend(q DBTX, objectID, tableName, remote string, atNs int64) (bool, error) {
	if remote == "" {
		return true, nil
	}
	ctx := context.Background()
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
	if remote == "" {
		return nil
	}
	ctx := context.Background()
	upsertSyncSQL := `INSERT INTO ` + CoreTableSyncName + ` (object_id, table_name, at_ns, remote) VALUES (?, ?, ?, ?) ON CONFLICT(object_id, table_name, remote) DO UPDATE SET at_ns = CASE WHEN excluded.at_ns > at_ns THEN excluded.at_ns ELSE at_ns END`
	if _, err := q.ExecContext(ctx, upsertSyncSQL, objectID, tableName, atNs, remote); err != nil {
		return fmt.Errorf("upsert sync row for %s/%s/%s: %w", tableName, objectID, remote, err)
	}
	return nil
}

func LocalMaxAtNs(q DBTX, tableName, objectID string) (int64, error) {
	ctx := context.Background()
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
		diskUsageBytes, err := tableDiskUsageBytes(q, descriptor.TableName)
		if err != nil {
			return nil, err
		}
		introspectionRows = append(introspectionRows, TableIntrospection{
			Descriptor:     descriptor,
			ObjectCount:    objectCount,
			DiskUsageBytes: diskUsageBytes,
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

func tableDiskUsageBytes(q DBTX, tableName string) (int64, error) {
	ctx := context.Background()
	columnNames, err := tableColumnNames(q, tableName)
	if err != nil {
		return 0, err
	}
	tableNameIdentifier := quoteSQLiteIdentifier(tableName)
	var diskUsageBytes int64
	var query string
	if containsColumn(columnNames, dataColumnName) {
		query = `SELECT COALESCE(SUM(LENGTH(` + quoteSQLiteIdentifier(dataColumnName) + `)), 0) FROM ` + tableNameIdentifier
	} else {
		query = `SELECT COALESCE(SUM(` + estimatedRowPayloadBytesSQL(columnNames) + `), 0) FROM ` + tableNameIdentifier
	}
	if err := q.QueryRowContext(ctx, query).Scan(&diskUsageBytes); err != nil {
		return 0, fmt.Errorf("read disk usage for table %s: %w", tableName, err)
	}
	return diskUsageBytes, nil
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
