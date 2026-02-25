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

func EnsureCoreTables(q DBTX) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	ctx := context.Background()
	if _, err := q.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _deleted (table_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, PRIMARY KEY (table_name, id))`); err != nil {
		return fmt.Errorf("create _deleted table: %w", err)
	}
	if _, err := q.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _sync (object_id TEXT NOT NULL, table_name TEXT NOT NULL, at_ns INTEGER NOT NULL, remote TEXT NOT NULL, PRIMARY KEY (object_id, table_name, remote))`); err != nil {
		return fmt.Errorf("create _sync table: %w", err)
	}
	if _, err := q.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _proprdb_schema (table_name TEXT PRIMARY KEY, schema_hash TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("create _proprdb_schema table: %w", err)
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

func SyncNeedsSend(q DBTX, objectID, tableName, remote string, atNs int64) (bool, error) {
	ctx := context.Background()
	var syncedAtNs int64
	err := q.QueryRowContext(ctx, `SELECT at_ns FROM _sync WHERE object_id = ? AND table_name = ? AND remote = ?`, objectID, tableName, remote).Scan(&syncedAtNs)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("select sync row for %s/%s/%s: %w", tableName, objectID, remote, err)
	}
	return syncedAtNs < atNs, nil
}

func SyncUpsert(q DBTX, objectID, tableName, remote string, atNs int64) error {
	ctx := context.Background()
	if _, err := q.ExecContext(ctx, `INSERT INTO _sync (object_id, table_name, at_ns, remote) VALUES (?, ?, ?, ?) ON CONFLICT(object_id, table_name, remote) DO UPDATE SET at_ns = CASE WHEN excluded.at_ns > at_ns THEN excluded.at_ns ELSE at_ns END`, objectID, tableName, atNs, remote); err != nil {
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
	tombstoneErr := q.QueryRowContext(ctx, `SELECT at_ns FROM _deleted WHERE table_name = ? AND id = ?`, tableName, objectID).Scan(&tombstoneAtNs)
	if tombstoneErr != nil && !errors.Is(tombstoneErr, sql.ErrNoRows) {
		return 0, fmt.Errorf("select tombstone timestamp for %s/%s: %w", tableName, objectID, tombstoneErr)
	}
	if tombstoneErr == nil && tombstoneAtNs > maxAtNs {
		maxAtNs = tombstoneAtNs
	}
	return maxAtNs, nil
}
