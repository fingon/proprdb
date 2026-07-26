package proprdbrt

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func validateBinding(binding GeneratedTableBinding) error {
	if binding.Descriptor.TableName == "" || binding.Descriptor.TypeName == "" {
		return errors.New("binding has empty table or type name")
	}
	if binding.NewMessage == nil || binding.ProjectedValues == nil {
		return fmt.Errorf("binding for %s has nil callback", binding.Descriptor.TypeName)
	}
	if binding.InsertSQL == "" || binding.UpsertSQL == "" {
		return fmt.Errorf("binding for %s has empty write SQL", binding.Descriptor.TypeName)
	}
	return nil
}

func bindingValues(binding GeneratedTableBinding, id string, atNs int64, message proto.Message) ([]any, error) {
	dataBytes, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshal %s: %w", binding.Descriptor.TypeName, err)
	}
	projectedValues, err := binding.ProjectedValues(message)
	if err != nil {
		return nil, fmt.Errorf("project %s: %w", binding.Descriptor.TypeName, err)
	}
	values := make([]any, 0, 3+len(projectedValues))
	values = append(values, id, atNs, dataBytes)
	values = append(values, projectedValues...)
	return values, nil
}

func WriteLocalObjectContext(ctx context.Context, q DBTX, binding GeneratedTableBinding, id string, message proto.Message, insert bool) (atNs int64, err error) {
	if q == nil {
		return 0, errors.New("nil DBTX")
	}
	if err := validateBinding(binding); err != nil {
		return 0, err
	}
	if id == "" {
		return 0, errors.New("empty id")
	}
	if err := ValidateUUIDv7(id); err != nil {
		return 0, err
	}
	if message == nil {
		return 0, errors.New("nil message")
	}
	return atNs, q.WithTransaction(ctx, func(tx DBTX) error {
		allocatedAtNs, allocateErr := NextObjectAtNsContext(ctx, tx, binding.Descriptor.TableName, id)
		if allocateErr != nil {
			return allocateErr
		}
		values, valuesErr := bindingValues(binding, id, allocatedAtNs, message)
		if valuesErr != nil {
			return valuesErr
		}
		if _, deleteErr := tx.ExecContext(ctx, `DELETE FROM `+CoreTableDeletedName+` WHERE table_name = ? AND id = ?`, binding.Descriptor.TableName, id); deleteErr != nil {
			return fmt.Errorf("delete tombstone for %s/%s: %w", binding.Descriptor.TableName, id, deleteErr)
		}
		writeSQL := binding.UpsertSQL
		if insert {
			writeSQL = binding.InsertSQL
		}
		if _, writeErr := tx.ExecContext(ctx, writeSQL, values...); writeErr != nil {
			return fmt.Errorf("write %s/%s: %w", binding.Descriptor.TableName, id, writeErr)
		}
		atNs = allocatedAtNs
		queueGeneratedTableChange(tx, binding, id, allocatedAtNs, false, message)
		return nil
	})
}

func DeleteLocalBoundObjectContext(ctx context.Context, q DBTX, binding GeneratedTableBinding, id string) error {
	if q == nil {
		return errors.New("nil DBTX")
	}
	if err := validateBinding(binding); err != nil {
		return err
	}
	if id == "" {
		return errors.New("empty id")
	}
	if err := ValidateUUIDv7(id); err != nil {
		return err
	}
	return q.WithTransaction(ctx, func(tx DBTX) error {
		atNs, err := NextObjectAtNsContext(ctx, tx, binding.Descriptor.TableName, id)
		if err != nil {
			return err
		}
		if err := applyBoundDeletionSQL(ctx, tx, binding, id, atNs); err != nil {
			return err
		}
		queueGeneratedTableChange(tx, binding, id, atNs, true, nil)
		return nil
	})
}

func ApplyIncomingObjectContext(ctx context.Context, q DBTX, binding GeneratedTableBinding, record JSONLRecord) error {
	if err := validateBinding(binding); err != nil {
		return err
	}
	if err := ValidateUUIDv7(record.ID); err != nil {
		return err
	}
	if record.Deleted {
		return applyIncomingTombstoneContext(ctx, q, binding, record)
	}
	anyMessage := &anypb.Any{}
	if err := protojson.Unmarshal(record.Data, anyMessage); err != nil {
		return fmt.Errorf("unmarshal %s any json: %w", binding.Descriptor.TypeName, err)
	}
	message := binding.NewMessage()
	if message == nil {
		return fmt.Errorf("binding for %s created nil message", binding.Descriptor.TypeName)
	}
	if err := anypb.UnmarshalTo(anyMessage, message, proto.UnmarshalOptions{}); err != nil {
		return fmt.Errorf("unmarshal %s payload: %w", binding.Descriptor.TypeName, err)
	}
	localAtNs, err := LocalMaxAtNsContext(ctx, q, binding.Descriptor.TableName, record.ID)
	if err != nil {
		return err
	}
	if record.AtNs < localAtNs {
		return nil
	}
	if record.AtNs == localAtNs {
		var localBytes []byte
		liveErr := q.QueryRowContext(ctx, `SELECT data FROM "`+binding.Descriptor.TableName+`" WHERE id = ? AND at_ns = ?`, record.ID, record.AtNs).Scan(&localBytes)
		if liveErr == nil {
			localMessage := binding.NewMessage()
			if err := proto.Unmarshal(localBytes, localMessage); err != nil {
				return fmt.Errorf("unmarshal local equal-timestamp payload: %w", err)
			}
			if proto.Equal(localMessage, message) {
				return nil
			}
			return &ConflictError{TypeName: binding.Descriptor.TypeName, ID: record.ID, AtNs: record.AtNs}
		}
		if !errors.Is(liveErr, sql.ErrNoRows) {
			return fmt.Errorf("read equal-timestamp payload: %w", liveErr)
		}
		return &ConflictError{TypeName: binding.Descriptor.TypeName, ID: record.ID, AtNs: record.AtNs, LocalDeleted: true}
	}
	values, err := bindingValues(binding, record.ID, record.AtNs, message)
	if err != nil {
		return err
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM `+CoreTableDeletedName+` WHERE table_name = ? AND id = ?`, binding.Descriptor.TableName, record.ID); err != nil {
		return fmt.Errorf("delete tombstone for %s/%s: %w", binding.Descriptor.TableName, record.ID, err)
	}
	if _, err := q.ExecContext(ctx, binding.UpsertSQL, values...); err != nil {
		return fmt.Errorf("upsert %s/%s: %w", binding.Descriptor.TableName, record.ID, err)
	}
	queueGeneratedTableChange(q, binding, record.ID, record.AtNs, false, message)
	return nil
}

func applyIncomingTombstoneContext(ctx context.Context, q DBTX, binding GeneratedTableBinding, record JSONLRecord) error {
	localAtNs, err := LocalMaxAtNsContext(ctx, q, binding.Descriptor.TableName, record.ID)
	if err != nil {
		return err
	}
	if record.AtNs < localAtNs {
		return nil
	}
	if record.AtNs == localAtNs {
		var tombstoneAtNs int64
		err := q.QueryRowContext(ctx, `SELECT at_ns FROM `+CoreTableDeletedName+` WHERE table_name = ? AND id = ?`, binding.Descriptor.TableName, record.ID).Scan(&tombstoneAtNs)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read equal-timestamp tombstone: %w", err)
		}
		return &ConflictError{TypeName: binding.Descriptor.TypeName, ID: record.ID, AtNs: record.AtNs, RemoteDeleted: true}
	}
	if err := applyBoundDeletionSQL(ctx, q, binding, record.ID, record.AtNs); err != nil {
		return err
	}
	queueGeneratedTableChange(q, binding, record.ID, record.AtNs, true, nil)
	return nil
}

func applyTombstoneSQL(ctx context.Context, q DBTX, tableName, id string, atNs int64) error {
	query := `INSERT INTO ` + CoreTableDeletedName + ` (table_name, id, at_ns) VALUES (?, ?, ?)
ON CONFLICT(table_name, id) DO UPDATE SET at_ns = excluded.at_ns`
	if _, err := q.ExecContext(ctx, query, tableName, id, atNs); err != nil {
		return fmt.Errorf("insert tombstone for %s/%s: %w", tableName, id, err)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM "`+tableName+`" WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete from %s/%s: %w", tableName, id, err)
	}
	return nil
}

func applyBoundDeletionSQL(ctx context.Context, q DBTX, binding GeneratedTableBinding, id string, atNs int64) error {
	if binding.Descriptor.SyncEnabled {
		return applyTombstoneSQL(ctx, q, binding.Descriptor.TableName, id, atNs)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM "`+binding.Descriptor.TableName+`" WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete from %s/%s: %w", binding.Descriptor.TableName, id, err)
	}
	return nil
}

func ReadBoundJSONLContext(ctx context.Context, q DBTX, bindings []GeneratedTableBinding, remote string, reader io.Reader) error {
	if reader == nil {
		return errors.New("nil reader")
	}
	byType := make(map[string]GeneratedTableBinding, len(bindings))
	for _, binding := range bindings {
		if err := validateBinding(binding); err != nil {
			return err
		}
		if _, exists := byType[binding.Descriptor.TypeName]; exists {
			return fmt.Errorf("duplicate binding type %s", binding.Descriptor.TypeName)
		}
		byType[binding.Descriptor.TypeName] = binding
	}
	return ReadJSONL(reader, func(record JSONLRecord, lineNumber int) error {
		if record.ID == "" || len(record.Data) == 0 {
			return fmt.Errorf("jsonl line %d has empty id or data", lineNumber)
		}
		typeName, err := TypeNameFromAnyJSON(record.Data)
		if err != nil {
			return fmt.Errorf("read @type on line %d: %w", lineNumber, err)
		}
		binding, known := byType[typeName]
		if known && !binding.Descriptor.SyncEnabled {
			slog.Error("ignoring unsynced jsonl record", "type", typeName, "id", record.ID, "remote", remote, "line", lineNumber)
			return nil
		}
		return q.WithTransaction(ctx, func(tx DBTX) error {
			if known {
				if err := ApplyIncomingObjectContext(ctx, tx, binding, record); err != nil {
					return err
				}
				return SyncUpsertContext(ctx, tx, record.ID, binding.Descriptor.TableName, remote, record.AtNs)
			}
			if err := UnknownInsertContext(ctx, tx, typeName, record); err != nil {
				return err
			}
			return UnknownSyncUpsertContext(ctx, tx, typeName, record.ID, remote, record.AtNs)
		})
	})
}

func PrepareBoundJSONLContext(ctx context.Context, q DBTX, bindings []GeneratedTableBinding, remote string, writer io.Writer) (checkpoint JSONLCheckpoint, err error) {
	if writer == nil {
		return JSONLCheckpoint{}, errors.New("nil writer")
	}
	if err := EnsureCoreTablesContext(ctx, q); err != nil {
		return JSONLCheckpoint{}, err
	}
	err = q.WithTransaction(ctx, func(tx DBTX) error {
		var beginErr error
		checkpoint, beginErr = BeginJSONLBatchContext(ctx, tx, remote)
		if beginErr != nil {
			return beginErr
		}
		sequence := 0
		for _, binding := range bindings {
			if err := validateBinding(binding); err != nil {
				return err
			}
			if !binding.Descriptor.SyncEnabled {
				continue
			}
			query := `SELECT row.id, row.at_ns, row.data FROM "` + binding.Descriptor.TableName + `" row
LEFT JOIN ` + CoreTableSyncName + ` sync_row
ON sync_row.object_id = row.id AND sync_row.table_name = ? AND sync_row.remote = ?
WHERE ? = '' OR sync_row.at_ns IS NULL OR sync_row.at_ns < row.at_ns ORDER BY row.id`
			rows, queryErr := tx.QueryContext(ctx, query, binding.Descriptor.TableName, remote, remote)
			if queryErr != nil {
				return fmt.Errorf("select export rows for %s: %w", binding.Descriptor.TableName, queryErr)
			}
			defer closeRowsLog(rows, "export rows")
			for rows.Next() {
				var id string
				var atNs int64
				var dataBytes []byte
				if scanErr := rows.Scan(&id, &atNs, &dataBytes); scanErr != nil {
					closeRowsLog(rows, "export rows")
					return fmt.Errorf("scan export row for %s: %w", binding.Descriptor.TableName, scanErr)
				}
				message := binding.NewMessage()
				if unmarshalErr := proto.Unmarshal(dataBytes, message); unmarshalErr != nil {
					closeRowsLog(rows, "export rows")
					return fmt.Errorf("unmarshal export row for %s/%s: %w", binding.Descriptor.TableName, id, unmarshalErr)
				}
				dataJSON, marshalErr := MarshalAnyJSON(message)
				if marshalErr != nil {
					closeRowsLog(rows, "export rows")
					return marshalErr
				}
				record := JSONLRecord{ID: id, AtNs: atNs, Data: dataJSON}
				if stageErr := stageJSONLRecord(ctx, tx, checkpoint, sequence, binding.Descriptor.TableName, record); stageErr != nil {
					closeRowsLog(rows, "export rows")
					return stageErr
				}
				sequence++
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				closeRowsLog(rows, "export rows")
				return fmt.Errorf("iterate export rows for %s: %w", binding.Descriptor.TableName, rowsErr)
			}
		}
		nextSequence, stageErr := stageTombstonesAndUnknown(ctx, tx, bindings, remote, checkpoint, sequence)
		if stageErr != nil {
			return stageErr
		}
		_ = nextSequence
		return CompleteJSONLBatchContext(ctx, tx, checkpoint)
	})
	if err != nil {
		return JSONLCheckpoint{}, err
	}
	if err := writeStagedJSONL(ctx, q, checkpoint, writer); err != nil {
		if discardErr := DiscardJSONLContext(ctx, q, checkpoint); discardErr != nil {
			return JSONLCheckpoint{}, fmt.Errorf("%w (additionally, discard export batch: %v)", err, discardErr)
		}
		return JSONLCheckpoint{}, err
	}
	return checkpoint, nil
}

func stageJSONLRecord(ctx context.Context, q DBTX, checkpoint JSONLCheckpoint, sequence int, tableName string, record JSONLRecord) error {
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal staged jsonl record: %w", err)
	}
	line = append(line, '\n')
	query := `INSERT INTO ` + CoreTableExportEntryName + ` (batch_id, sequence, table_name, object_id, at_ns, record_json) VALUES (?, ?, ?, ?, ?, ?)`
	if _, err := q.ExecContext(ctx, query, checkpoint.BatchID, sequence, tableName, record.ID, record.AtNs, line); err != nil {
		return fmt.Errorf("stage export record: %w", err)
	}
	return nil
}

func stageTombstonesAndUnknown(ctx context.Context, q DBTX, bindings []GeneratedTableBinding, remote string, checkpoint JSONLCheckpoint, sequence int) (int, error) {
	byTable := make(map[string]GeneratedTableBinding, len(bindings))
	knownTypes := make(map[string]bool, len(bindings))
	for _, binding := range bindings {
		knownTypes[binding.Descriptor.TypeName] = true
		if binding.Descriptor.SyncEnabled {
			byTable[binding.Descriptor.TableName] = binding
		}
	}
	rows, err := q.QueryContext(ctx, `SELECT deleted.table_name, deleted.id, deleted.at_ns
FROM `+CoreTableDeletedName+` deleted LEFT JOIN `+CoreTableSyncName+` sync_row
ON sync_row.object_id = deleted.id AND sync_row.table_name = deleted.table_name AND sync_row.remote = ?
WHERE ? = '' OR sync_row.at_ns IS NULL OR sync_row.at_ns < deleted.at_ns
ORDER BY deleted.table_name, deleted.id`, remote, remote)
	if err != nil {
		return sequence, fmt.Errorf("select tombstones: %w", err)
	}
	defer closeRowsLog(rows, "tombstones")
	for rows.Next() {
		var tableName string
		var id string
		var atNs int64
		if err := rows.Scan(&tableName, &id, &atNs); err != nil {
			closeRowsLog(rows, "tombstones")
			return sequence, fmt.Errorf("scan tombstone: %w", err)
		}
		binding, ok := byTable[tableName]
		if !ok {
			continue
		}
		data, err := MarshalTypeOnlyAnyJSON(binding.Descriptor.TypeName)
		if err != nil {
			closeRowsLog(rows, "tombstones")
			return sequence, err
		}
		if err := stageJSONLRecord(ctx, q, checkpoint, sequence, tableName, JSONLRecord{ID: id, Deleted: true, AtNs: atNs, Data: data}); err != nil {
			closeRowsLog(rows, "tombstones")
			return sequence, err
		}
		sequence++
	}
	if err := rows.Err(); err != nil {
		closeRowsLog(rows, "tombstones")
		return sequence, fmt.Errorf("iterate tombstones: %w", err)
	}
	unknownRecords, err := UnknownExportRecordsContext(ctx, q, remote)
	if err != nil {
		return sequence, err
	}
	for _, unknownRecord := range unknownRecords {
		if knownTypes[unknownRecord.TypeName] {
			continue
		}
		if err := stageJSONLRecord(ctx, q, checkpoint, sequence, UnknownBatchTableName(unknownRecord.TypeName), unknownRecord.Record); err != nil {
			return sequence, err
		}
		sequence++
	}
	return sequence, nil
}

func writeStagedJSONL(ctx context.Context, q DBTX, checkpoint JSONLCheckpoint, writer io.Writer) (err error) {
	rows, err := q.QueryContext(ctx, `SELECT record_json FROM `+CoreTableExportEntryName+` WHERE batch_id = ? ORDER BY sequence`, checkpoint.BatchID)
	if err != nil {
		return fmt.Errorf("read staged export: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close staged export: %w", closeErr)
		}
	}()
	for rows.Next() {
		var line []byte
		if err := rows.Scan(&line); err != nil {
			return fmt.Errorf("scan staged export: %w", err)
		}
		writtenBytes, err := writer.Write(line)
		if err != nil {
			return fmt.Errorf("write staged export: %w", err)
		}
		if writtenBytes != len(line) {
			return io.ErrShortWrite
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate staged export: %w", err)
	}
	return nil
}

func WriteBoundJSONLContext(ctx context.Context, q DBTX, bindings []GeneratedTableBinding, remote string, writer io.Writer) error {
	checkpoint, err := PrepareBoundJSONLContext(ctx, q, bindings, remote, writer)
	if err != nil {
		return err
	}
	return AcknowledgeJSONLContext(ctx, q, checkpoint)
}

func DrainUnknownBindingsContext(ctx context.Context, q DBTX, bindings []GeneratedTableBinding) error {
	for _, binding := range bindings {
		if !binding.Descriptor.SyncEnabled {
			continue
		}
		rows, err := q.QueryContext(ctx, `SELECT id, at_ns, deleted, data_json FROM `+CoreTableUnknownName+` WHERE type_name = ? ORDER BY id`, binding.Descriptor.TypeName)
		if err != nil {
			return fmt.Errorf("select unknown rows for %s: %w", binding.Descriptor.TypeName, err)
		}
		defer closeRowsLog(rows, "unknown rows")
		records := make([]JSONLRecord, 0)
		for rows.Next() {
			var record JSONLRecord
			var deleted int
			var dataJSON string
			if err := rows.Scan(&record.ID, &record.AtNs, &deleted, &dataJSON); err != nil {
				closeRowsLog(rows, "unknown rows")
				return err
			}
			record.Deleted = deleted != 0
			record.Data = json.RawMessage(dataJSON)
			records = append(records, record)
		}
		for _, record := range records {
			if err := q.WithTransaction(ctx, func(tx DBTX) error {
				if err := ApplyIncomingObjectContext(ctx, tx, binding, record); err != nil {
					return err
				}
				if err := TransferUnknownSyncContext(ctx, tx, binding.Descriptor.TypeName, binding.Descriptor.TableName, record.ID); err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `DELETE FROM `+CoreTableUnknownName+` WHERE type_name = ? AND id = ?`, binding.Descriptor.TypeName, record.ID); err != nil {
					return fmt.Errorf("delete drained unknown row: %w", err)
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func closeRowsLog(rows *sql.Rows, operation string) {
	if err := rows.Close(); err != nil {
		slog.Error("close sqlite rows", "operation", operation, "error", err)
	}
}
