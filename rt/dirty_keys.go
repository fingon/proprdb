package proprdbrt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
)

const (
	CoreTableDirtyKeysDefinitionName = "_proprdb_dirty_key_definitions"
	CoreTableDirtyKeysConsumerName   = "_proprdb_dirty_key_consumers"
	CoreTableDirtyKeysEntryName      = "_proprdb_dirty_keys"
	dirtyKeysTriggerPrefix           = "_proprdb_dirty_keys_"
)

var ErrStaleDirtyKeys = errors.New("stale dirty keys acknowledgment")

type DirtyKeysDescriptor struct {
	Name           string
	WatchedColumns []string
}

type DirtyKeysToken struct {
	table        string
	tracker      string
	consumer     string
	registration string
	revision     int64
}

type DirtyKeysState struct {
	Dirty bool
	Full  bool
	IDs   []string
	Token DirtyKeysToken
}

func ensureDirtyKeysTables(ctx context.Context, q DBTX) (err error) {
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS ` + CoreTableDirtyKeysDefinitionName + ` (table_name TEXT NOT NULL, tracker_name TEXT NOT NULL, watched_columns TEXT NOT NULL, PRIMARY KEY(table_name, tracker_name))`,
		`CREATE TABLE IF NOT EXISTS ` + CoreTableDirtyKeysConsumerName + ` (table_name TEXT NOT NULL, tracker_name TEXT NOT NULL, consumer TEXT NOT NULL, registration TEXT NOT NULL, revision INTEGER NOT NULL CHECK(typeof(revision) = 'integer' AND revision >= 0), dirty INTEGER NOT NULL, full INTEGER NOT NULL, PRIMARY KEY(table_name, tracker_name, consumer))`,
		`CREATE TABLE IF NOT EXISTS ` + CoreTableDirtyKeysEntryName + ` (table_name TEXT NOT NULL, tracker_name TEXT NOT NULL, consumer TEXT NOT NULL, id TEXT NOT NULL, PRIMARY KEY(table_name, tracker_name, consumer, id))`,
	} {
		if _, err := q.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create dirty keys tables: %w", err)
		}
	}
	return nil
}

func dirtyKeysSignature(trackers []DirtyKeysDescriptor) string {
	trackers = slices.Clone(trackers)
	slices.SortFunc(trackers, func(a, b DirtyKeysDescriptor) int { return strings.Compare(a.Name, b.Name) })
	var parts []string
	for _, tracker := range trackers {
		columns := slices.Clone(tracker.WatchedColumns)
		slices.Sort(columns)
		parts = append(parts, "keys:"+tracker.Name+":"+strings.Join(columns, ","))
	}
	if len(parts) == 0 {
		return ""
	}
	return "|" + strings.Join(parts, "|")
}

func dropDirtyKeysTriggers(ctx context.Context, q DBTX, table string) (err error) {
	for _, event := range []string{mutationInsert, mutationUpdate, mutationDelete} {
		if _, err := q.ExecContext(ctx, `DROP TRIGGER IF EXISTS `+quoteSQLiteIdentifier(dirtyKeysTriggerPrefix+table+"_"+strings.ToLower(event))); err != nil {
			return fmt.Errorf("drop dirty keys trigger: %w", err)
		}
	}
	return nil
}

func reconcileDirtyKeys(ctx context.Context, q DBTX, binding GeneratedTableBinding, changed bool) (err error) {
	table := binding.Descriptor.TableName
	if changed {
		if _, err := q.ExecContext(ctx, `UPDATE `+CoreTableDirtyKeysConsumerName+` SET revision = revision + 1, dirty = 1, full = 1 WHERE table_name = ?`, table); err != nil {
			return fmt.Errorf("invalidate dirty keys: %w", err)
		}
		if _, err := q.ExecContext(ctx, `DELETE FROM `+CoreTableDirtyKeysDefinitionName+` WHERE table_name = ?`, table); err != nil {
			return fmt.Errorf("replace dirty keys definitions: %w", err)
		}
		for _, tracker := range binding.DirtyKeys {
			columns := slices.Clone(tracker.WatchedColumns)
			slices.Sort(columns)
			if _, err := q.ExecContext(ctx, `INSERT INTO `+CoreTableDirtyKeysDefinitionName+` (table_name, tracker_name, watched_columns) VALUES (?, ?, ?)`, table, tracker.Name, strings.Join(columns, ",")); err != nil {
				return fmt.Errorf("write dirty keys definition: %w", err)
			}
		}
		for _, target := range []string{CoreTableDirtyKeysEntryName, CoreTableDirtyKeysConsumerName} {
			if _, err := q.ExecContext(ctx, `DELETE FROM `+target+` WHERE table_name = ? AND tracker_name NOT IN (SELECT tracker_name FROM `+CoreTableDirtyKeysDefinitionName+` WHERE table_name = ?)`, table, table); err != nil {
				return fmt.Errorf("remove stale dirty keys consumers: %w", err)
			}
		}
	}
	if len(binding.DirtyKeys) == 0 {
		return nil
	}
	for _, event := range []string{mutationInsert, mutationUpdate, mutationDelete} {
		if _, err := q.ExecContext(ctx, dirtyKeysTriggerSQL(binding, event)); err != nil {
			return fmt.Errorf("create dirty keys trigger: %w", err)
		}
	}
	return nil
}

func dirtyKeysTriggerSQL(binding GeneratedTableBinding, event string) string {
	table := binding.Descriptor.TableName
	var statements []string
	for _, tracker := range binding.DirtyKeys {
		predicate := "table_name = " + quoteSQLiteString(table) + " AND tracker_name = " + quoteSQLiteString(tracker.Name)
		if event == mutationUpdate {
			columns := append([]string{"id"}, tracker.WatchedColumns...)
			if len(tracker.WatchedColumns) == 0 {
				columns = append(columns, "at_ns", dataColumnName)
				for _, column := range binding.ProjectedColumns {
					columns = append(columns, column.Name)
				}
			}
			comparisons := make([]string, 0, len(columns))
			for _, column := range columns {
				quoted := quoteSQLiteIdentifier(column)
				comparisons = append(comparisons, "OLD."+quoted+" IS NOT NEW."+quoted)
			}
			predicate += " AND (" + strings.Join(comparisons, " OR ") + ")"
		}
		statements = append(statements, `UPDATE `+CoreTableDirtyKeysConsumerName+` SET revision = revision + 1, dirty = 1 WHERE `+predicate+`;`)
		prefixes := []string{mutationNew}
		if event == mutationDelete {
			prefixes = []string{mutationOld}
		}
		if event == mutationUpdate {
			prefixes = []string{mutationOld, mutationNew}
		}
		for _, prefix := range prefixes {
			statements = append(statements, `INSERT INTO `+CoreTableDirtyKeysEntryName+` (table_name, tracker_name, consumer, id) SELECT table_name, tracker_name, consumer, `+prefix+`.id FROM `+CoreTableDirtyKeysConsumerName+` WHERE `+predicate+` ON CONFLICT(table_name, tracker_name, consumer, id) DO NOTHING;`)
		}
	}
	return `CREATE TRIGGER IF NOT EXISTS ` + quoteSQLiteIdentifier(dirtyKeysTriggerPrefix+table+"_"+strings.ToLower(event)) + ` AFTER ` + event + ` ON ` + quoteSQLiteIdentifier(table) + ` BEGIN ` + strings.Join(statements, "") + ` END`
}

func RegisterDirtyKeys(ctx context.Context, q DBTX, table, tracker, consumer string) (err error) {
	if q == nil {
		return errors.New(errNilDBTX)
	}
	if table == "" || tracker == "" || consumer == "" {
		return errors.New("empty dirty keys identity")
	}
	registration, err := UUIDv7()
	if err != nil {
		return fmt.Errorf("create dirty keys registration: %w", err)
	}
	return q.WithTransaction(ctx, func(tx DBTX) error {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM `+CoreTableDirtyKeysDefinitionName+` WHERE table_name = ? AND tracker_name = ?`, table, tracker).Scan(&exists); err != nil {
			return fmt.Errorf("lookup dirty keys definition: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+CoreTableDirtyKeysConsumerName+` (table_name, tracker_name, consumer, registration, revision, dirty, full) VALUES (?, ?, ?, ?, 0, 1, 1) ON CONFLICT(table_name, tracker_name, consumer) DO NOTHING`, table, tracker, consumer, registration); err != nil {
			return fmt.Errorf("register dirty keys consumer: %w", err)
		}
		return nil
	})
}

func ReadDirtyKeys(ctx context.Context, q DBTX, table, tracker, consumer string) (state DirtyKeysState, err error) {
	if q == nil {
		return state, errors.New(errNilDBTX)
	}
	slog.Debug("read dirty keys", "table", table, "tracker", tracker, "consumer", consumer)
	err = q.WithTransaction(ctx, func(tx DBTX) (err error) {
		state.Token = DirtyKeysToken{table: table, tracker: tracker, consumer: consumer}
		if err := tx.QueryRowContext(ctx, `SELECT registration, revision, dirty, full FROM `+CoreTableDirtyKeysConsumerName+` WHERE table_name = ? AND tracker_name = ? AND consumer = ?`, table, tracker, consumer).Scan(&state.Token.registration, &state.Token.revision, &state.Dirty, &state.Full); err != nil {
			return fmt.Errorf("read dirty keys consumer: %w", err)
		}
		rows, err := tx.QueryContext(ctx, `SELECT id FROM `+CoreTableDirtyKeysEntryName+` WHERE table_name = ? AND tracker_name = ? AND consumer = ? ORDER BY id`, table, tracker, consumer)
		if err != nil {
			return fmt.Errorf("read dirty keys: %w", err)
		}
		defer func() {
			if closeErr := CloseRows(rows, "dirty keys"); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}()
		state.IDs = make([]string, 0)
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return fmt.Errorf("scan dirty key: %w", err)
			}
			state.IDs = append(state.IDs, id)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate dirty keys: %w", err)
		}
		return nil
	})
	if err != nil {
		return DirtyKeysState{}, err
	}
	return state, nil
}

func AcknowledgeDirtyKeys(ctx context.Context, q DBTX, token DirtyKeysToken) (err error) {
	if q == nil {
		return errors.New(errNilDBTX)
	}
	return q.WithTransaction(ctx, func(tx DBTX) error {
		var revision int64
		err := tx.QueryRowContext(ctx, `UPDATE `+CoreTableDirtyKeysConsumerName+` SET revision = revision + 1, dirty = 0, full = 0 WHERE table_name = ? AND tracker_name = ? AND consumer = ? AND registration = ? AND revision = ? RETURNING revision`, token.table, token.tracker, token.consumer, token.registration, token.revision).Scan(&revision)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrStaleDirtyKeys
		}
		if err != nil {
			return fmt.Errorf("acknowledge dirty keys: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+CoreTableDirtyKeysEntryName+` WHERE table_name = ? AND tracker_name = ? AND consumer = ?`, token.table, token.tracker, token.consumer); err != nil {
			return fmt.Errorf("clear dirty keys: %w", err)
		}
		return nil
	})
}

func RemoveDirtyKeysConsumer(ctx context.Context, q DBTX, table, tracker, consumer string) (err error) {
	if q == nil {
		return errors.New(errNilDBTX)
	}
	return q.WithTransaction(ctx, func(tx DBTX) error {
		for _, target := range []string{CoreTableDirtyKeysEntryName, CoreTableDirtyKeysConsumerName} {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+target+` WHERE table_name = ? AND tracker_name = ? AND consumer = ?`, table, tracker, consumer); err != nil {
				return fmt.Errorf("remove dirty keys consumer: %w", err)
			}
		}
		return nil
	})
}
