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
	CoreTableGenerationName      = "_proprdb_generations"
	CoreTableDirtyDefinitionName = "_proprdb_dirty_definitions"
	CoreTableDirtyWindowName     = "_proprdb_dirty_windows"
	mutationTriggerPrefix        = "_proprdb_mutation_"
	mutationFormat               = "v3|"
	mutationNew                  = "NEW"
	mutationOld                  = "OLD"
	mutationInsert               = "INSERT"
	mutationUpdate               = "UPDATE"
	mutationDelete               = "DELETE"
	mutationBeforeInsert         = "BEFORE_INSERT"
	mutationBeforeUpdate         = "BEFORE_UPDATE"
)

var ErrStaleDirtyWindow = errors.New("stale dirty window acknowledgment")

func quoteSQLiteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

type DirtyWindowDescriptor struct {
	Name        string
	StartColumn string
	EndColumn   string
}

type DirtyWindowToken struct {
	table        string
	window       string
	consumer     string
	registration string
	revision     int64
}

type DirtyWindowState struct {
	Dirty bool
	Full  bool
	Lower sql.NullInt64
	Upper sql.NullInt64
	Token DirtyWindowToken
}

func ensureMutationTables(ctx context.Context, q DBTX) (err error) {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS ` + CoreTableGenerationName + ` (table_name TEXT PRIMARY KEY, generation INTEGER NOT NULL CHECK(typeof(generation) = 'integer' AND generation >= 0), signature TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS ` + CoreTableDirtyDefinitionName + ` (table_name TEXT NOT NULL, window_name TEXT NOT NULL, start_column TEXT NOT NULL, end_column TEXT NOT NULL, PRIMARY KEY(table_name, window_name))`,
		`CREATE TABLE IF NOT EXISTS ` + CoreTableDirtyWindowName + ` (table_name TEXT NOT NULL, window_name TEXT NOT NULL, consumer TEXT NOT NULL, registration TEXT NOT NULL, revision INTEGER NOT NULL CHECK(typeof(revision) = 'integer' AND revision >= 0), dirty INTEGER NOT NULL, full INTEGER NOT NULL, lower_bound INTEGER, upper_bound INTEGER, PRIMARY KEY(table_name, window_name, consumer))`,
	}
	for _, statement := range statements {
		if _, err := q.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create mutation tracking tables: %w", err)
		}
	}
	return ensureDirtyKeysTables(ctx, q)
}

func mutationSignature(binding GeneratedTableBinding) string {
	windows := slices.Clone(binding.DirtyWindows)
	slices.SortFunc(windows, func(a, b DirtyWindowDescriptor) int { return strings.Compare(a.Name, b.Name) })
	parts := []string{mutationFormat + binding.ProjectionSchema}
	for _, window := range windows {
		parts = append(parts, window.Name+":"+window.StartColumn+":"+window.EndColumn)
	}
	return strings.Join(parts, "|") + dirtyKeysSignature(binding.DirtyKeys)
}

func mutationTriggerName(table, event string) string {
	return mutationTriggerPrefix + table + "_" + strings.ToLower(event)
}

func prepareMutationTracking(ctx context.Context, q DBTX, binding GeneratedTableBinding) (changed bool, err error) {
	var signature string
	err = q.QueryRowContext(ctx, `SELECT signature FROM `+CoreTableGenerationName+` WHERE table_name = ?`, binding.Descriptor.TableName).Scan(&signature)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("read mutation signature: %w", err)
	}
	changed = signature != mutationSignature(binding)
	if changed {
		if err := dropDirtyKeysTriggers(ctx, q, binding.Descriptor.TableName); err != nil {
			return false, err
		}
		for _, event := range []string{mutationInsert, mutationUpdate, mutationDelete, mutationBeforeInsert, mutationBeforeUpdate} {
			if _, err := q.ExecContext(ctx, `DROP TRIGGER IF EXISTS `+quoteSQLiteIdentifier(mutationTriggerName(binding.Descriptor.TableName, event))); err != nil {
				return false, fmt.Errorf("drop mutation trigger: %w", err)
			}
		}
	}
	return changed, nil
}

func finishMutationTracking(ctx context.Context, q DBTX, binding GeneratedTableBinding, changed bool) (err error) {
	table := binding.Descriptor.TableName
	if changed {
		slog.Debug("reconcile mutation tracking", "table", table, "window_count", len(binding.DirtyWindows))
		if _, err := q.ExecContext(ctx, `INSERT INTO `+CoreTableGenerationName+` (table_name, generation, signature) VALUES (?, 0, ?) ON CONFLICT(table_name) DO UPDATE SET generation = generation + 1, signature = excluded.signature`, table, mutationSignature(binding)); err != nil {
			return fmt.Errorf("initialize table generation: %w", err)
		}
		if _, err := q.ExecContext(ctx, `UPDATE `+CoreTableDirtyWindowName+` SET revision = revision + 1, dirty = 1, full = 1, lower_bound = NULL, upper_bound = NULL WHERE table_name = ?`, table); err != nil {
			return fmt.Errorf("invalidate dirty windows: %w", err)
		}
		if _, err := q.ExecContext(ctx, `DELETE FROM `+CoreTableDirtyDefinitionName+` WHERE table_name = ?`, table); err != nil {
			return fmt.Errorf("replace dirty definitions: %w", err)
		}
		for _, window := range binding.DirtyWindows {
			if _, err := q.ExecContext(ctx, `INSERT INTO `+CoreTableDirtyDefinitionName+` (table_name, window_name, start_column, end_column) VALUES (?, ?, ?, ?)`, table, window.Name, window.StartColumn, window.EndColumn); err != nil {
				return fmt.Errorf("write dirty definition: %w", err)
			}
		}
		if _, err := q.ExecContext(ctx, `DELETE FROM `+CoreTableDirtyWindowName+` WHERE table_name = ? AND window_name NOT IN (SELECT window_name FROM `+CoreTableDirtyDefinitionName+` WHERE table_name = ?)`, table, table); err != nil {
			return fmt.Errorf("remove stale dirty consumers: %w", err)
		}
	}
	for _, event := range []string{mutationInsert, mutationUpdate, mutationDelete, mutationBeforeInsert, mutationBeforeUpdate} {
		if (event == mutationBeforeInsert || event == mutationBeforeUpdate) && len(binding.DirtyWindows) == 0 {
			continue
		}
		if _, err := q.ExecContext(ctx, mutationTriggerSQL(binding, event)); err != nil {
			return fmt.Errorf("create mutation trigger for %s: %w", table, err)
		}
	}
	return reconcileDirtyKeys(ctx, q, binding, changed)
}

func mutationTriggerSQL(binding GeneratedTableBinding, event string) string {
	table := binding.Descriptor.TableName
	capturesExisting := event == mutationBeforeInsert || event == mutationBeforeUpdate
	condition := ""
	if event == mutationUpdate || capturesExisting {
		columns := []string{"id", "at_ns", dataColumnName}
		for _, column := range binding.ProjectedColumns {
			columns = append(columns, column.Name)
		}
		comparisons := make([]string, 0, len(columns))
		for _, column := range columns {
			quoted := quoteSQLiteIdentifier(column)
			prefix := "OLD."
			if capturesExisting {
				prefix = "existing."
			}
			comparisons = append(comparisons, prefix+quoted+" IS NOT NEW."+quoted)
		}
		condition = " WHEN " + strings.Join(comparisons, " OR ")
		if capturesExisting {
			condition = " WHEN EXISTS(SELECT 1 FROM " + quoteSQLiteIdentifier(table) + " AS existing WHERE existing.id = NEW.id AND (" + strings.Join(comparisons, " OR ") + "))"
		}
	}
	if event == mutationBeforeUpdate {
		condition = strings.Replace(condition, " WHEN ", " WHEN NEW.id IS NOT OLD.id AND ", 1)
	}
	statements := []string{`UPDATE ` + CoreTableGenerationName + ` SET generation = generation + 1 WHERE table_name = ` + quoteSQLiteString(table) + `;`}
	if capturesExisting {
		statements = nil
	}
	for _, window := range binding.DirtyWindows {
		prefixes := []string{mutationNew}
		if event == mutationDelete {
			prefixes = []string{mutationOld}
		}
		if event == mutationUpdate {
			prefixes = []string{mutationOld, mutationNew}
		}
		if capturesExisting {
			prefixes = []string{"EXISTING"}
		}
		for _, prefix := range prefixes {
			start := prefix + "." + quoteSQLiteIdentifier(window.StartColumn)
			end := prefix + "." + quoteSQLiteIdentifier(window.EndColumn)
			if capturesExisting {
				start = "(SELECT " + quoteSQLiteIdentifier(window.StartColumn) + " FROM " + quoteSQLiteIdentifier(table) + " WHERE id = NEW.id)"
				end = "(SELECT " + quoteSQLiteIdentifier(window.EndColumn) + " FROM " + quoteSQLiteIdentifier(table) + " WHERE id = NEW.id)"
			}
			invalid := "typeof(" + start + ") != 'integer' OR typeof(" + end + ") != 'integer' OR " + start + " > " + end
			statements = append(statements, `UPDATE `+CoreTableDirtyWindowName+` SET revision = revision + 1, dirty = 1, full = full OR (`+invalid+`), lower_bound = CASE WHEN `+invalid+` THEN lower_bound WHEN lower_bound IS NULL THEN `+start+` ELSE min(lower_bound, `+start+`) END, upper_bound = CASE WHEN `+invalid+` THEN upper_bound WHEN upper_bound IS NULL THEN `+end+` ELSE max(upper_bound, `+end+`) END WHERE table_name = `+quoteSQLiteString(table)+` AND window_name = `+quoteSQLiteString(window.Name)+`;`)
		}
	}
	timing := "AFTER " + event
	if event == mutationBeforeInsert {
		timing = "BEFORE INSERT"
	}
	if event == mutationBeforeUpdate {
		timing = "BEFORE UPDATE"
	}
	if len(statements) == 0 {
		statements = []string{"SELECT 1;"}
	}
	return `CREATE TRIGGER IF NOT EXISTS ` + quoteSQLiteIdentifier(mutationTriggerName(table, event)) + ` ` + timing + ` ON ` + quoteSQLiteIdentifier(table) + condition + ` BEGIN ` + strings.Join(statements, "") + ` END`
}

func TableGenerations(ctx context.Context, q DBTX, tables []string) (generations map[string]int64, err error) {
	if q == nil {
		return nil, errors.New(errNilDBTX)
	}
	generations = make(map[string]int64, len(tables))
	for _, table := range tables {
		var generation int64
		if err := q.QueryRowContext(ctx, `SELECT generation FROM `+CoreTableGenerationName+` WHERE table_name = ?`, table).Scan(&generation); err != nil {
			return nil, fmt.Errorf("read generation for %s: %w", table, err)
		}
		generations[table] = generation
	}
	return generations, nil
}

func RegisterDirtyWindow(ctx context.Context, q DBTX, table, window, consumer string) (err error) {
	if q == nil {
		return errors.New(errNilDBTX)
	}
	if table == "" || window == "" || consumer == "" {
		return errors.New("empty dirty window identity")
	}
	registration, err := UUIDv7()
	if err != nil {
		return fmt.Errorf("create dirty registration: %w", err)
	}
	return q.WithTransaction(ctx, func(tx DBTX) error {
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM `+CoreTableDirtyDefinitionName+` WHERE table_name = ? AND window_name = ?`, table, window).Scan(&exists); err != nil {
			return fmt.Errorf("lookup dirty window definition: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+CoreTableDirtyWindowName+` (table_name, window_name, consumer, registration, revision, dirty, full) VALUES (?, ?, ?, ?, 0, 1, 1) ON CONFLICT(table_name, window_name, consumer) DO NOTHING`, table, window, consumer, registration); err != nil {
			return fmt.Errorf("register dirty consumer: %w", err)
		}
		return nil
	})
}

func ReadDirtyWindow(ctx context.Context, q DBTX, table, window, consumer string) (state DirtyWindowState, err error) {
	if q == nil {
		return state, errors.New(errNilDBTX)
	}
	state.Token = DirtyWindowToken{table: table, window: window, consumer: consumer}
	err = q.QueryRowContext(ctx, `SELECT registration, revision, dirty, full, lower_bound, upper_bound FROM `+CoreTableDirtyWindowName+` WHERE table_name = ? AND window_name = ? AND consumer = ?`, table, window, consumer).Scan(&state.Token.registration, &state.Token.revision, &state.Dirty, &state.Full, &state.Lower, &state.Upper)
	if err != nil {
		return state, fmt.Errorf("read dirty window: %w", err)
	}
	return state, nil
}

func AcknowledgeDirtyWindow(ctx context.Context, q DBTX, token DirtyWindowToken) (err error) {
	if q == nil {
		return errors.New(errNilDBTX)
	}
	var revision int64
	err = q.QueryRowContext(ctx, `UPDATE `+CoreTableDirtyWindowName+` SET revision = revision + 1, dirty = 0, full = 0, lower_bound = NULL, upper_bound = NULL WHERE table_name = ? AND window_name = ? AND consumer = ? AND registration = ? AND revision = ? RETURNING revision`, token.table, token.window, token.consumer, token.registration, token.revision).Scan(&revision)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrStaleDirtyWindow
	}
	if err != nil {
		return fmt.Errorf("acknowledge dirty window: %w", err)
	}
	return nil
}

func RemoveDirtyWindowConsumer(ctx context.Context, q DBTX, table, window, consumer string) (err error) {
	if q == nil {
		return errors.New(errNilDBTX)
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM `+CoreTableDirtyWindowName+` WHERE table_name = ? AND window_name = ? AND consumer = ?`, table, window, consumer); err != nil {
		return fmt.Errorf("remove dirty consumer: %w", err)
	}
	return nil
}
