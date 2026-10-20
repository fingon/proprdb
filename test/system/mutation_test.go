package genexample

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"gotest.tools/v3/assert"
)

func TestMutationSyncAcceptedOlderTimestamps(t *testing.T) {
	_, q, crud := mutationDatabase(t, ":memory:")
	registerAgeWindow(t, q, mutationConsumer)
	input, err := os.ReadFile("testdata/mutation-import.jsonl")
	assert.NilError(t, err)
	assert.NilError(t, crud.ReadJSONL("peer", bytes.NewReader(input)))
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(2))
	state := readAgeWindow(t, q, mutationConsumer)
	assert.Equal(t, state.Lower.Int64, int64(10))
	assert.Equal(t, state.Upper.Int64, int64(30))
	assert.NilError(t, rt.AcknowledgeDirtyWindow(context.Background(), q, state.Token))
	assert.NilError(t, crud.ReadJSONL("peer", bytes.NewReader(input)))
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(2))
	assert.Assert(t, !readAgeWindow(t, q, mutationConsumer).Dirty)
}

func TestMutationUnknownDrain(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, ":memory:")
	registerAgeWindow(t, q, mutationConsumer)
	input, err := os.ReadFile("testdata/mutation-import.jsonl")
	assert.NilError(t, err)
	assert.NilError(t, rt.ReadBoundJSONLContext(ctx, q, nil, "peer", bytes.NewReader(input)))
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(0))
	assert.NilError(t, crud.Init())
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(2))
	state := readAgeWindow(t, q, mutationConsumer)
	assert.Equal(t, state.Lower.Int64, int64(10))
	assert.Equal(t, state.Upper.Int64, int64(30))
}

const (
	mutationConsumer     = "refresh"
	ageWindow            = "age"
	captureWindow        = "capture"
	mutationPersonID     = "01951d6e-a000-7000-8000-000000000001"
	mutationUpdateAgeSQL = `UPDATE "` + PersonTableName + `" SET age = ? WHERE id = ?`
)

func mutationDatabase(t *testing.T, path string) (*sql.DB, rt.DBTX, *CRUD) {
	t.Helper()
	db, err := sql.Open(sqliteDriverName, path)
	assert.NilError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	q := rt.WrapDB(db)
	crud := NewCRUD(q)
	assert.NilError(t, crud.Init())
	return db, q, crud
}

func mutationGeneration(t *testing.T, q rt.DBTX, table string) int64 {
	t.Helper()
	generations, err := rt.TableGenerations(context.Background(), q, []string{table})
	assert.NilError(t, err)
	return generations[table]
}

func readAgeWindow(t *testing.T, q rt.DBTX, consumer string) rt.DirtyWindowState {
	t.Helper()
	state, err := rt.ReadDirtyWindow(context.Background(), q, PersonTableName, ageWindow, consumer)
	assert.NilError(t, err)
	return state
}

func registerAgeWindow(t *testing.T, q rt.DBTX, consumer string) {
	t.Helper()
	ctx := context.Background()
	assert.NilError(t, rt.RegisterDirtyWindow(ctx, q, PersonTableName, ageWindow, consumer))
	state := readAgeWindow(t, q, consumer)
	assert.Assert(t, state.Dirty && state.Full)
	assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
}

func TestMutationGenerations(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		table  string
		mutate func(*testing.T, *sql.DB, *CRUD)
	}{
		{"insert", PersonTableName, func(t *testing.T, _ *sql.DB, crud *CRUD) {
			_, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
			assert.NilError(t, err)
		}},
		{"direct insert", PersonTableName, func(t *testing.T, db *sql.DB, _ *CRUD) {
			_, err := db.ExecContext(ctx, `INSERT INTO "`+PersonTableName+`" (id, at_ns, data, name, age) VALUES (?, 1, X'', ?, 10)`, mutationPersonID, testPersonNameAda)
			assert.NilError(t, err)
		}},
		{"omit sync", NoteTableName, func(t *testing.T, _ *sql.DB, crud *CRUD) {
			_, err := crud.Note.Insert(&Note{Text: "local"})
			assert.NilError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, q, crud := mutationDatabase(t, ":memory:")
			assert.Equal(t, mutationGeneration(t, q, tc.table), int64(0))
			tc.mutate(t, db, crud)
			assert.Equal(t, mutationGeneration(t, q, tc.table), int64(1))
			assert.NilError(t, crud.Init())
			assert.Equal(t, mutationGeneration(t, q, tc.table), int64(1))
		})
	}
}

func TestDirtyWindowDirectSQLReplacement(t *testing.T) {
	ctx := context.Background()
	for _, recursive := range []bool{false, true} {
		t.Run(fmt.Sprintf("recursive=%t", recursive), func(t *testing.T) {
			db, q, crud := mutationDatabase(t, ":memory:")
			_, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA recursive_triggers = %t", recursive))
			assert.NilError(t, err)
			registerAgeWindow(t, q, mutationConsumer)
			row, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
			assert.NilError(t, err)
			state := readAgeWindow(t, q, mutationConsumer)
			assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
			_, err = db.ExecContext(ctx, `INSERT OR REPLACE INTO "`+PersonTableName+`" (id, at_ns, data, name, age) VALUES (?, 2, X'', ?, 100)`, row.ID, testPersonNameAda)
			assert.NilError(t, err)
			state = readAgeWindow(t, q, mutationConsumer)
			assert.Equal(t, state.Lower.Int64, int64(10))
			assert.Equal(t, state.Upper.Int64, int64(100))
			assert.Assert(t, !state.Full)
			assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
			generation := mutationGeneration(t, q, PersonTableName)
			_, err = db.ExecContext(ctx, `INSERT OR IGNORE INTO "`+PersonTableName+`" (id, at_ns, data, name, age) VALUES (?, 3, X'', ?, 200)`, row.ID, testPersonNameAda)
			assert.NilError(t, err)
			assert.Equal(t, mutationGeneration(t, q, PersonTableName), generation)
			state = readAgeWindow(t, q, mutationConsumer)
			assert.Equal(t, state.Lower.Int64, int64(100))
			assert.Equal(t, state.Upper.Int64, int64(100))
			assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
			target, err := crud.Person.Insert(&Person{Name: testPersonNameGrace, Age: 1000})
			assert.NilError(t, err)
			state = readAgeWindow(t, q, mutationConsumer)
			assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
			_, err = db.ExecContext(ctx, `UPDATE OR REPLACE "`+PersonTableName+`" SET id = ?, age = 200 WHERE id = ?`, target.ID, row.ID)
			assert.NilError(t, err)
			state = readAgeWindow(t, q, mutationConsumer)
			assert.Equal(t, state.Lower.Int64, int64(100))
			assert.Equal(t, state.Upper.Int64, int64(1000))
		})
	}
}

func TestDirtyWindowAccumulationAndConsumers(t *testing.T) {
	ctx := context.Background()
	db, q, crud := mutationDatabase(t, ":memory:")
	registerAgeWindow(t, q, mutationConsumer)
	registerAgeWindow(t, q, "other")
	row, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
	assert.NilError(t, err)
	initial := readAgeWindow(t, q, mutationConsumer)
	assert.Equal(t, initial.Lower.Int64, int64(10))
	assert.Equal(t, initial.Upper.Int64, int64(10))
	assert.Assert(t, initial.Dirty && !initial.Full && initial.Lower.Valid && initial.Upper.Valid)
	for _, age := range []int64{30, -5, 20} {
		_, err := db.ExecContext(ctx, mutationUpdateAgeSQL, age, row.ID)
		assert.NilError(t, err)
	}
	state := readAgeWindow(t, q, mutationConsumer)
	assert.Equal(t, state.Lower.Int64, int64(-5))
	assert.Equal(t, state.Upper.Int64, int64(30))
	assert.ErrorIs(t, rt.AcknowledgeDirtyWindow(ctx, q, initial.Token), rt.ErrStaleDirtyWindow)
	assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
	assert.Assert(t, !readAgeWindow(t, q, mutationConsumer).Dirty)
	assert.Assert(t, readAgeWindow(t, q, "other").Dirty)
	assert.ErrorIs(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token), rt.ErrStaleDirtyWindow)
	_, err = crud.Person.UpdateByID(row.ID, &Person{Name: testPersonNameGrace, Age: 20})
	assert.NilError(t, err)
	state = readAgeWindow(t, q, mutationConsumer)
	assert.Assert(t, state.Dirty)
	assert.Equal(t, state.Lower.Int64, int64(20))
	assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
	generation := mutationGeneration(t, q, PersonTableName)
	_, err = db.ExecContext(ctx, mutationUpdateAgeSQL, int64(20), row.ID)
	assert.NilError(t, err)
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), generation)
	assert.Assert(t, !readAgeWindow(t, q, mutationConsumer).Dirty)
	assert.NilError(t, crud.Person.DeleteByID(row.ID))
	state = readAgeWindow(t, q, mutationConsumer)
	assert.Equal(t, state.Lower.Int64, int64(20))
	assert.Assert(t, state.Dirty)
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), generation+1)
	assert.NilError(t, rt.RemoveDirtyWindowConsumer(ctx, q, PersonTableName, ageWindow, mutationConsumer))
	assert.NilError(t, rt.RegisterDirtyWindow(ctx, q, PersonTableName, ageWindow, mutationConsumer))
	assert.ErrorIs(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token), rt.ErrStaleDirtyWindow)
	var count int
	assert.NilError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM `+rt.CoreTableDirtyWindowName+` WHERE table_name = ?`, PersonTableName).Scan(&count))
	assert.Equal(t, count, 2)
}

func TestMutationRollbackAndOutputAcknowledgment(t *testing.T) {
	ctx := context.Background()
	db, q, crud := mutationDatabase(t, ":memory:")
	registerAgeWindow(t, q, mutationConsumer)
	row, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
	assert.NilError(t, err)
	generation := mutationGeneration(t, q, PersonTableName)
	state := readAgeWindow(t, q, mutationConsumer)
	failure := errors.New("rollback refresh")
	err = q.WithTransaction(ctx, func(tx rt.DBTX) error {
		if _, err := NewNoteTable(tx).Insert(&Note{Text: "derived"}); err != nil {
			return err
		}
		if err := rt.AcknowledgeDirtyWindow(ctx, tx, state.Token); err != nil {
			return err
		}
		return failure
	})
	assert.ErrorIs(t, err, failure)
	assert.Assert(t, readAgeWindow(t, q, mutationConsumer).Dirty)
	assert.Equal(t, mutationGeneration(t, q, NoteTableName), int64(0))
	err = q.WithTransaction(ctx, func(tx rt.DBTX) error {
		err := tx.WithTransaction(ctx, func(nested rt.DBTX) error {
			if _, err := nested.ExecContext(ctx, mutationUpdateAgeSQL, int64(100), row.ID); err != nil {
				return err
			}
			return failure
		})
		if !errors.Is(err, failure) {
			return fmt.Errorf("unexpected savepoint error: %w", err)
		}
		return rt.AcknowledgeDirtyWindow(ctx, tx, state.Token)
	})
	assert.NilError(t, err)
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), generation)
	assert.Assert(t, !readAgeWindow(t, q, mutationConsumer).Dirty)
	_, err = db.ExecContext(ctx, `UPDATE `+rt.CoreTableGenerationName+` SET generation = ? WHERE table_name = ?`, int64(9223372036854775807), PersonTableName)
	assert.NilError(t, err)
	_, err = db.ExecContext(ctx, mutationUpdateAgeSQL, int64(100), row.ID)
	assert.Assert(t, err != nil)
	rows, err := crud.Person.Select("id = ?", row.ID)
	assert.NilError(t, err)
	assert.Equal(t, rows[0].Data.Age, int64(10))
}

func TestDirtyWindowMissingAndReversedBounds(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		start, end any
		full       bool
	}{
		{"known bounds", int64(10), int64(20), false},
		{"missing", nil, int64(20), true},
		{"reversed", int64(30), int64(20), true},
		{"wrong type", "unknown", int64(20), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, q, crud := mutationDatabase(t, ":memory:")
			assert.NilError(t, rt.RegisterDirtyWindow(ctx, q, PhotoTableName, captureWindow, mutationConsumer))
			state, err := rt.ReadDirtyWindow(ctx, q, PhotoTableName, captureWindow, mutationConsumer)
			assert.NilError(t, err)
			assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
			row, err := crud.Photo.Insert(&Photo{})
			assert.NilError(t, err)
			state, err = rt.ReadDirtyWindow(ctx, q, PhotoTableName, captureWindow, mutationConsumer)
			assert.NilError(t, err)
			assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
			_, err = db.ExecContext(ctx, `UPDATE "`+PhotoTableName+`" SET exif_create_utc_time_seconds = ?, exif_modify_utc_time_seconds = ? WHERE id = ?`, tc.start, tc.end, row.ID)
			assert.NilError(t, err)
			state, err = rt.ReadDirtyWindow(ctx, q, PhotoTableName, captureWindow, mutationConsumer)
			assert.NilError(t, err)
			// The old row has unknown bounds, so moving from it needs a full refresh.
			assert.Assert(t, state.Full)
			assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
			_, err = db.ExecContext(ctx, `UPDATE "`+PhotoTableName+`" SET at_ns = at_ns + 1 WHERE id = ?`, row.ID)
			assert.NilError(t, err)
			state, err = rt.ReadDirtyWindow(ctx, q, PhotoTableName, captureWindow, mutationConsumer)
			assert.NilError(t, err)
			assert.Equal(t, state.Full, tc.full)
		})
	}
}

func TestMutationReopenAndConcurrentWriters(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "mutation.db") + "?_busy_timeout=10000&_journal_mode=WAL"
	_, first, crud := mutationDatabase(t, path)
	_, second, _ := mutationDatabase(t, path)
	registerAgeWindow(t, first, mutationConsumer)
	row, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
	assert.NilError(t, err)
	stale := readAgeWindow(t, first, mutationConsumer)
	assert.NilError(t, first.WithTransaction(ctx, func(tx rt.DBTX) error {
		before := mutationGeneration(t, tx, PersonTableName)
		if _, err := second.ExecContext(ctx, mutationUpdateAgeSQL, int64(20), row.ID); err != nil {
			return err
		}
		assert.Equal(t, mutationGeneration(t, tx, PersonTableName), before)
		var age int64
		if err := tx.QueryRowContext(ctx, `SELECT age FROM "`+PersonTableName+`" WHERE id = ?`, row.ID).Scan(&age); err != nil {
			return err
		}
		assert.Equal(t, age, int64(10))
		return nil
	}))
	assert.ErrorIs(t, rt.AcknowledgeDirtyWindow(ctx, first, stale.Token), rt.ErrStaleDirtyWindow)
	const writers = 2
	const writes = 12
	var group sync.WaitGroup
	errorsCh := make(chan error, writers)
	for index, q := range []rt.DBTX{first, second} {
		group.Go(func() {
			for offset := range writes {
				if _, err := q.ExecContext(ctx, mutationUpdateAgeSQL, int64(100+index*writes+offset), row.ID); err != nil {
					errorsCh <- err
					return
				}
			}
		})
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		assert.NilError(t, err)
	}
	assert.Equal(t, mutationGeneration(t, first, PersonTableName), int64(2+writers*writes))
	_, reopened, _ := mutationDatabase(t, path)
	assert.Equal(t, mutationGeneration(t, reopened, PersonTableName), int64(2+writers*writes))
	state := readAgeWindow(t, reopened, mutationConsumer)
	assert.Equal(t, state.Lower.Int64, int64(10))
	assert.Equal(t, state.Upper.Int64, int64(123))
}

func TestMutationSchemaInvalidation(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, ":memory:")
	registerAgeWindow(t, q, mutationConsumer)
	_, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
	assert.NilError(t, err)
	before := mutationGeneration(t, q, PersonTableName)
	binding := PersonGeneratedBinding
	binding.DirtyWindows = append(binding.DirtyWindows, rt.DirtyWindowDescriptor{Name: "extra", StartColumn: "age", EndColumn: "age"})
	assert.NilError(t, rt.ReconcileGeneratedTableContext(ctx, q, binding))
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), before+1)
	state := readAgeWindow(t, q, mutationConsumer)
	assert.Assert(t, state.Full)
	assert.NilError(t, rt.RegisterDirtyWindow(ctx, q, PersonTableName, "extra", mutationConsumer))
	assert.NilError(t, rt.ReconcileGeneratedTableContext(ctx, q, PersonGeneratedBinding))
	_, err = rt.ReadDirtyWindow(ctx, q, PersonTableName, "extra", mutationConsumer)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}
