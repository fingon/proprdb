package genexample

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"gotest.tools/v3/assert"
)

const (
	keysMemoryPath    = ":memory:"
	keysAll           = "all"
	keysAge           = "age"
	keysOtherConsumer = "other"
	keysSecondID      = "01951d6e-a000-7000-8000-000000000002"
	keysSetNameSQL    = `UPDATE "` + PersonTableName + `" SET name = ? WHERE id = ?`
)

func keyState(t *testing.T, q rt.DBTX, tracker, consumer string) rt.DirtyKeysState {
	t.Helper()
	state, err := rt.ReadDirtyKeys(context.Background(), q, PersonTableName, tracker, consumer)
	assert.NilError(t, err)
	return state
}

func registerKeys(t *testing.T, q rt.DBTX, tracker, consumer string) {
	t.Helper()
	ctx := context.Background()
	assert.NilError(t, rt.RegisterDirtyKeys(ctx, q, PersonTableName, tracker, consumer))
	state := keyState(t, q, tracker, consumer)
	assert.Assert(t, state.Dirty && state.Full)
	assert.Equal(t, len(state.IDs), 0)
	assert.NilError(t, rt.RegisterDirtyKeys(ctx, q, PersonTableName, tracker, consumer))
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token))
}

func TestDirtyKeysConsumersAndRepeatedMutations(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, keysMemoryPath)
	for _, tracker := range []string{keysAll, keysAge} {
		registerKeys(t, q, tracker, mutationConsumer)
	}
	registerKeys(t, q, keysAge, keysOtherConsumer)
	row, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
	assert.NilError(t, err)
	first := keyState(t, q, keysAge, mutationConsumer)
	assert.DeepEqual(t, first.IDs, []string{row.ID})
	assert.Assert(t, first.Dirty && !first.Full)
	for _, age := range []int64{20, 30, 10} {
		_, err := q.ExecContext(ctx, mutationUpdateAgeSQL, age, row.ID)
		assert.NilError(t, err)
	}
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, q, first.Token), rt.ErrStaleDirtyKeys)
	state := keyState(t, q, keysAge, mutationConsumer)
	assert.DeepEqual(t, state.IDs, []string{row.ID})
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token))
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token), rt.ErrStaleDirtyKeys)
	assert.Assert(t, keyState(t, q, keysAge, keysOtherConsumer).Dirty)
	assert.Assert(t, keyState(t, q, keysAll, mutationConsumer).Dirty)
	clean := keyState(t, q, keysAge, mutationConsumer)
	assert.Assert(t, !clean.Dirty && !clean.Full && len(clean.IDs) == 0)
	_, err = q.ExecContext(ctx, keysSetNameSQL, testPersonNameGrace, row.ID)
	assert.NilError(t, err)
	_, err = q.ExecContext(ctx, mutationUpdateAgeSQL, int64(10), row.ID)
	assert.NilError(t, err)
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, clean.Token))
	assert.NilError(t, crud.Person.DeleteByID(row.ID))
	deleted := keyState(t, q, keysAge, mutationConsumer)
	assert.DeepEqual(t, deleted.IDs, []string{row.ID})
	_, err = crud.Person.InsertWithID(row.ID, &Person{Name: testPersonNameAda, Age: 10})
	assert.NilError(t, err)
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, q, deleted.Token), rt.ErrStaleDirtyKeys)
	assert.DeepEqual(t, keyState(t, q, keysAge, mutationConsumer).IDs, []string{row.ID})
	assert.NilError(t, rt.RemoveDirtyKeysConsumer(ctx, q, PersonTableName, keysAge, mutationConsumer))
	assert.NilError(t, rt.RegisterDirtyKeys(ctx, q, PersonTableName, keysAge, mutationConsumer))
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, q, deleted.Token), rt.ErrStaleDirtyKeys)
	assert.Assert(t, keyState(t, q, keysAge, mutationConsumer).Full)
}

func TestDirtyKeysReplacementAndIDChanges(t *testing.T) {
	ctx := context.Background()
	for _, recursive := range []bool{false, true} {
		for _, tc := range []struct {
			name, query string
			ids         []string
		}{
			{"insert replacement", `INSERT OR REPLACE INTO "` + PersonTableName + `" (id, at_ns, data, name, age) SELECT id, at_ns, data, name, age FROM "` + PersonTableName + `" WHERE id = ?`, []string{mutationPersonID}},
			{"update replacement", `UPDATE OR REPLACE "` + PersonTableName + `" SET id = '` + keysSecondID + `' WHERE id = ?`, []string{mutationPersonID, keysSecondID}},
			{"change ID", `UPDATE "` + PersonTableName + `" SET id = '01951d6e-a000-7000-8000-000000000003' WHERE id = ?`, []string{mutationPersonID, "01951d6e-a000-7000-8000-000000000003"}},
		} {
			t.Run(tc.name+map[bool]string{false: "/no-recursion", true: "/recursive"}[recursive], func(t *testing.T) {
				_, q, crud := mutationDatabase(t, keysMemoryPath)
				_, err := q.ExecContext(ctx, "PRAGMA recursive_triggers = "+map[bool]string{true: "ON", false: "OFF"}[recursive])
				assert.NilError(t, err)
				for _, id := range []string{mutationPersonID, keysSecondID} {
					_, err := crud.Person.InsertWithID(id, &Person{Name: testPersonNameAda, Age: 10})
					assert.NilError(t, err)
				}
				registerKeys(t, q, keysAge, mutationConsumer)
				_, err = q.ExecContext(ctx, tc.query, mutationPersonID)
				assert.NilError(t, err)
				state := keyState(t, q, keysAge, mutationConsumer)
				assert.DeepEqual(t, state.IDs, tc.ids)
				assert.Assert(t, state.Dirty && !state.Full)
			})
		}
	}
}

func TestDirtyKeysRollbackAndOverflow(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, keysMemoryPath)
	registerKeys(t, q, keysAge, mutationConsumer)
	row, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
	assert.NilError(t, err)
	state := keyState(t, q, keysAge, mutationConsumer)
	rollback := errors.New("rollback refresh")
	assert.ErrorIs(t, q.WithTransaction(ctx, func(tx rt.DBTX) error {
		assert.NilError(t, rt.RegisterDirtyKeys(ctx, tx, PersonTableName, keysAge, keysOtherConsumer))
		assert.NilError(t, tx.WithTransaction(ctx, func(inner rt.DBTX) error {
			return rt.AcknowledgeDirtyKeys(ctx, inner, state.Token)
		}))
		_, err := NewCRUD(tx).Note.Insert(&Note{Text: "derived"})
		assert.NilError(t, err)
		return rollback
	}), rollback)
	assert.DeepEqual(t, keyState(t, q, keysAge, mutationConsumer).IDs, []string{row.ID})
	_, err = rt.ReadDirtyKeys(ctx, q, PersonTableName, keysAge, keysOtherConsumer)
	assert.ErrorIs(t, err, sql.ErrNoRows)
	assert.Equal(t, mutationGeneration(t, q, NoteTableName), int64(0))
	assert.ErrorIs(t, q.WithTransaction(ctx, func(tx rt.DBTX) error {
		assert.ErrorIs(t, tx.WithTransaction(ctx, func(inner rt.DBTX) error {
			_, err := inner.ExecContext(ctx, mutationUpdateAgeSQL, int64(20), row.ID)
			assert.NilError(t, err)
			return rollback
		}), rollback)
		return rollback
	}), rollback)
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token))
	_, err = q.ExecContext(ctx, `UPDATE `+rt.CoreTableDirtyKeysConsumerName+` SET revision = ? WHERE table_name = ? AND tracker_name = ? AND consumer = ?`, int64(1<<63-1), PersonTableName, keysAge, mutationConsumer)
	assert.NilError(t, err)
	_, err = q.ExecContext(ctx, mutationUpdateAgeSQL, int64(20), row.ID)
	assert.Assert(t, err != nil)
	var age int64
	assert.NilError(t, q.QueryRowContext(ctx, `SELECT age FROM "`+PersonTableName+`" WHERE id = ?`, row.ID).Scan(&age))
	assert.Equal(t, age, int64(10))
}

func TestDirtyKeysSyncAndUnknownDrain(t *testing.T) {
	ctx := context.Background()
	for _, drain := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "drain"}[drain], func(t *testing.T) {
			_, q, crud := mutationDatabase(t, keysMemoryPath)
			registerKeys(t, q, keysAge, mutationConsumer)
			input, err := os.ReadFile("testdata/mutation-import.jsonl")
			assert.NilError(t, err)
			if drain {
				assert.NilError(t, rt.ReadBoundJSONLContext(ctx, q, nil, "peer", bytes.NewReader(input)))
				assert.Assert(t, !keyState(t, q, keysAge, mutationConsumer).Dirty)
				assert.NilError(t, crud.Init())
			} else {
				assert.NilError(t, crud.ReadJSONL("peer", bytes.NewReader(input)))
			}
			state := keyState(t, q, keysAge, mutationConsumer)
			assert.DeepEqual(t, state.IDs, []string{mutationPersonID, keysSecondID})
			assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token))
			assert.NilError(t, crud.ReadJSONL("peer", bytes.NewReader(input)))
			assert.Assert(t, !keyState(t, q, keysAge, mutationConsumer).Dirty)
		})
	}
}

func TestDirtyKeysReconciliation(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, keysMemoryPath)
	registerKeys(t, q, keysAge, mutationConsumer)
	state := keyState(t, q, keysAge, mutationConsumer)
	assert.NilError(t, crud.Init())
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token))
	state = keyState(t, q, keysAge, mutationConsumer)
	binding := PersonGeneratedBinding
	binding.DirtyKeys = []rt.DirtyKeysDescriptor{{Name: keysAge, WatchedColumns: []string{"name"}}}
	assert.NilError(t, rt.ReconcileGeneratedTableContext(ctx, q, binding))
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token), rt.ErrStaleDirtyKeys)
	assert.Assert(t, keyState(t, q, keysAge, mutationConsumer).Full)
	binding.DirtyKeys = nil
	assert.NilError(t, rt.ReconcileGeneratedTableContext(ctx, q, binding))
	_, err := rt.ReadDirtyKeys(ctx, q, PersonTableName, keysAge, mutationConsumer)
	assert.ErrorIs(t, err, sql.ErrNoRows)
	assert.NilError(t, crud.Init())
	registerKeys(t, q, keysAge, mutationConsumer)
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token), rt.ErrStaleDirtyKeys)
}

func TestDirtyKeysConcurrentAcknowledgment(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "keys.db") + "?_busy_timeout=10000&_journal_mode=WAL"
	_, first, crud := mutationDatabase(t, path)
	_, second, _ := mutationDatabase(t, path)
	registerKeys(t, first, keysAge, mutationConsumer)
	row, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
	assert.NilError(t, err)
	stale := keyState(t, first, keysAge, mutationConsumer)
	_, err = second.ExecContext(ctx, mutationUpdateAgeSQL, int64(20), row.ID)
	assert.NilError(t, err)
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, first, stale.Token), rt.ErrStaleDirtyKeys)
	state := keyState(t, second, keysAge, mutationConsumer)
	assert.DeepEqual(t, state.IDs, []string{row.ID})
	var group sync.WaitGroup
	results := make(chan error, 2)
	for _, q := range []rt.DBTX{first, second} {
		group.Go(func() { results <- rt.AcknowledgeDirtyKeys(ctx, q, state.Token) })
	}
	group.Wait()
	close(results)
	successes, staleErrors := 0, 0
	for err := range results {
		if errors.Is(err, rt.ErrStaleDirtyKeys) {
			staleErrors++
		} else {
			assert.NilError(t, err)
			successes++
		}
	}
	assert.Equal(t, successes, 1)
	assert.Equal(t, staleErrors, 1)
	_, err = second.ExecContext(ctx, mutationUpdateAgeSQL, int64(30), row.ID)
	assert.NilError(t, err)
	_, reopened, _ := mutationDatabase(t, path)
	assert.DeepEqual(t, keyState(t, reopened, keysAge, mutationConsumer).IDs, []string{row.ID})
}

func TestDirtyKeysNestedNullAndOmitSync(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, keysMemoryPath)
	for _, tc := range []struct{ table, tracker, column string }{
		{PhotoTableName, "location", "location_altitude"},
		{NoteTableName, keysAll, "text"},
	} {
		t.Run(tc.table, func(t *testing.T) {
			assert.NilError(t, rt.RegisterDirtyKeys(ctx, q, tc.table, tc.tracker, mutationConsumer))
			var id string
			if tc.table == PhotoTableName {
				row, err := crud.Photo.Insert(&Photo{})
				assert.NilError(t, err)
				id = row.ID
			} else {
				row, err := crud.Note.Insert(&Note{Text: "hello"})
				assert.NilError(t, err)
				id = row.ID
			}
			state, err := rt.ReadDirtyKeys(ctx, q, tc.table, tc.tracker, mutationConsumer)
			assert.NilError(t, err)
			assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token))
			values := []any{int64(42), nil}
			if tc.table == NoteTableName {
				values = []any{"updated", "again"}
			}
			for _, value := range values {
				_, err = q.ExecContext(ctx, `UPDATE "`+tc.table+`" SET "`+tc.column+`" = ? WHERE id = ?`, value, id)
				assert.NilError(t, err)
				state, err = rt.ReadDirtyKeys(ctx, q, tc.table, tc.tracker, mutationConsumer)
				assert.NilError(t, err)
				assert.DeepEqual(t, state.IDs, []string{id})
				assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token))
			}
		})
	}
}

func TestDirtyKeysUpgradeFromWindows(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, keysMemoryPath)
	registerAgeWindow(t, q, mutationConsumer)
	row, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
	assert.NilError(t, err)
	before := readAgeWindow(t, q, mutationConsumer)
	generation := mutationGeneration(t, q, PersonTableName)
	for _, table := range []string{PersonTableName, NoteTableName, PhotoTableName} {
		for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
			_, err = q.ExecContext(ctx, `DROP TRIGGER IF EXISTS "_proprdb_dirty_keys_`+table+`_`+strings.ToLower(event)+`"`)
			assert.NilError(t, err)
		}
	}
	for _, table := range []string{rt.CoreTableDirtyKeysEntryName, rt.CoreTableDirtyKeysConsumerName, rt.CoreTableDirtyKeysDefinitionName} {
		_, err = q.ExecContext(ctx, `DROP TABLE `+table)
		assert.NilError(t, err)
	}
	_, err = q.ExecContext(ctx, `UPDATE `+rt.CoreTableGenerationName+` SET signature = 'v2|legacy'`)
	assert.NilError(t, err)
	assert.NilError(t, crud.Init())
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), generation+1)
	assert.Assert(t, readAgeWindow(t, q, mutationConsumer).Full)
	assert.ErrorIs(t, rt.AcknowledgeDirtyWindow(ctx, q, before.Token), rt.ErrStaleDirtyWindow)
	registerKeys(t, q, keysAge, mutationConsumer)
	_, err = q.ExecContext(ctx, mutationUpdateAgeSQL, int64(20), row.ID)
	assert.NilError(t, err)
	assert.DeepEqual(t, keyState(t, q, keysAge, mutationConsumer).IDs, []string{row.ID})
}

func TestDirtyKeysSyncDeletionAndIgnoredWrites(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, keysMemoryPath)
	registerKeys(t, q, keysAge, mutationConsumer)
	registerKeys(t, q, keysAll, mutationConsumer)
	person := &Person{Name: testPersonNameAda, Age: 10}
	assert.NilError(t, crud.Person.upsertWithAtNs(mutationPersonID, 100, person))
	state := keyState(t, q, keysAge, mutationConsumer)
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token))
	assert.NilError(t, crud.Person.tombstoneWithAtNs(mutationPersonID, 200))
	deleted := keyState(t, q, keysAge, mutationConsumer)
	assert.DeepEqual(t, deleted.IDs, []string{mutationPersonID})
	assert.NilError(t, crud.Person.upsertWithAtNs(mutationPersonID, 150, person))
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, deleted.Token))
	clean := keyState(t, q, keysAge, mutationConsumer)
	assert.NilError(t, crud.Person.tombstoneWithAtNs(mutationPersonID, 300))
	assert.NilError(t, crud.Person.DeleteByID(mutationPersonID))
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, clean.Token))
	assert.NilError(t, crud.Person.upsertWithAtNs(mutationPersonID, rt.NowNs()+1000, person))
	assert.DeepEqual(t, keyState(t, q, keysAge, mutationConsumer).IDs, []string{mutationPersonID})
	all := keyState(t, q, keysAll, mutationConsumer)
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, all.Token))
	_, err := q.ExecContext(ctx, `UPDATE "`+PersonTableName+`" SET at_ns = at_ns + 1 WHERE id = ?`, mutationPersonID)
	assert.NilError(t, err)
	assert.DeepEqual(t, keyState(t, q, keysAll, mutationConsumer).IDs, []string{mutationPersonID})
}

func TestDirtyKeysFailedAcknowledgmentPreservesToken(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, keysMemoryPath)
	registerKeys(t, q, keysAge, mutationConsumer)
	row, err := crud.Person.Insert(&Person{Name: testPersonNameAda, Age: 10})
	assert.NilError(t, err)
	state := keyState(t, q, keysAge, mutationConsumer)
	_, err = q.ExecContext(ctx, `CREATE TRIGGER fail_dirty_keys_cleanup BEFORE DELETE ON `+rt.CoreTableDirtyKeysEntryName+` BEGIN SELECT RAISE(ABORT, 'injected cleanup failure'); END`)
	assert.NilError(t, err)
	err = rt.AcknowledgeDirtyKeys(ctx, q, state.Token)
	assert.Assert(t, err != nil)
	assert.DeepEqual(t, keyState(t, q, keysAge, mutationConsumer).IDs, []string{row.ID})
	_, err = q.ExecContext(ctx, `DROP TRIGGER fail_dirty_keys_cleanup`)
	assert.NilError(t, err)
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, state.Token))
	assert.Assert(t, !keyState(t, q, keysAge, mutationConsumer).Dirty)
}
