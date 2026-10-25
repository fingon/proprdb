package genexample

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	rt "github.com/fingon/proprdb/rt"
	"gotest.tools/v3/assert"
)

const (
	connectionTestTimeout = 10 * time.Second
	connectionWaitTimeout = 25 * time.Millisecond
)

func TestWrapDBLimitsConnections(t *testing.T) {
	const query = "SELECT 1"
	for _, maxConnections := range []int{0, 1, 4} {
		t.Run(strconv.Itoa(maxConnections), func(t *testing.T) {
			db, err := sql.Open(sqliteDriverName, ":memory:")
			assert.NilError(t, err)
			t.Cleanup(func() { assert.NilError(t, db.Close()) })
			db.SetMaxOpenConns(maxConnections)
			q := rt.WrapDB(db)
			assert.Equal(t, db.Stats().MaxOpenConnections, 1)

			ctx, cancel := context.WithTimeout(t.Context(), connectionTestTimeout)
			defer cancel()
			func() {
				conn, err := db.Conn(ctx)
				assert.NilError(t, err)
				defer func() { assert.NilError(t, conn.Close()) }()
				waitingCtx, stopWaiting := context.WithTimeout(ctx, connectionWaitTimeout)
				defer stopWaiting()
				_, err = q.ExecContext(waitingCtx, query)
				assert.ErrorIs(t, err, context.DeadlineExceeded)
			}()
			_, err = q.ExecContext(ctx, query)
			assert.NilError(t, err)
		})
	}
}

func TestConcurrentWritesThroughSharedPool(t *testing.T) {
	const writers = 8
	const writesPerWriter = 8
	db, err := sql.Open(sqliteDriverName, filepath.Join(t.TempDir(), "shared.db"))
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), connectionTestTimeout)
	defer cancel()
	crud := NewCRUD(rt.WrapDB(db))
	assert.NilError(t, crud.InitContext(ctx))

	start := make(chan struct{})
	errorsCh := make(chan error, writers)
	var group sync.WaitGroup
	for range writers {
		table := NewPersonTable(rt.WrapDB(db))
		group.Go(func() {
			<-start
			for range writesPerWriter {
				row, err := table.InsertContext(ctx, &Person{Name: testPersonNameAda})
				if err != nil {
					errorsCh <- err
					return
				}
				rows, err := table.SelectContext(ctx, selectObjectByID, row.ID)
				if err != nil {
					errorsCh <- err
					return
				}
				if len(rows) != 1 {
					errorsCh <- fmt.Errorf("select inserted row: got %d rows", len(rows))
					return
				}
			}
		})
	}
	close(start)
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		assert.NilError(t, err)
	}
	rows, err := crud.Person.SelectContext(ctx, "name = ?", testPersonNameAda)
	assert.NilError(t, err)
	assert.Equal(t, len(rows), writers*writesPerWriter)
	statistics, err := rt.QueryStatisticsContext(ctx, rt.WrapDB(db))
	assert.NilError(t, err)
	var calls int64
	for _, statistic := range statistics {
		calls += statistic.Calls
	}
	assert.Equal(t, calls, int64(writers*writesPerWriter+1))
}
