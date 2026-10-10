package genexample

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"gotest.tools/v3/assert"
)

func BenchmarkInitialization(b *testing.B) {
	fixture, err := os.ReadFile("../testdata/initialization.sql")
	assert.NilError(b, err)
	for _, storage := range []string{"memory", "file"} {
		for _, rows := range []int{0, 1000, 10000} {
			b.Run(fmt.Sprintf("%s/rows=%d", storage, rows), func(b *testing.B) {
				path := ":memory:"
				if storage == "file" {
					path = filepath.Join(b.TempDir(), "init.sqlite")
				}
				db, err := sql.Open("sqlite3", path)
				assert.NilError(b, err)
				db.SetMaxOpenConns(1)
				b.Cleanup(func() { assert.NilError(b, db.Close()) })
				crud := NewCRUD(rt.WrapDB(db))
				assert.NilError(b, crud.Init())
				tx, err := db.Begin()
				assert.NilError(b, err)
				b.Cleanup(func() {
					if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
						b.Errorf("rollback fixture: %v", err)
					}
				})
				for _, statement := range strings.Split(string(fixture), ";") {
					if strings.TrimSpace(statement) == "" {
						continue
					}
					var args []any
					if strings.Contains(statement, "?") {
						args = []any{rows, rows}
					}
					_, err := tx.Exec(statement, args...)
					assert.NilError(b, err)
				}
				assert.NilError(b, tx.Commit())
				statements := []string{}
				recorded := NewCRUD(recordingDBTX{DBTX: rt.WrapDB(db), statements: &statements})
				assert.NilError(b, recorded.Init())
				statementCount := len(statements)
				b.ResetTimer()
				for b.Loop() {
					assert.NilError(b, crud.Init())
				}
				b.ReportMetric(float64(statementCount), "statements/op")
			})
		}
	}
}
