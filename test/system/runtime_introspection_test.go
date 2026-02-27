package genexample

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	_ "github.com/mattn/go-sqlite3"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

func TestRTIntrospectTablesUsesDataBlobWhenPresent(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file:rt-introspect-data-column?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})
	assert.NilError(t, rt.EnsureCoreTables(db))
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS "thing" ("id" TEXT PRIMARY KEY, "at_ns" INTEGER NOT NULL, "data" BLOB NOT NULL)`)
	assert.NilError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO "thing" ("id", "at_ns", "data") VALUES ('a', 1, X'0102'), ('b', 2, X''), ('c', 3, X'ffffff')`)
	assert.NilError(t, err)

	descriptors := []rt.GeneratedTableDescriptor{
		{TableName: "thing", TypeName: "example.Thing", IsCore: false, SyncEnabled: true},
	}
	introspectionRows, err := rt.IntrospectTables(db, descriptors)
	assert.NilError(t, err)
	assert.Check(t, is.Len(introspectionRows, 1))
	assert.Check(t, is.Equal(introspectionRows[0].Descriptor.TableName, "thing"))
	assert.Check(t, is.Equal(introspectionRows[0].ObjectCount, int64(3)))
	assert.Check(t, is.Equal(introspectionRows[0].DiskUsageBytes, int64(5)))
}

func TestRTIntrospectTablesFallbackForNoDataColumn(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file:rt-introspect-no-data-column?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})
	assert.NilError(t, rt.EnsureCoreTables(db))
	_, err = db.ExecContext(ctx, `INSERT INTO "_deleted" ("table_name", "id", "at_ns") VALUES ('person', 'one', 123), ('note', 'two', 7)`)
	assert.NilError(t, err)

	descriptors := []rt.GeneratedTableDescriptor{{TableName: rt.CoreTableDeletedName, IsCore: true, SyncEnabled: false}}
	introspectionRows, err := rt.IntrospectTables(db, descriptors)
	assert.NilError(t, err)
	assert.Check(t, is.Len(introspectionRows, 1))
	assert.Check(t, is.Equal(introspectionRows[0].ObjectCount, int64(2)))

	var expectedBytes int64
	err = db.QueryRowContext(
		ctx,
		`SELECT COALESCE(SUM(COALESCE(LENGTH(CAST("table_name" AS BLOB)), 0) + COALESCE(LENGTH(CAST("id" AS BLOB)), 0) + COALESCE(LENGTH(CAST("at_ns" AS BLOB)), 0)), 0) FROM "_deleted"`,
	).Scan(&expectedBytes)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(introspectionRows[0].DiskUsageBytes, expectedBytes))
}

func TestRTIntrospectTablesEmptyTableReturnsZero(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file:rt-introspect-empty?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS "thing" ("id" TEXT PRIMARY KEY, "at_ns" INTEGER NOT NULL, "data" BLOB NOT NULL)`)
	assert.NilError(t, err)

	introspectionRows, err := rt.IntrospectTables(db, []rt.GeneratedTableDescriptor{{TableName: "thing", TypeName: "example.Thing", IsCore: false, SyncEnabled: true}})
	assert.NilError(t, err)
	assert.Check(t, is.Len(introspectionRows, 1))
	assert.Check(t, is.Equal(introspectionRows[0].ObjectCount, int64(0)))
	assert.Check(t, is.Equal(introspectionRows[0].DiskUsageBytes, int64(0)))
}

func TestRTIntrospectTablesMissingTableErrors(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:rt-introspect-missing-table?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})

	_, err = rt.IntrospectTables(db, []rt.GeneratedTableDescriptor{{TableName: "missing_table", TypeName: "example.Missing", IsCore: false, SyncEnabled: true}})
	assert.Assert(t, err != nil)
	assert.Check(t, strings.Contains(err.Error(), "count objects for table missing_table"))
}
