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
	"google.golang.org/protobuf/proto"
)

func TestEnsureCoreTablesMigratesLegacyUnknownRowsToLatestState(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:legacy-unknown-schema?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	_, err = db.Exec(`CREATE TABLE _unknown_types (type_name TEXT NOT NULL, id TEXT NOT NULL, at_ns INTEGER NOT NULL, deleted INTEGER NOT NULL, data_json TEXT NOT NULL, PRIMARY KEY (type_name, id, at_ns))`)
	assert.NilError(t, err)
	_, err = db.Exec(insertUnknownRowSQL, unknownTypeName, validationUUIDv7, int64(10), 0, `{"value":"old"}`)
	assert.NilError(t, err)
	_, err = db.Exec(insertUnknownRowSQL, unknownTypeName, validationUUIDv7, int64(20), 0, `{"value":"latest"}`)
	assert.NilError(t, err)

	assert.NilError(t, rt.EnsureCoreTables(rt.WrapDB(db)))

	var rowCount int
	assert.NilError(t, db.QueryRow(selectUnknownCountByIDSQL, unknownTypeName, validationUUIDv7).Scan(&rowCount))
	assert.Equal(t, rowCount, 1)
	var atNs int64
	var dataJSON string
	assert.NilError(t, db.QueryRow(`SELECT at_ns, data_json FROM _unknown_types WHERE type_name = ? AND id = ?`, unknownTypeName, validationUUIDv7).Scan(&atNs, &dataJSON))
	assert.Equal(t, atNs, int64(20))
	assert.Equal(t, dataJSON, `{"value":"latest"}`)

	_, err = db.Exec(insertUnknownRowSQL, unknownTypeName, validationUUIDv7, int64(30), 0, `{"value":"duplicate"}`)
	assert.Check(t, err != nil)
}

func TestProjectionReconciliationRemovesObsoleteColumns(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:projection-remove?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	crud := NewCRUD(rt.WrapDB(db))
	assert.NilError(t, crud.Init())
	_, err = db.Exec(`ALTER TABLE "` + PersonTableName + `" ADD COLUMN "obsolete" TEXT`)
	assert.NilError(t, err)

	assert.NilError(t, crud.Person.Init())

	rows, err := db.Query(`PRAGMA table_info("` + PersonTableName + `")`)
	assert.NilError(t, err)
	defer rows.Close()
	foundObsolete := false
	for rows.Next() {
		var cid int
		var name string
		var columnType string
		var notNull int
		var defaultValue any
		var primaryKey int
		assert.NilError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
		foundObsolete = foundObsolete || name == "obsolete"
	}
	assert.NilError(t, rows.Err())
	assert.Check(t, !foundObsolete)
}

func TestProjectionReconciliationRejectsTypeChanges(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:projection-type?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	q := rt.WrapDB(db)
	assert.NilError(t, rt.EnsureCoreTables(q))
	_, err = db.Exec(`CREATE TABLE "` + PersonTableName + `" (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL, name INTEGER NOT NULL DEFAULT 0, age INTEGER NOT NULL DEFAULT 0)`)
	assert.NilError(t, err)

	err = NewPersonTable(q).Init()
	assert.Check(t, err != nil)
	assert.Check(t, strings.Contains(err.Error(), "has SQLite type INTEGER, expected TEXT"))
}

func TestProjectionReconciliationRepairsLegacyOneofPresence(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:projection-oneof?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	q := rt.WrapDB(db)
	assert.NilError(t, rt.EnsureCoreTables(q))
	_, err = db.Exec(`CREATE TABLE "` + ChoiceTableName + `" (id TEXT PRIMARY KEY, at_ns INTEGER NOT NULL, data BLOB NOT NULL, label TEXT NOT NULL DEFAULT '')`)
	assert.NilError(t, err)
	_, err = db.Exec(`INSERT INTO _proprdb_schema (table_name, schema_hash) VALUES (?, ?)`, ChoiceTableName, "label:string")
	assert.NilError(t, err)
	data, err := proto.Marshal(&Choice{Selection: &Choice_Count{Count: 7}})
	assert.NilError(t, err)
	_, err = db.Exec(`INSERT INTO "`+ChoiceTableName+`" (id, at_ns, data, label) VALUES (?, ?, ?, ?)`, validationUUIDv7, int64(1), data, "")
	assert.NilError(t, err)

	assert.NilError(t, NewChoiceTable(q).Init())

	var notNull int
	assert.NilError(t, db.QueryRow(`SELECT "notnull" FROM pragma_table_info(?) WHERE name = ?`, ChoiceTableName, "label").Scan(&notNull))
	assert.Check(t, is.Equal(notNull, 0))
	var label sql.NullString
	assert.NilError(t, db.QueryRow(`SELECT label FROM "`+ChoiceTableName+`" WHERE id = ?`, validationUUIDv7).Scan(&label))
	assert.Check(t, !label.Valid)
}

func TestKnownOmitSyncUnknownRecordDoesNotExport(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:known-omit-sync?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	crud := NewCRUD(rt.WrapDB(db))
	assert.NilError(t, crud.Init())
	dataJSON, err := rt.MarshalTypeOnlyAnyJSON(NoteTypeName)
	assert.NilError(t, err)
	_, err = db.ExecContext(context.Background(), `INSERT INTO _unknown_types (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?)`, NoteTypeName, validationUUIDv7, int64(1), false, string(dataJSON))
	assert.NilError(t, err)

	var exported strings.Builder
	assert.NilError(t, crud.WriteJSONL("", &exported))
	assert.Check(t, is.Equal(exported.String(), ""))

	var parkedCount int
	assert.NilError(t, db.QueryRow(`SELECT COUNT(*) FROM _unknown_types WHERE type_name = ? AND id = ?`, NoteTypeName, validationUUIDv7).Scan(&parkedCount))
	assert.Check(t, is.Equal(parkedCount, 1))
}

func TestStoredInvalidObjectIDFailsInitialization(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:invalid-stored-id?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	q := rt.WrapDB(db)
	assert.NilError(t, rt.EnsureCoreTables(q))
	_, err = db.Exec(`INSERT INTO _deleted (table_name, id, at_ns) VALUES (?, ?, ?)`, PersonTableName, "legacy-id", int64(1))
	assert.NilError(t, err)

	err = NewPersonTable(q).Init()
	assert.Check(t, err != nil)
	assert.Check(t, strings.Contains(err.Error(), "invalid stored object ID"))
}
