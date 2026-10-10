package genexample

import (
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"gotest.tools/v3/assert"
)

const (
	legacyObjectID  = "legacy-id"
	totalChangesSQL = "SELECT total_changes()"
)

func TestInitializationAndReadsTrustStoredData(t *testing.T) {
	db, q := openRuntimeTestDB(t)
	crud := NewCRUD(q)
	assert.NilError(t, crud.Init())
	row, err := crud.Person.Insert(&Person{Name: testPersonNameAda})
	assert.NilError(t, err)
	_, err = db.Exec(`UPDATE "`+PersonTableName+`" SET id = ?, data = ? WHERE id = ?`, legacyObjectID, []byte{}, row.ID)
	assert.NilError(t, err)
	assert.NilError(t, crud.Person.Init())
	assert.NilError(t, crud.Init())
	rows, err := crud.Person.Select(selectObjectByID, legacyObjectID)
	assert.NilError(t, err)
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, rows[0].ID, legacyObjectID)
	assert.Equal(t, rows[0].Data.Name, "")
	_, err = crud.Person.InsertWithID(legacyObjectID, &Person{Name: testPersonNameAda})
	assert.ErrorContains(t, err, "validate id")
	_, err = db.Exec(`UPDATE _proprdb_schema SET schema_hash = 'name:string' WHERE table_name = ?`, PersonTableName)
	assert.NilError(t, err)
	assert.NilError(t, crud.Person.Init())
	_, err = db.Exec(`UPDATE "`+PersonTableName+`" SET data = ? WHERE id = ?`, []byte{0xff}, legacyObjectID)
	assert.NilError(t, err)
	assert.NilError(t, crud.Init())
}

func TestUnchangedInitializationOnlyInspectsMetadata(t *testing.T) {
	for _, fullInit := range []bool{false, true} {
		t.Run(map[bool]string{false: "table", true: "CRUD"}[fullInit], func(t *testing.T) {
			db, original := openRuntimeTestDB(t)
			statements := []string{}
			q := recordingDBTX{DBTX: original, statements: &statements}
			crud := NewCRUD(q)
			assert.NilError(t, crud.Init())
			var changesBefore int
			assert.NilError(t, db.QueryRow(totalChangesSQL).Scan(&changesBefore))
			statements = nil
			if fullInit {
				assert.NilError(t, crud.Init())
			} else {
				assert.NilError(t, crud.Person.Init())
			}
			var changesAfter int
			assert.NilError(t, db.QueryRow(totalChangesSQL).Scan(&changesAfter))
			assert.Equal(t, changesAfter, changesBefore)
			coreSetups := 0
			drainQueries := 0
			for _, statement := range statements {
				if strings.HasPrefix(statement, "CREATE TABLE IF NOT EXISTS "+rt.CoreTableDeletedName+" ") {
					coreSetups++
				}
				if strings.HasPrefix(statement, "SELECT id, at_ns, deleted, data_json FROM "+rt.CoreTableUnknownName+" WHERE type_name = ?") {
					drainQueries++
				}
				assert.Check(t, !strings.HasPrefix(statement, "INSERT "), statement)
				assert.Check(t, !strings.HasPrefix(statement, "UPDATE "), statement)
				assert.Check(t, !strings.HasPrefix(statement, "CREATE INDEX "), statement)
				assert.Check(t, !strings.HasPrefix(statement, "DROP INDEX "), statement)
				if strings.HasPrefix(statement, "SELECT ") && !strings.Contains(statement, "pragma_") {
					assert.Check(t, strings.Contains(statement, " WHERE "), statement)
				}
			}
			assert.Equal(t, coreSetups, 1)
			expectedDrains := 1
			if fullInit {
				expectedDrains = 0
				for _, binding := range crudGeneratedBindings {
					if binding.Descriptor.SyncEnabled {
						expectedDrains++
					}
				}
			}
			assert.Equal(t, drainQueries, expectedDrains)
		})
	}
}
