package genexample

import (
	"context"
	"database/sql"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	_ "github.com/mattn/go-sqlite3"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

const countTombstoneByIDSQL = "SELECT COUNT(*) FROM _deleted WHERE table_name = ? AND id = ?"
const personNameIndex = "idx_generatedtest_example_person__name"
const personNameAgeIndex = "idx_generatedtest_example_person__name_age"
const personStaleIndex = "idx_generatedtest_example_person__stale"

func TestGeneratedCRUD(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})

	crud := NewCRUD(db)
	assert.NilError(t, crud.Init())

	indexesAfterInit := tableIndexNamesByName(t, ctx, db, PersonTableName)
	expectedIndexes := []string{personNameIndex, personNameAgeIndex}
	for _, indexName := range expectedIndexes {
		assert.Check(t, indexesAfterInit[indexName])
	}

	assert.NilError(t, crud.Person.Init())
	indexesAfterSecondInit := tableIndexNamesByName(t, ctx, db, PersonTableName)
	for _, indexName := range expectedIndexes {
		assert.Check(t, indexesAfterSecondInit[indexName])
	}

	_, err = db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS "`+personStaleIndex+`" ON "`+PersonTableName+`" ("name")`)
	assert.NilError(t, err)
	indexesWithStale := tableIndexNamesByName(t, ctx, db, PersonTableName)
	assert.Check(t, indexesWithStale[personStaleIndex])

	assert.NilError(t, crud.Person.Init())
	indexesAfterCleanup := tableIndexNamesByName(t, ctx, db, PersonTableName)
	assert.Check(t, !indexesAfterCleanup[personStaleIndex])

	var hiddenTableCount int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", "generatedtest_example_hidden").Scan(&hiddenTableCount)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(hiddenTableCount, 0))

	_, err = crud.Person.Insert(&Person{Name: "", Age: 1})
	assert.Check(t, err != nil)

	inserted, err := crud.Person.Insert(&Person{Name: "Ada", Age: 37})
	assert.NilError(t, err)
	assert.Check(t, inserted.ID != "")
	assert.Check(t, inserted.AtNs > 0)

	customID := "018f4f3f-6f9f-7a1b-8f55-1234567890ab"
	insertedWithID, err := crud.Person.InsertWithID(customID, &Person{Name: "Grace", Age: 30})
	assert.NilError(t, err)
	assert.Check(t, is.Equal(insertedWithID.ID, customID))
	assert.Check(t, insertedWithID.AtNs > 0)

	insertWithIDCases := []struct {
		name string
		id   string
		data *Person
	}{
		{name: "empty ID", id: "", data: &Person{Name: "Empty ID", Age: 1}},
		{name: "invalid UUID", id: "not-a-uuid", data: &Person{Name: "Bad ID", Age: 1}},
		{name: "nil data", id: customID, data: nil},
	}
	for _, testCase := range insertWithIDCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, caseErr := crud.Person.InsertWithID(testCase.id, testCase.data)
			assert.Check(t, caseErr != nil)
		})
	}

	selected, err := crud.Person.Select("name = ?", "Ada")
	assert.NilError(t, err)
	assert.Check(t, is.Len(selected, 1))
	assert.Check(t, is.Equal(selected[0].ID, inserted.ID))

	assert.NilError(t, crud.Person.DeleteByID(inserted.ID))

	var tombstoneCount int
	err = db.QueryRowContext(ctx, countTombstoneByIDSQL, PersonTableName, inserted.ID).Scan(&tombstoneCount)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(tombstoneCount, 1))

	updated, err := crud.Person.UpdateByID(inserted.ID, &Person{Name: "Ada Lovelace", Age: 38})
	assert.NilError(t, err)
	assert.Check(t, is.Equal(updated.ID, inserted.ID))

	err = db.QueryRowContext(ctx, countTombstoneByIDSQL, PersonTableName, inserted.ID).Scan(&tombstoneCount)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(tombstoneCount, 0))

	_, err = crud.Person.UpdateByID("not-a-uuid", &Person{Name: "Nope", Age: 10})
	assert.Check(t, err != nil)

	_, err = db.ExecContext(ctx, "UPDATE \""+PersonTableName+"\" SET \"age\" = 0 WHERE id = ?", inserted.ID)
	assert.NilError(t, err)
	_, err = db.ExecContext(ctx, "UPDATE _proprdb_schema SET schema_hash = ? WHERE table_name = ?", "stale", PersonTableName)
	assert.NilError(t, err)
	assert.NilError(t, crud.Person.Init())

	var projectedAge int64
	err = db.QueryRowContext(ctx, "SELECT \"age\" FROM \""+PersonTableName+"\" WHERE id = ?", inserted.ID).Scan(&projectedAge)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(projectedAge, int64(38)))

	updatedByRow, err := crud.Person.UpdateRow(PersonRow{
		ID:   inserted.ID,
		Data: &Person{Name: "Countess of Lovelace", Age: 39},
	})
	assert.NilError(t, err)
	assert.Check(t, is.Equal(updatedByRow.ID, inserted.ID))

	assert.NilError(t, crud.Person.DeleteRow(PersonRow{ID: inserted.ID}))

	err = db.QueryRowContext(ctx, countTombstoneByIDSQL, PersonTableName, inserted.ID).Scan(&tombstoneCount)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(tombstoneCount, 1))

	tx, err := db.BeginTx(ctx, nil)
	assert.NilError(t, err)

	txTable := NewPersonTable(tx)
	if _, err := txTable.Insert(&Person{Name: "Tx User", Age: 41}); err != nil {
		rollbackErr := tx.Rollback()
		assert.NilError(t, rollbackErr)
		t.Fatalf("insert using tx table: %v", err)
	}
	assert.NilError(t, tx.Commit())

	insertedNote, err := crud.Note.Insert(&Note{Text: "Projected note"})
	assert.NilError(t, err)
	var projectedText string
	err = db.QueryRowContext(ctx, "SELECT \"text\" FROM \""+NoteTableName+"\" WHERE id = ?", insertedNote.ID).Scan(&projectedText)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(projectedText, "Projected note"))
}

func TestGeneratedCRUDTableDescriptors(t *testing.T) {
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})

	crud := NewCRUD(db)
	descriptors := crud.TableDescriptors()
	expected := []rt.GeneratedTableDescriptor{
		{TableName: PersonTableName, TypeName: PersonTypeName, IsCore: false, SyncEnabled: true},
		{TableName: NoteTableName, TypeName: NoteTypeName, IsCore: false, SyncEnabled: false},
		{TableName: rt.CoreTableDeletedName, TypeName: "", IsCore: true, SyncEnabled: false},
		{TableName: rt.CoreTableSyncName, TypeName: "", IsCore: true, SyncEnabled: false},
		{TableName: rt.CoreTableSchemaStateName, TypeName: "", IsCore: true, SyncEnabled: false},
		{TableName: rt.CoreTableUnknownName, TypeName: "", IsCore: true, SyncEnabled: false},
	}
	assert.DeepEqual(t, descriptors, expected)

	descriptors[0].TableName = "mutated"
	descriptorsSecondRead := crud.TableDescriptors()
	assert.Check(t, is.Equal(descriptorsSecondRead[0].TableName, PersonTableName))
}

func tableIndexNamesByName(t *testing.T, ctx context.Context, db *sql.DB, tableName string) map[string]bool {
	t.Helper()

	rows, err := db.QueryContext(ctx, `SELECT name FROM pragma_index_list("`+tableName+`")`)
	assert.NilError(t, err)
	defer func() {
		assert.NilError(t, rows.Close())
	}()

	indexesByName := make(map[string]bool)
	for rows.Next() {
		var name string
		assert.NilError(t, rows.Scan(&name))
		indexesByName[name] = true
	}
	assert.NilError(t, rows.Err())

	return indexesByName
}
