package genexample

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"google.golang.org/protobuf/testing/protocmp"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

const (
	testPersonNameAda   = "Ada"
	testPersonNameGrace = "Grace"
	testInvalidUUID     = "not-a-uuid"
	testSourceRemote    = "source"
	validationUUIDv4    = "018f4f3f-6f9f-4a1b-8f55-1234567890ab"
	validationUUIDv5    = "21f7f8de-8051-5b89-8680-0195ef798b6a"
	selectObjectByID    = "id = ?"
)

func openRuntimeTestDB(t *testing.T) (*sql.DB, rt.DBTX) {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NilError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	return db, rt.WrapDB(db)
}

func TestOtherUUIDVersionsCRUDAndSync(t *testing.T) {
	for _, testCase := range []struct{ name, id string }{
		{name: "v4", id: validationUUIDv4},
		{name: "v5", id: validationUUIDv5},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, q := openRuntimeTestDB(t)
			crud := NewCRUD(q)
			assert.NilError(t, crud.Init())
			row, err := crud.Person.InsertWithID(testCase.id, &Person{Name: testPersonNameAda, Age: 37})
			assert.NilError(t, err)
			assert.Equal(t, row.ID, testCase.id)
			assert.NilError(t, crud.Init())
			row, err = crud.Person.UpdateRow(PersonRow{ID: row.ID, Data: &Person{Name: testPersonNameGrace, Age: 38}})
			assert.NilError(t, err)
			selected, err := crud.Person.Select(selectObjectByID, testCase.id)
			assert.NilError(t, err)
			assert.DeepEqual(t, selected, []PersonRow{row}, protocmp.Transform())

			var output strings.Builder
			assert.NilError(t, crud.WriteJSONL("", &output))
			_, targetQ := openRuntimeTestDB(t)
			target := NewCRUD(targetQ)
			assert.NilError(t, target.Init())
			assert.NilError(t, target.ReadJSONL(testSourceRemote, strings.NewReader(output.String())))
			imported, err := target.Person.Select(selectObjectByID, testCase.id)
			assert.NilError(t, err)
			assert.DeepEqual(t, imported, selected, protocmp.Transform())

			assert.NilError(t, crud.Person.DeleteRow(row))
			assert.NilError(t, crud.Init())
			output.Reset()
			assert.NilError(t, crud.WriteJSONL("", &output))
			assert.NilError(t, target.ReadJSONL(testSourceRemote, strings.NewReader(output.String())))
			imported, err = target.Person.Select(selectObjectByID, testCase.id)
			assert.NilError(t, err)
			assert.Check(t, is.Len(imported, 0))
			assert.NilError(t, target.Init())
		})
	}
}

func TestUUIDv5UnknownRowsDrain(t *testing.T) {
	_, q := openRuntimeTestDB(t)
	assert.NilError(t, rt.EnsureCoreTables(q))
	dataJSON, err := rt.MarshalAnyJSON(&Person{Name: testPersonNameAda, Age: 37})
	assert.NilError(t, err)
	record := rt.JSONLRecord{ID: validationUUIDv5, AtNs: 42, Data: dataJSON}
	var output strings.Builder
	assert.NilError(t, json.NewEncoder(&output).Encode(record))
	assert.NilError(t, rt.ReadBoundJSONLContext(context.Background(), q, nil, testSourceRemote, strings.NewReader(output.String())))
	crud := NewCRUD(q)
	assert.NilError(t, crud.Init())
	rows, err := crud.Person.Select(selectObjectByID, validationUUIDv5)
	assert.NilError(t, err)
	assert.Assert(t, is.Len(rows, 1))
	assert.Equal(t, rows[0].AtNs, int64(42))
	assert.Equal(t, rows[0].Data.Name, testPersonNameAda)
	output.Reset()
	assert.NilError(t, crud.WriteJSONL(testSourceRemote, &output))
	assert.Equal(t, output.String(), "")
}

func TestGenericRuntimeCRUD(t *testing.T) {
	_, q := openRuntimeTestDB(t)
	table := rt.NewTable(q, PersonGeneratedBinding)
	assert.NilError(t, table.Init(true))
	_, err := table.Insert[Person](&Person{})
	assert.ErrorContains(t, err, "validate")
	row, err := table.Insert[Person](&Person{Name: testPersonNameAda, Age: 37})
	assert.NilError(t, err)
	assert.NilError(t, rt.ValidateUUIDv7(row.ID))
	rows, err := table.Select[Person, *Person](selectObjectByID, row.ID)
	assert.NilError(t, err)
	assert.DeepEqual(t, rows, []rt.Row[*Person]{row}, protocmp.Transform())
	updated, err := table.UpdateByID[Person](row.ID, &Person{Name: testPersonNameGrace, Age: 38})
	assert.NilError(t, err)
	assert.Check(t, updated.AtNs > row.AtNs)
	assert.NilError(t, table.DeleteRow(updated))
	assert.NilError(t, table.DrainUnknownRows())
}

func TestGenericRuntimeInvalidWrites(t *testing.T) {
	_, q := openRuntimeTestDB(t)
	table := rt.NewTable(q, PersonGeneratedBinding)
	assert.NilError(t, table.Init(true))
	for _, testCase := range []struct {
		name, id string
		data     *Person
		expected string
	}{
		{name: "nil", id: validationUUIDv5, expected: "nil data"},
		{name: "empty id", data: &Person{Name: testPersonNameAda}, expected: "empty id"},
		{name: "malformed id", id: "bad-id", data: &Person{Name: testPersonNameAda}, expected: "validate id"},
		{name: "invalid data", id: validationUUIDv5, data: &Person{}, expected: "validate"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := table.InsertWithID[Person](testCase.id, testCase.data)
			assert.ErrorContains(t, err, testCase.expected)
			_, err = table.UpdateByID[Person](testCase.id, testCase.data)
			assert.ErrorContains(t, err, testCase.expected)
		})
	}
	var zero rt.Table
	_, err := zero.Select[Person, *Person](selectObjectByID, validationUUIDv5)
	assert.ErrorContains(t, err, "nil DBTX")
	_, err = zero.Insert[Person](&Person{Name: testPersonNameAda})
	assert.ErrorContains(t, err, "nil DBTX")
	assert.ErrorContains(t, zero.Init(true), "nil DBTX")
	assert.ErrorContains(t, zero.DeleteByID(validationUUIDv5), "nil DBTX")
}

func TestGenericSelectionDecodeFailureClosesRows(t *testing.T) {
	db, q := openRuntimeTestDB(t)
	crud := NewCRUD(q)
	assert.NilError(t, crud.Init())
	row, err := crud.Person.InsertWithID(validationUUIDv5, &Person{Name: testPersonNameAda})
	assert.NilError(t, err)
	_, err = db.Exec(`UPDATE "`+PersonTableName+`" SET data = ? WHERE id = ?`, []byte{0xff}, row.ID)
	assert.NilError(t, err)
	rows, err := crud.Person.Select(selectObjectByID, row.ID)
	assert.ErrorContains(t, err, "unmarshal")
	assert.Assert(t, rows == nil)
	_, err = crud.Person.UpdateByID(row.ID, row.Data)
	assert.NilError(t, err)
	rows, err = crud.Person.Select(selectObjectByID, row.ID)
	assert.NilError(t, err)
	assert.Assert(t, is.Len(rows, 1))
}

func TestGeneratedTableOptInAPIs(t *testing.T) {
	for _, testCase := range []struct {
		name              string
		table             any
		customID, changes bool
	}{
		{name: "Person", table: (*PersonTable)(nil), customID: true, changes: true},
		{name: "Note", table: (*NoteTable)(nil), changes: true},
		{name: "Choice", table: (*ChoiceTable)(nil)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			tableType := reflect.TypeOf(testCase.table)
			_, hasCustomID := tableType.MethodByName("InsertWithID")
			_, hasChanges := tableType.MethodByName("Changes")
			assert.Equal(t, hasCustomID, testCase.customID)
			assert.Equal(t, hasChanges, testCase.changes)
		})
	}
}
