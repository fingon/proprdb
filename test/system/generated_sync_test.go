package genexample

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

const (
	testRemoteA               = "remote-a"
	testRemoteWS              = "   "
	testRemoteEmpty           = ""
	typeURLPrefix             = "type.googleapis.com/"
	selectByIDSQL             = "id = ?"
	selectUnknownCountByIDSQL = "SELECT COUNT(*) FROM _unknown_types WHERE type_name = ? AND id = ?"
	insertUnknownRowSQL       = "INSERT INTO _unknown_types (type_name, id, at_ns, deleted, data_json) VALUES (?, ?, ?, ?, ?)"
)

const (
	unknownTypeName = "generatedtest.example.UnknownThing"
	unknownID       = "018f4f3f-6f9f-7a1b-8f55-1234567890aa"
	drainPersonID   = "018f4f3f-6f9f-7a1b-8f55-1234567890ac"
)

func TestGeneratedJSONLSync(t *testing.T) {
	ctx := context.Background()
	sourceDB, err := sql.Open("sqlite3", "file:source-sync?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, sourceDB.Close())
	})

	targetDB, err := sql.Open("sqlite3", "file:target-sync?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, targetDB.Close())
	})

	source := NewCRUD(sourceDB)
	assert.NilError(t, source.Init())
	target := NewCRUD(targetDB)
	assert.NilError(t, target.Init())

	personRow, err := source.Person.Insert(&Person{Name: "Ada", Age: 37})
	if err != nil {
		t.Fatalf("insert source person: %v", err)
	}
	noteRow, err := source.Note.Insert(&Note{Text: "to be deleted"})
	if err != nil {
		t.Fatalf("insert source note: %v", err)
	}
	if err := source.Note.DeleteByID(noteRow.ID); err != nil {
		t.Fatalf("delete source note: %v", err)
	}

	var firstExport bytes.Buffer
	assert.NilError(t, source.WriteJSONL(testRemoteA, &firstExport))
	firstLines := strings.Split(strings.TrimSpace(firstExport.String()), "\n")
	assert.Check(t, is.Len(firstLines, 1))

	var secondExport bytes.Buffer
	assert.NilError(t, source.WriteJSONL(testRemoteA, &secondExport))
	assert.Check(t, is.Equal(strings.TrimSpace(secondExport.String()), ""))

	assert.NilError(t, target.ReadJSONL(testRemoteA, strings.NewReader(firstExport.String())))

	targetPeople, err := target.Person.Select(selectByIDSQL, personRow.ID)
	if err != nil {
		t.Fatalf("select target person: %v", err)
	}
	assert.Check(t, is.Len(targetPeople, 1))
	assert.Check(t, is.Equal(targetPeople[0].Data.GetName(), "Ada"))

	var remoteSyncCount int
	if err := targetDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM _sync WHERE remote = ?", testRemoteA).Scan(&remoteSyncCount); err != nil {
		t.Fatalf("count remote sync entries: %v", err)
	}
	assert.Check(t, is.Equal(remoteSyncCount, 1))

	noteLine := fmt.Sprintf("{\"id\":%q,\"atNs\":%d,\"data\":{\"@type\":%q,\"text\":\"ignored\"}}\n", noteRow.ID, personRow.AtNs+10, typeURLPrefix+NoteTypeName)
	if err := target.ReadJSONL(testRemoteA, strings.NewReader(noteLine)); err != nil {
		t.Fatalf("read note line into target: %v", err)
	}
	targetNotes, err := target.Note.Select(selectByIDSQL, noteRow.ID)
	if err != nil {
		t.Fatalf("select target note after ignored sync line: %v", err)
	}
	assert.Check(t, is.Len(targetNotes, 0))
	var ignoredSyncCount int
	if err := targetDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM _sync WHERE object_id = ? AND table_name = ? AND remote = ?", noteRow.ID, NoteTableName, testRemoteA).Scan(&ignoredSyncCount); err != nil {
		t.Fatalf("count ignored note sync rows: %v", err)
	}
	assert.Check(t, is.Equal(ignoredSyncCount, 0))

	updatedPerson, err := source.Person.UpdateByID(personRow.ID, &Person{Name: "Ada Updated", Age: 38})
	if err != nil {
		t.Fatalf("update source person: %v", err)
	}

	var thirdExport bytes.Buffer
	assert.NilError(t, source.WriteJSONL(testRemoteA, &thirdExport))
	thirdLines := strings.Split(strings.TrimSpace(thirdExport.String()), "\n")
	assert.Check(t, is.Len(thirdLines, 1))

	if err := target.ReadJSONL(testRemoteA, strings.NewReader(thirdExport.String())); err != nil {
		t.Fatalf("read third export into target: %v", err)
	}
	targetPeople, err = target.Person.Select(selectByIDSQL, personRow.ID)
	if err != nil {
		t.Fatalf("re-select target person: %v", err)
	}
	assert.Check(t, is.Len(targetPeople, 1))
	assert.Check(t, is.Equal(targetPeople[0].Data.GetName(), "Ada Updated"))

	invalidByValidateLine := fmt.Sprintf(
		"{\"id\":%q,\"atNs\":%d,\"data\":{\"@type\":%q,\"name\":\"\",\"age\":1}}\n",
		personRow.ID,
		targetPeople[0].AtNs+1,
		typeURLPrefix+PersonTypeName,
	)
	if err := target.ReadJSONL(testRemoteA, strings.NewReader(invalidByValidateLine)); err != nil {
		t.Fatalf("read invalid-by-valid line into target: %v", err)
	}
	targetPeople, err = target.Person.Select(selectByIDSQL, personRow.ID)
	if err != nil {
		t.Fatalf("select target person after invalid-by-valid import: %v", err)
	}
	assert.Check(t, is.Len(targetPeople, 1))
	assert.Check(t, is.Equal(targetPeople[0].Data.GetName(), ""))

	localNewer, err := target.Person.UpdateByID(personRow.ID, &Person{Name: "Local Newer", Age: 99})
	if err != nil {
		t.Fatalf("update target person locally: %v", err)
	}

	staleDeleteAtNs := localNewer.AtNs - 1
	if staleDeleteAtNs < 0 {
		staleDeleteAtNs = 0
	}
	newerDeleteAtNs := localNewer.AtNs + 1
	deleteCases := []struct {
		name            string
		atNs            int64
		expectedRows    int
		expectedTopName string
	}{
		{name: "stale delete ignored", atNs: staleDeleteAtNs, expectedRows: 1, expectedTopName: "Local Newer"},
		{name: "newer delete applied", atNs: newerDeleteAtNs, expectedRows: 0},
	}
	for _, testCase := range deleteCases {
		t.Run(testCase.name, func(t *testing.T) {
			deleteLine := fmt.Sprintf("{\"id\":%q,\"deleted\":true,\"atNs\":%d,\"data\":{\"@type\":%q}}\n", personRow.ID, testCase.atNs, typeURLPrefix+PersonTypeName)
			assert.NilError(t, target.ReadJSONL(testRemoteA, strings.NewReader(deleteLine)))
			peopleAfterDelete, selectErr := target.Person.Select(selectByIDSQL, personRow.ID)
			assert.NilError(t, selectErr)
			assert.Check(t, is.Len(peopleAfterDelete, testCase.expectedRows))
			if testCase.expectedRows > 0 {
				assert.Check(t, is.Equal(peopleAfterDelete[0].Data.GetName(), testCase.expectedTopName))
			}
		})
	}

	var targetPersonTombstoneAtNs int64
	if err := targetDB.QueryRowContext(ctx, "SELECT at_ns FROM _deleted WHERE table_name = ? AND id = ?", PersonTableName, personRow.ID).Scan(&targetPersonTombstoneAtNs); err != nil {
		t.Fatalf("read target person tombstone timestamp: %v", err)
	}
	if targetPersonTombstoneAtNs != newerDeleteAtNs {
		t.Fatalf("expected tombstone at_ns %d, got %d", newerDeleteAtNs, targetPersonTombstoneAtNs)
	}

	var syncedAtNs int64
	if err := targetDB.QueryRowContext(ctx, "SELECT at_ns FROM _sync WHERE object_id = ? AND table_name = ? AND remote = ?", personRow.ID, PersonTableName, testRemoteA).Scan(&syncedAtNs); err != nil {
		t.Fatalf("read target _sync entry: %v", err)
	}
	assert.Check(t, syncedAtNs >= updatedPerson.AtNs)
}

func TestGeneratedJSONLEmptyRemoteNoSyncEntries(t *testing.T) {
	ctx := context.Background()
	sourceDB, err := sql.Open("sqlite3", "file:source-sync-empty-remote?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, sourceDB.Close())
	})

	targetDB, err := sql.Open("sqlite3", "file:target-sync-empty-remote?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, targetDB.Close())
	})

	source := NewCRUD(sourceDB)
	assert.NilError(t, source.Init())
	target := NewCRUD(targetDB)
	assert.NilError(t, target.Init())

	personRow, err := source.Person.Insert(&Person{Name: "Empty Remote", Age: 1})
	assert.NilError(t, err)

	var firstExport bytes.Buffer
	assert.NilError(t, source.WriteJSONL(testRemoteEmpty, &firstExport))
	firstExportText := strings.TrimSpace(firstExport.String())
	assert.Check(t, firstExportText != "")

	var secondExport bytes.Buffer
	assert.NilError(t, source.WriteJSONL(testRemoteEmpty, &secondExport))
	secondExportText := strings.TrimSpace(secondExport.String())
	assert.Check(t, is.Equal(secondExportText, firstExportText))

	assert.NilError(t, target.ReadJSONL(testRemoteEmpty, strings.NewReader(firstExport.String())))

	targetPeople, err := target.Person.Select(selectByIDSQL, personRow.ID)
	assert.NilError(t, err)
	assert.Check(t, is.Len(targetPeople, 1))
	assert.Check(t, is.Equal(targetPeople[0].Data.GetName(), "Empty Remote"))

	for _, db := range []*sql.DB{sourceDB, targetDB} {
		var emptyRemoteSyncCount int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM _sync WHERE remote = ?", testRemoteEmpty).Scan(&emptyRemoteSyncCount)
		assert.NilError(t, err)
		assert.Check(t, is.Equal(emptyRemoteSyncCount, 0))
	}

	var wsFirstExport bytes.Buffer
	assert.NilError(t, source.WriteJSONL(testRemoteWS, &wsFirstExport))
	assert.Check(t, strings.TrimSpace(wsFirstExport.String()) != "")

	var wsSecondExport bytes.Buffer
	assert.NilError(t, source.WriteJSONL(testRemoteWS, &wsSecondExport))
	assert.Check(t, is.Equal(strings.TrimSpace(wsSecondExport.String()), ""))

	assert.NilError(t, target.ReadJSONL(testRemoteWS, strings.NewReader(wsFirstExport.String())))

	for _, db := range []*sql.DB{sourceDB, targetDB} {
		var wsRemoteSyncCount int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM _sync WHERE remote = ?", testRemoteWS).Scan(&wsRemoteSyncCount)
		assert.NilError(t, err)
		assert.Check(t, is.Equal(wsRemoteSyncCount, 1))
	}
}

func TestGeneratedJSONLUnknownTypesAreCompacted(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file:unknown-sync-compact?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})

	crud := NewCRUD(db)
	assert.NilError(t, crud.Init())

	firstLine := fmt.Sprintf("{\"id\":%q,\"atNs\":10,\"data\":{\"@type\":%q,\"payload\":\"old\"}}\n", unknownID, typeURLPrefix+unknownTypeName)
	secondLine := fmt.Sprintf("{\"id\":%q,\"atNs\":20,\"data\":{\"@type\":%q,\"payload\":\"new\"}}\n", unknownID, typeURLPrefix+unknownTypeName)
	importData := firstLine + secondLine
	assert.NilError(t, crud.ReadJSONL(testRemoteA, strings.NewReader(importData)))

	var unknownRowCount int
	err = db.QueryRowContext(ctx, selectUnknownCountByIDSQL, unknownTypeName, unknownID).Scan(&unknownRowCount)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(unknownRowCount, 1))

	var storedAtNs int64
	err = db.QueryRowContext(ctx, "SELECT at_ns FROM _unknown_types WHERE type_name = ? AND id = ?", unknownTypeName, unknownID).Scan(&storedAtNs)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(storedAtNs, int64(20)))
}

func TestGeneratedInitDrainsUnknownRowsForKnownType(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file:unknown-sync-drain?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})

	crud := NewCRUD(db)
	assert.NilError(t, crud.Init())

	personAnyJSON := fmt.Sprintf("{\"@type\":%q,\"name\":\"Recovered\",\"age\":\"44\"}", typeURLPrefix+PersonTypeName)
	_, err = db.ExecContext(ctx, insertUnknownRowSQL, PersonTypeName, drainPersonID, int64(77), 0, personAnyJSON)
	assert.NilError(t, err)

	assert.NilError(t, crud.Person.Init())

	recoveredRows, err := crud.Person.Select(selectByIDSQL, drainPersonID)
	assert.NilError(t, err)
	assert.Check(t, is.Len(recoveredRows, 1))
	assert.Check(t, is.Equal(recoveredRows[0].Data.GetName(), "Recovered"))
	assert.Check(t, is.Equal(recoveredRows[0].Data.GetAge(), int64(44)))

	var unknownRowCount int
	err = db.QueryRowContext(ctx, selectUnknownCountByIDSQL, PersonTypeName, drainPersonID).Scan(&unknownRowCount)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(unknownRowCount, 0))
}
