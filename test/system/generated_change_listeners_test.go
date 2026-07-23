package genexample

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
	"time"

	rt "github.com/fingon/proprdb/rt"
	_ "github.com/mattn/go-sqlite3"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

const changeListenerTimeout = 2 * time.Second

func TestGeneratedChangeListenersLocalWrites(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:change-listeners-local?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, db.Close())
	})

	crud := NewCRUD(rt.WrapDB(db))
	assert.NilError(t, crud.Init())
	ctx, cancel := context.WithCancel(context.Background())
	changes, err := crud.Person.Changes(ctx)
	assert.NilError(t, err)
	noteChanges, err := crud.Note.Changes(ctx)
	assert.NilError(t, err)

	inserted, err := crud.Person.Insert(&Person{Name: "Ada", Age: 37})
	assert.NilError(t, err)
	insertChange := receivePersonChange(t, changes)
	assert.Check(t, !insertChange.Deleted)
	assert.Check(t, is.Equal(insertChange.ID, inserted.ID))
	assert.Check(t, is.Equal(insertChange.AtNs, inserted.AtNs))
	assert.Check(t, is.Equal(insertChange.Data.GetName(), "Ada"))

	updated, err := crud.Person.UpdateByID(inserted.ID, &Person{Name: "Ada Updated", Age: 38})
	assert.NilError(t, err)
	updateChange := receivePersonChange(t, changes)
	assert.Check(t, !updateChange.Deleted)
	assert.Check(t, is.Equal(updateChange.AtNs, updated.AtNs))
	assert.Check(t, is.Equal(updateChange.Data.GetName(), "Ada Updated"))

	assert.NilError(t, crud.Person.DeleteByID(inserted.ID))
	deleteChange := receivePersonChange(t, changes)
	assert.Check(t, deleteChange.Deleted)
	assert.Check(t, is.Equal(deleteChange.ID, inserted.ID))
	assert.Check(t, deleteChange.Data == nil)

	insertedNote, err := crud.Note.Insert(&Note{Text: "Local only"})
	assert.NilError(t, err)
	noteInsertChange := receiveNoteChange(t, noteChanges)
	assert.Check(t, !noteInsertChange.Deleted)
	assert.Check(t, is.Equal(noteInsertChange.ID, insertedNote.ID))
	assert.Check(t, is.Equal(noteInsertChange.AtNs, insertedNote.AtNs))

	assert.NilError(t, crud.Note.DeleteByID(insertedNote.ID))
	noteDeleteChange := receiveNoteChange(t, noteChanges)
	assert.Check(t, noteDeleteChange.Deleted)
	assert.Check(t, is.Equal(noteDeleteChange.ID, insertedNote.ID))
	assert.Check(t, noteDeleteChange.AtNs > insertedNote.AtNs)

	notes, err := crud.Note.Select("id = ?", insertedNote.ID)
	assert.NilError(t, err)
	assert.Check(t, is.Len(notes, 0))
	var noteTombstoneCount int
	err = db.QueryRowContext(ctx, countTombstoneByIDSQL, NoteTableName, insertedNote.ID).Scan(&noteTombstoneCount)
	assert.NilError(t, err)
	assert.Check(t, is.Equal(noteTombstoneCount, 0))

	cancel()
	select {
	case _, open := <-changes:
		assert.Check(t, !open)
	case <-time.After(changeListenerTimeout):
		t.Fatal("change listener did not close after cancellation")
	}
}

func TestGeneratedChangeListenersImportsAndFailures(t *testing.T) {
	sourceDB, err := sql.Open("sqlite3", "file:change-listeners-source?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, sourceDB.Close())
	})
	targetDB, err := sql.Open("sqlite3", "file:change-listeners-target?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() {
		assert.NilError(t, targetDB.Close())
	})

	source := NewCRUD(rt.WrapDB(sourceDB))
	target := NewCRUD(rt.WrapDB(targetDB))
	assert.NilError(t, source.Init())
	assert.NilError(t, target.Init())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changes, err := target.Person.Changes(ctx)
	assert.NilError(t, err)

	inserted, err := source.Person.Insert(&Person{Name: "Imported", Age: 1})
	assert.NilError(t, err)
	var exported bytes.Buffer
	assert.NilError(t, source.WriteJSONL("", &exported))
	exportedText := exported.String()
	assert.NilError(t, target.ReadJSONL("", bytes.NewBufferString(exportedText)))
	importChange := receivePersonChange(t, changes)
	assert.Check(t, !importChange.Deleted)
	assert.Check(t, is.Equal(importChange.ID, inserted.ID))
	assert.Check(t, is.Equal(importChange.Data.GetName(), "Imported"))

	assert.NilError(t, target.ReadJSONL("", bytes.NewBufferString(exportedText)))
	assertNoPersonChange(t, changes)

	_, err = target.Person.InsertWithID(inserted.ID, &Person{Name: "Duplicate", Age: 2})
	assert.ErrorContains(t, err, "UNIQUE constraint failed")
	assertNoPersonChange(t, changes)
}

func receivePersonChange(t *testing.T, changes <-chan PersonChange) PersonChange {
	t.Helper()
	select {
	case change, open := <-changes:
		assert.Check(t, open)
		return change
	case <-time.After(changeListenerTimeout):
		t.Fatal("timed out waiting for person change")
		return PersonChange{}
	}
}

func receiveNoteChange(t *testing.T, changes <-chan NoteChange) NoteChange {
	t.Helper()
	select {
	case change, open := <-changes:
		assert.Check(t, open)
		return change
	case <-time.After(changeListenerTimeout):
		t.Fatal("timed out waiting for note change")
		return NoteChange{}
	}
}

func assertNoPersonChange(t *testing.T, changes <-chan PersonChange) {
	t.Helper()
	select {
	case change := <-changes:
		t.Fatalf("unexpected person change: %+v", change)
	default:
	}
}
