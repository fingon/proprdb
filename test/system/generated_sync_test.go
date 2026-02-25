package genexample

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestGeneratedJSONLSync(t *testing.T) {
	ctx := context.Background()
	sourceDB, err := sql.Open("sqlite3", "file:source-sync?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open source sqlite: %v", err)
	}
	defer func() {
		_ = sourceDB.Close()
	}()

	targetDB, err := sql.Open("sqlite3", "file:target-sync?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("open target sqlite: %v", err)
	}
	defer func() {
		_ = targetDB.Close()
	}()

	source := NewCRUD(sourceDB)
	if err := source.Init(); err != nil {
		t.Fatalf("init source CRUD: %v", err)
	}
	target := NewCRUD(targetDB)
	if err := target.Init(); err != nil {
		t.Fatalf("init target CRUD: %v", err)
	}

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
	if err := source.WriteJSONL("remote-a", &firstExport); err != nil {
		t.Fatalf("first source export: %v", err)
	}
	firstLines := strings.Split(strings.TrimSpace(firstExport.String()), "\n")
	if len(firstLines) != 1 {
		t.Fatalf("expected 1 line in first export, got %d: %q", len(firstLines), firstExport.String())
	}

	var secondExport bytes.Buffer
	if err := source.WriteJSONL("remote-a", &secondExport); err != nil {
		t.Fatalf("second source export: %v", err)
	}
	if strings.TrimSpace(secondExport.String()) != "" {
		t.Fatalf("expected no lines in second export, got %q", secondExport.String())
	}

	if err := target.ReadJSONL("remote-a", strings.NewReader(firstExport.String())); err != nil {
		t.Fatalf("read first export into target: %v", err)
	}

	targetPeople, err := target.Person.Select("id = ?", personRow.ID)
	if err != nil {
		t.Fatalf("select target person: %v", err)
	}
	if len(targetPeople) != 1 {
		t.Fatalf("expected 1 target person after import, got %d", len(targetPeople))
	}
	if targetPeople[0].Data.GetName() != "Ada" {
		t.Fatalf("unexpected imported person name: %q", targetPeople[0].Data.GetName())
	}

	var remoteSyncCount int
	if err := targetDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM _sync WHERE remote = ?", "remote-a").Scan(&remoteSyncCount); err != nil {
		t.Fatalf("count remote sync entries: %v", err)
	}
	if remoteSyncCount != 1 {
		t.Fatalf("expected 1 remote sync entry, got %d", remoteSyncCount)
	}

	noteLine := fmt.Sprintf("{\"id\":%q,\"atNs\":%d,\"data\":{\"@type\":%q,\"text\":\"ignored\"}}\n", noteRow.ID, personRow.AtNs+10, "type.googleapis.com/"+NoteTypeName)
	if err := target.ReadJSONL("remote-a", strings.NewReader(noteLine)); err != nil {
		t.Fatalf("read note line into target: %v", err)
	}
	targetNotes, err := target.Note.Select("id = ?", noteRow.ID)
	if err != nil {
		t.Fatalf("select target note after ignored sync line: %v", err)
	}
	if len(targetNotes) != 0 {
		t.Fatalf("expected note sync line to be ignored, got %d rows", len(targetNotes))
	}
	var ignoredSyncCount int
	if err := targetDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM _sync WHERE object_id = ? AND table_name = ? AND remote = ?", noteRow.ID, NoteTableName, "remote-a").Scan(&ignoredSyncCount); err != nil {
		t.Fatalf("count ignored note sync rows: %v", err)
	}
	if ignoredSyncCount != 0 {
		t.Fatalf("expected no _sync entry for ignored note sync line, got %d", ignoredSyncCount)
	}

	updatedPerson, err := source.Person.UpdateByID(personRow.ID, &Person{Name: "Ada Updated", Age: 38})
	if err != nil {
		t.Fatalf("update source person: %v", err)
	}

	var thirdExport bytes.Buffer
	if err := source.WriteJSONL("remote-a", &thirdExport); err != nil {
		t.Fatalf("third source export: %v", err)
	}
	thirdLines := strings.Split(strings.TrimSpace(thirdExport.String()), "\n")
	if len(thirdLines) != 1 {
		t.Fatalf("expected 1 line in third export, got %d", len(thirdLines))
	}

	if err := target.ReadJSONL("remote-a", strings.NewReader(thirdExport.String())); err != nil {
		t.Fatalf("read third export into target: %v", err)
	}
	targetPeople, err = target.Person.Select("id = ?", personRow.ID)
	if err != nil {
		t.Fatalf("re-select target person: %v", err)
	}
	if len(targetPeople) != 1 {
		t.Fatalf("expected 1 target person after update import, got %d", len(targetPeople))
	}
	if targetPeople[0].Data.GetName() != "Ada Updated" {
		t.Fatalf("unexpected target person name after update import: %q", targetPeople[0].Data.GetName())
	}

	localNewer, err := target.Person.UpdateByID(personRow.ID, &Person{Name: "Local Newer", Age: 99})
	if err != nil {
		t.Fatalf("update target person locally: %v", err)
	}

	staleDeleteAtNs := localNewer.AtNs - 1
	if staleDeleteAtNs < 0 {
		staleDeleteAtNs = 0
	}
	staleDeleteLine := fmt.Sprintf("{\"id\":%q,\"deleted\":true,\"atNs\":%d,\"data\":{\"@type\":%q}}\n", personRow.ID, staleDeleteAtNs, "type.googleapis.com/"+PersonTypeName)
	if err := target.ReadJSONL("remote-a", strings.NewReader(staleDeleteLine)); err != nil {
		t.Fatalf("read stale delete line into target: %v", err)
	}
	targetPeople, err = target.Person.Select("id = ?", personRow.ID)
	if err != nil {
		t.Fatalf("select target person after stale delete: %v", err)
	}
	if len(targetPeople) != 1 {
		t.Fatalf("expected person to survive stale delete, got %d rows", len(targetPeople))
	}
	if targetPeople[0].Data.GetName() != "Local Newer" {
		t.Fatalf("expected local newer person to survive stale delete, got %q", targetPeople[0].Data.GetName())
	}

	newerDeleteAtNs := localNewer.AtNs + 1
	newerDeleteLine := fmt.Sprintf("{\"id\":%q,\"deleted\":true,\"atNs\":%d,\"data\":{\"@type\":%q}}\n", personRow.ID, newerDeleteAtNs, "type.googleapis.com/"+PersonTypeName)
	if err := target.ReadJSONL("remote-a", strings.NewReader(newerDeleteLine)); err != nil {
		t.Fatalf("read newer delete line into target: %v", err)
	}
	targetPeople, err = target.Person.Select("id = ?", personRow.ID)
	if err != nil {
		t.Fatalf("select target person after newer delete: %v", err)
	}
	if len(targetPeople) != 0 {
		t.Fatalf("expected person to be deleted by newer delete line, got %d rows", len(targetPeople))
	}

	var targetPersonTombstoneAtNs int64
	if err := targetDB.QueryRowContext(ctx, "SELECT at_ns FROM _deleted WHERE table_name = ? AND id = ?", PersonTableName, personRow.ID).Scan(&targetPersonTombstoneAtNs); err != nil {
		t.Fatalf("read target person tombstone timestamp: %v", err)
	}
	if targetPersonTombstoneAtNs != newerDeleteAtNs {
		t.Fatalf("expected tombstone at_ns %d, got %d", newerDeleteAtNs, targetPersonTombstoneAtNs)
	}

	var syncedAtNs int64
	if err := targetDB.QueryRowContext(ctx, "SELECT at_ns FROM _sync WHERE object_id = ? AND table_name = ? AND remote = ?", personRow.ID, PersonTableName, "remote-a").Scan(&syncedAtNs); err != nil {
		t.Fatalf("read target _sync entry: %v", err)
	}
	if syncedAtNs < updatedPerson.AtNs {
		t.Fatalf("expected synced at_ns >= %d, got %d", updatedPerson.AtNs, syncedAtNs)
	}
}
