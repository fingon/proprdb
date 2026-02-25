package genexample

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestGeneratedCRUD(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	crud := NewCRUD(db)
	if err := crud.Init(); err != nil {
		t.Fatalf("init CRUD: %v", err)
	}

	inserted, err := crud.Person.Insert(&Person{Name: "Ada", Age: 37})
	if err != nil {
		t.Fatalf("insert person: %v", err)
	}
	if inserted.ID == "" {
		t.Fatalf("inserted ID is empty")
	}
	if inserted.AtNs <= 0 {
		t.Fatalf("inserted AtNs is not positive: %d", inserted.AtNs)
	}

	selected, err := crud.Person.Select("name = ?", "Ada")
	if err != nil {
		t.Fatalf("select person: %v", err)
	}
	if len(selected) != 1 {
		t.Fatalf("expected 1 selected person, got %d", len(selected))
	}
	if selected[0].ID != inserted.ID {
		t.Fatalf("selected ID mismatch: got %q want %q", selected[0].ID, inserted.ID)
	}

	if err := crud.Person.DeleteByID(inserted.ID); err != nil {
		t.Fatalf("delete person by id: %v", err)
	}

	var tombstoneCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM _deleted WHERE table_name = ? AND id = ?", PersonTableName, inserted.ID).Scan(&tombstoneCount); err != nil {
		t.Fatalf("count tombstones: %v", err)
	}
	if tombstoneCount != 1 {
		t.Fatalf("expected 1 tombstone, got %d", tombstoneCount)
	}

	updated, err := crud.Person.UpdateByID(inserted.ID, &Person{Name: "Ada Lovelace", Age: 38})
	if err != nil {
		t.Fatalf("update person by id: %v", err)
	}
	if updated.ID != inserted.ID {
		t.Fatalf("updated ID mismatch: got %q want %q", updated.ID, inserted.ID)
	}

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM _deleted WHERE table_name = ? AND id = ?", PersonTableName, inserted.ID).Scan(&tombstoneCount); err != nil {
		t.Fatalf("count tombstones after update: %v", err)
	}
	if tombstoneCount != 0 {
		t.Fatalf("expected 0 tombstones after update, got %d", tombstoneCount)
	}

	if _, err := db.ExecContext(ctx, "UPDATE \""+PersonTableName+"\" SET \"age\" = 0 WHERE id = ?", inserted.ID); err != nil {
		t.Fatalf("degrade projection column: %v", err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE _proprdb_schema SET schema_hash = ? WHERE table_name = ?", "stale", PersonTableName); err != nil {
		t.Fatalf("set stale schema hash: %v", err)
	}
	if err := crud.Person.Init(); err != nil {
		t.Fatalf("re-init person table: %v", err)
	}

	var projectedAge int64
	if err := db.QueryRowContext(ctx, "SELECT \"age\" FROM \""+PersonTableName+"\" WHERE id = ?", inserted.ID).Scan(&projectedAge); err != nil {
		t.Fatalf("read projected age: %v", err)
	}
	if projectedAge != 38 {
		t.Fatalf("expected projected age=38 after reprojection, got %d", projectedAge)
	}

	updatedByRow, err := crud.Person.UpdateRow(PersonRow{
		ID:   inserted.ID,
		Data: &Person{Name: "Countess of Lovelace", Age: 39},
	})
	if err != nil {
		t.Fatalf("update person by row: %v", err)
	}
	if updatedByRow.ID != inserted.ID {
		t.Fatalf("updated-by-row ID mismatch: got %q want %q", updatedByRow.ID, inserted.ID)
	}

	if err := crud.Person.DeleteRow(PersonRow{ID: inserted.ID}); err != nil {
		t.Fatalf("delete person by row: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM _deleted WHERE table_name = ? AND id = ?", PersonTableName, inserted.ID).Scan(&tombstoneCount); err != nil {
		t.Fatalf("count tombstones after row delete: %v", err)
	}
	if tombstoneCount != 1 {
		t.Fatalf("expected 1 tombstone after row delete, got %d", tombstoneCount)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	txTable := NewPersonTable(tx)
	if _, err := txTable.Insert(&Person{Name: "Tx User", Age: 41}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert using tx table: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
}
