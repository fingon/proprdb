package genexample

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"gotest.tools/v3/assert"
)

const (
	rustInteropEnv = "PROPRDB_RUST_INTEROP_DIR"
	rustInteropID  = "01951d6e-a000-7000-8000-000000000001"
)

func TestRustRuntimeInterop(t *testing.T) {
	dir := os.Getenv(rustInteropEnv)
	if dir == "" {
		t.Skip("invoked by the Rust cross-language integration test")
	}
	db, err := sql.Open(sqliteDriverName, filepath.Join(dir, "rust.db"))
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	crud := NewCRUD(rt.WrapDB(db))
	assert.NilError(t, crud.Init())
	rows, err := crud.Person.Select("id = ?", rustInteropID)
	assert.NilError(t, err)
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, rows[0].Data.Name, testPersonNameAda)
	assert.Equal(t, rows[0].Data.Age, int64(35))
	q := rt.WrapDB(db)
	ctx := context.Background()
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(1))
	state := readAgeWindow(t, q, mutationConsumer)
	assert.Equal(t, state.Lower.Int64, int64(35))
	assert.Equal(t, state.Upper.Int64, int64(35))
	assert.NilError(t, rt.AcknowledgeDirtyWindow(ctx, q, state.Token))
	keys := keyState(t, q, keysAge, mutationConsumer)
	assert.Assert(t, keys.Dirty && !keys.Full)
	assert.DeepEqual(t, keys.IDs, []string{rustInteropID})
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, keys.Token))

	checkpointBytes, err := os.ReadFile(filepath.Join(dir, "rust.checkpoint"))
	assert.NilError(t, err)
	var checkpoint rt.JSONLCheckpoint
	assert.NilError(t, checkpoint.UnmarshalText(checkpointBytes))
	assert.NilError(t, crud.AcknowledgeJSONL(checkpoint))
	var acknowledged bytes.Buffer
	assert.NilError(t, crud.WriteJSONL("go", &acknowledged))
	assert.Equal(t, acknowledged.Len(), 0)

	jsonl, err := os.ReadFile(filepath.Join(dir, "rust.jsonl"))
	assert.NilError(t, err)
	target, err := sql.Open(sqliteDriverName, ":memory:")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, target.Close()) })
	targetCRUD := NewCRUD(rt.WrapDB(target))
	assert.NilError(t, targetCRUD.Init())
	assert.NilError(t, targetCRUD.ReadJSONL("rust", bytes.NewReader(jsonl)))
	choiceRows, err := targetCRUD.Choice.Select("id = ?", "01951d6e-a000-7000-8000-000000000002")
	assert.NilError(t, err)
	assert.Equal(t, len(rows), 1)
	assert.Equal(t, choiceRows[0].Data.Selection.(*Choice_Count).Count, int64(9007199254740993))
	var roundTrip bytes.Buffer
	assert.NilError(t, targetCRUD.WriteJSONL("", &roundTrip))
	assert.Equal(t, len(strings.Split(strings.TrimSpace(roundTrip.String()), "\n")), 4)

	_, err = crud.Person.UpdateByID(rustInteropID, &Person{Name: testPersonNameGrace, Age: 41})
	assert.NilError(t, err)
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(2))
	state = readAgeWindow(t, q, mutationConsumer)
	assert.Equal(t, state.Lower.Int64, int64(35))
	assert.Equal(t, state.Upper.Int64, int64(41))
	var exported bytes.Buffer
	goCheckpoint, err := crud.PrepareJSONL("rust", &exported)
	assert.NilError(t, err)
	assert.NilError(t, os.WriteFile(filepath.Join(dir, "go.jsonl"), exported.Bytes(), 0o600))
	goCheckpointBytes, err := goCheckpoint.MarshalText()
	assert.NilError(t, err)
	assert.NilError(t, os.WriteFile(filepath.Join(dir, "go.checkpoint"), goCheckpointBytes, 0o600))
}
