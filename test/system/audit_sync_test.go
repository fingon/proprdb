package genexample

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	_ "github.com/mattn/go-sqlite3"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

type failAfterWriter struct {
	successfulWrites int
	writes           int
	buffer           bytes.Buffer
}

func (w *failAfterWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.writes > w.successfulWrites {
		return 0, errors.New("injected writer failure")
	}
	return w.buffer.Write(data)
}

func TestPrepareAcknowledgeAndWriterRetry(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:audit-prepare?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	crud := NewCRUD(rt.WrapDB(db))
	assert.NilError(t, crud.Init())
	_, err = crud.Person.Insert(&Person{Name: "first"})
	assert.NilError(t, err)
	_, err = crud.Person.Insert(&Person{Name: "second"})
	assert.NilError(t, err)

	failingWriter := &failAfterWriter{successfulWrites: 1}
	_, err = crud.PrepareJSONL("remote", failingWriter)
	assert.ErrorContains(t, err, "injected writer failure")

	var retry bytes.Buffer
	checkpoint, err := crud.PrepareJSONL("remote", &retry)
	assert.NilError(t, err)
	assert.Equal(t, len(strings.Split(strings.TrimSpace(retry.String()), "\n")), 2)

	var beforeAck bytes.Buffer
	secondCheckpoint, err := crud.PrepareJSONL("remote", &beforeAck)
	assert.NilError(t, err)
	assert.Equal(t, len(strings.Split(strings.TrimSpace(beforeAck.String()), "\n")), 2)
	assert.NilError(t, crud.DiscardJSONL(secondCheckpoint))

	encodedCheckpoint, err := checkpoint.MarshalText()
	assert.NilError(t, err)
	var decodedCheckpoint rt.JSONLCheckpoint
	assert.NilError(t, decodedCheckpoint.UnmarshalText(encodedCheckpoint))
	assert.NilError(t, crud.AcknowledgeJSONL(decodedCheckpoint))

	var afterAck bytes.Buffer
	assert.NilError(t, crud.WriteJSONL("remote", &afterAck))
	assert.Equal(t, strings.TrimSpace(afterAck.String()), "")
}

func TestEqualTimestampConflictAndStringTimestamp(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:audit-conflict?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	crud := NewCRUD(rt.WrapDB(db))
	assert.NilError(t, crud.Init())

	const objectID = "018f4f3f-6f9f-7a1b-8f55-1234567890dd"
	const atNs = int64(1_761_736_535_123_456_789)
	line := fmt.Sprintf("{\"id\":%q,\"atNs\":%q,\"data\":{\"@type\":%q,\"name\":\"same\"}}\n", objectID, fmt.Sprint(atNs), typeURLPrefix+PersonTypeName)
	assert.NilError(t, crud.ReadJSONL("remote", strings.NewReader(line)))
	assert.NilError(t, crud.ReadJSONL("remote", strings.NewReader(line)))

	conflictingLine := fmt.Sprintf("{\"id\":%q,\"atNs\":%d,\"data\":{\"@type\":%q,\"name\":\"different\"}}\n", objectID, atNs, typeURLPrefix+PersonTypeName)
	err = crud.ReadJSONL("remote", strings.NewReader(conflictingLine))
	var conflict *rt.ConflictError
	assert.Assert(t, errors.As(err, &conflict))
	assert.Equal(t, conflict.ID, objectID)
}

func TestUnknownRecordsAreForwarded(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:audit-unknown-forward?mode=memory&cache=shared")
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, db.Close()) })
	crud := NewCRUD(rt.WrapDB(db))
	assert.NilError(t, crud.Init())

	line := fmt.Sprintf("{\"id\":%q,\"atNs\":100,\"data\":{\"@type\":%q,\"value\":\"parked\"}}\n", unknownID, typeURLPrefix+unknownTypeName)
	assert.NilError(t, crud.ReadJSONL("source", strings.NewReader(line)))

	var forwarded bytes.Buffer
	assert.NilError(t, crud.WriteJSONL("destination", &forwarded))
	assert.Check(t, is.Equal(strings.TrimSpace(forwarded.String()), strings.TrimSpace(line)))

	var syncCount int
	err = db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM _unknown_sync WHERE type_name = ? AND id = ? AND remote = ?", unknownTypeName, unknownID, "destination").Scan(&syncCount)
	assert.NilError(t, err)
	assert.Equal(t, syncCount, 1)
}
