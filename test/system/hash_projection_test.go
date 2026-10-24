package genexample

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"google.golang.org/protobuf/proto"
	"gotest.tools/v3/assert"
)

const (
	hashTracker       = "support"
	hashMixed         = "mixed"
	hashNameColumn    = "_proprdb_hash_name"
	hashSupportColumn = "_proprdb_hash_support"
	hashReadSQL       = `SELECT data, at_ns, "` + hashSupportColumn + `" FROM "` + HashRecordTableName + `" WHERE id = ?`
)

func hashFingerprint(t *testing.T, data *HashRecord) []byte {
	t.Helper()
	values, err := HashRecordGeneratedBinding.ProjectedValues(data)
	assert.NilError(t, err)
	value, ok := values[2].([]byte)
	assert.Assert(t, ok)
	assert.Equal(t, len(value), 8)
	return value
}

func hashState(t *testing.T, q rt.DBTX, tracker string) rt.DirtyKeysState {
	t.Helper()
	state, err := rt.ReadDirtyKeys(context.Background(), q, HashRecordTableName, tracker, mutationConsumer)
	assert.NilError(t, err)
	return state
}

func hashAcknowledge(t *testing.T, q rt.DBTX, tracker string) {
	t.Helper()
	assert.NilError(t, rt.AcknowledgeDirtyKeys(context.Background(), q, hashState(t, q, tracker).Token))
}

func TestHashProjectionSemanticChanges(t *testing.T) {
	baseline := &HashRecord{Detail: &HashRecord_Detail{Name: "anchor", Values: []int64{-1, 2}, Labels: map[int32]string{1: "one", 2: "two"}}, Tags: []string{"a", "b"}, Counters: map[string]int64{"a": 1, "b": 2}}
	expected := hashFingerprint(t, baseline)
	for _, tc := range []struct {
		name   string
		change func(*HashRecord)
		equal  bool
	}{
		{"metadata", func(v *HashRecord) { v.Label = "notes" }, true},
		{"map ordering", func(v *HashRecord) { v.Counters = map[string]int64{"b": 2, "a": 1} }, true},
		{"unknown nested", func(v *HashRecord) { v.Detail.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) }, true},
		{"unknown root", func(v *HashRecord) { v.ProtoReflect().SetUnknown([]byte{0xa0, 6, 1}) }, true},
		{"classification", func(v *HashRecord) { v.Enabled = true }, false},
		{"optional presence", func(v *HashRecord) { v.OptionalValue = proto.Int32(0) }, false},
		{"repeated ordering", func(v *HashRecord) { slices.Reverse(v.Tags) }, false},
		{"nested repeated", func(v *HashRecord) { slices.Reverse(v.Detail.Values) }, false},
		{"nested map", func(v *HashRecord) { v.Detail.Labels[2] = "changed" }, false},
		{"nested presence", func(v *HashRecord) { v.Detail.Child = &HashRecord_Detail{} }, false},
		{"oneof default", func(v *HashRecord) { v.Detail.Selection = &HashRecord_Detail_Text{Text: ""} }, false},
		{"negative zero", func(v *HashRecord) { v.Detail.Score = math.Copysign(0, -1) }, true},
		{"unsigned", func(v *HashRecord) { v.Detail.UnsignedValue = math.MaxUint64 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := proto.Clone(baseline).(*HashRecord)
			tc.change(data)
			assert.Equal(t, bytes.Equal(expected, hashFingerprint(t, data)), tc.equal)
		})
	}
	first := proto.Clone(baseline).(*HashRecord)
	first.Detail.Score = math.Float64frombits(0x7ff8000000000001)
	second := proto.Clone(first).(*HashRecord)
	second.Detail.Score = math.Float64frombits(0x7ff8000000000002)
	assert.DeepEqual(t, hashFingerprint(t, first), hashFingerprint(t, second))
}

func TestHashProjectionTrackedWrites(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, keysMemoryPath)
	for _, tracker := range []string{hashTracker, hashMixed} {
		assert.NilError(t, rt.RegisterDirtyKeys(ctx, q, HashRecordTableName, tracker, mutationConsumer))
		hashAcknowledge(t, q, tracker)
	}
	assert.NilError(t, rt.RegisterDirtyKeys(ctx, q, HashRecordTableName, hashTracker, keysOtherConsumer))
	other, err := rt.ReadDirtyKeys(ctx, q, HashRecordTableName, hashTracker, keysOtherConsumer)
	assert.NilError(t, err)
	assert.NilError(t, rt.AcknowledgeDirtyKeys(ctx, q, other.Token))
	data := &HashRecord{Detail: &HashRecord_Detail{Name: "first"}}
	row, err := crud.HashRecord.Insert(data)
	assert.NilError(t, err)
	assert.DeepEqual(t, hashState(t, q, hashTracker).IDs, []string{row.ID})
	hashAcknowledge(t, q, hashTracker)
	hashAcknowledge(t, q, hashMixed)
	data.Label = "metadata"
	row, err = crud.HashRecord.UpdateByID(row.ID, data)
	assert.NilError(t, err)
	assert.Assert(t, !hashState(t, q, hashTracker).Dirty)
	assert.Assert(t, hashState(t, q, hashMixed).Dirty)
	hashAcknowledge(t, q, hashMixed)
	data.Detail.Name = "second"
	row, err = crud.HashRecord.UpdateByID(row.ID, data)
	assert.NilError(t, err)
	stale := hashState(t, q, hashTracker)
	assert.Assert(t, hashState(t, q, hashMixed).Dirty)
	data.Enabled = true
	row, err = crud.HashRecord.UpdateByID(row.ID, data)
	assert.NilError(t, err)
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, q, stale.Token), rt.ErrStaleDirtyKeys)
	hashAcknowledge(t, q, hashTracker)
	rollback := errors.New("rollback hash mutation")
	assert.ErrorIs(t, q.WithTransaction(ctx, func(tx rt.DBTX) error {
		changed := proto.Clone(data).(*HashRecord)
		changed.Enabled = false
		_, err := NewHashRecordTable(tx).UpdateByID(row.ID, changed)
		if err != nil {
			return err
		}
		return rollback
	}), rollback)
	assert.Assert(t, !hashState(t, q, hashTracker).Dirty)
	changed := proto.Clone(data).(*HashRecord)
	changed.Detail.Name = "synced"
	assert.NilError(t, crud.HashRecord.upsertWithAtNs(row.ID, row.AtNs+1, changed))
	assert.Assert(t, hashState(t, q, hashTracker).Dirty)
	hashAcknowledge(t, q, hashTracker)
	assert.NilError(t, crud.HashRecord.upsertWithAtNs(row.ID, row.AtNs, data))
	assert.Assert(t, !hashState(t, q, hashTracker).Dirty)
	assert.NilError(t, crud.HashRecord.upsertWithAtNs(row.ID, row.AtNs+1, changed))
	assert.Assert(t, !hashState(t, q, hashTracker).Dirty)
	assert.NilError(t, crud.HashRecord.DeleteByID(row.ID))
	pending := hashState(t, q, hashTracker)
	assert.DeepEqual(t, pending.IDs, []string{row.ID})
	_, err = crud.HashRecord.InsertWithID(row.ID, changed)
	assert.NilError(t, err)
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, q, pending.Token), rt.ErrStaleDirtyKeys)
	assert.DeepEqual(t, hashState(t, q, hashTracker).IDs, []string{row.ID})
	other, err = rt.ReadDirtyKeys(ctx, q, HashRecordTableName, hashTracker, keysOtherConsumer)
	assert.NilError(t, err)
	assert.DeepEqual(t, other.IDs, []string{row.ID})
}

func TestHashProjectionReplacementAndIDChanges(t *testing.T) {
	for _, recursive := range []bool{false, true} {
		t.Run(map[bool]string{false: "nonrecursive", true: "recursive"}[recursive], func(t *testing.T) {
			ctx := context.Background()
			_, q, crud := mutationDatabase(t, keysMemoryPath)
			_, err := q.ExecContext(ctx, "PRAGMA recursive_triggers = "+map[bool]string{false: "OFF", true: "ON"}[recursive])
			assert.NilError(t, err)
			assert.NilError(t, rt.RegisterDirtyKeys(ctx, q, HashRecordTableName, hashTracker, mutationConsumer))
			first, err := crud.HashRecord.InsertWithID(mutationPersonID, &HashRecord{})
			assert.NilError(t, err)
			_, err = crud.HashRecord.InsertWithID(keysSecondID, &HashRecord{Enabled: true})
			assert.NilError(t, err)
			hashAcknowledge(t, q, hashTracker)
			_, err = q.ExecContext(ctx, `INSERT OR REPLACE INTO "`+HashRecordTableName+`" SELECT * FROM "`+HashRecordTableName+`" WHERE id = ?`, first.ID)
			assert.NilError(t, err)
			assert.DeepEqual(t, hashState(t, q, hashTracker).IDs, []string{first.ID})
			hashAcknowledge(t, q, hashTracker)
			_, err = q.ExecContext(ctx, `UPDATE OR REPLACE "`+HashRecordTableName+`" SET id = ? WHERE id = ?`, keysSecondID, first.ID)
			assert.NilError(t, err)
			assert.DeepEqual(t, hashState(t, q, hashTracker).IDs, []string{first.ID, keysSecondID})
		})
	}
}

func TestHashProjectionReconciliation(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, keysMemoryPath)
	row, err := crud.HashRecord.Insert(&HashRecord{Detail: &HashRecord_Detail{Name: "existing"}})
	assert.NilError(t, err)
	var before, fingerprint []byte
	var atNs int64
	assert.NilError(t, q.QueryRowContext(ctx, hashReadSQL, row.ID).Scan(&before, &atNs, &fingerprint))
	assert.NilError(t, rt.RegisterDirtyKeys(ctx, q, HashRecordTableName, hashTracker, mutationConsumer))
	hashAcknowledge(t, q, hashTracker)
	stale := hashState(t, q, hashTracker)
	legacy := HashRecordGeneratedBinding
	legacy.ProjectedColumns = legacy.ProjectedColumns[:1]
	legacy.DirtyKeys = []rt.DirtyKeysDescriptor{{Name: hashTracker}, {Name: hashMixed}}
	legacy.ProjectionSchema = "label:string"
	assert.NilError(t, rt.ReconcileGeneratedTableContext(ctx, q, legacy))
	assert.NilError(t, crud.HashRecord.Init())
	var after, actual []byte
	var afterNs int64
	assert.NilError(t, q.QueryRowContext(ctx, hashReadSQL, row.ID).Scan(&after, &afterNs, &actual))
	assert.DeepEqual(t, after, before)
	assert.Equal(t, afterNs, atNs)
	assert.DeepEqual(t, actual, fingerprint)
	assert.Assert(t, hashState(t, q, hashTracker).Full)
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, q, stale.Token), rt.ErrStaleDirtyKeys)
	hashAcknowledge(t, q, hashTracker)
	changed := HashRecordGeneratedBinding
	changed.ProjectionSchema = strings.ReplaceAll(changed.ProjectionSchema, "hash=xxh3-64-v1-", "hash=xxh3-64-v2-")
	const altered = "12345678"
	changed.ProjectedValues = func(m proto.Message) (values []any, err error) {
		values, err = HashRecordGeneratedBinding.ProjectedValues(m)
		if err != nil {
			return nil, err
		}
		values[2] = []byte(altered)
		return values, nil
	}
	assert.NilError(t, rt.ReconcileGeneratedTableContext(ctx, q, changed))
	assert.NilError(t, q.QueryRowContext(ctx, hashReadSQL, row.ID).Scan(&after, &afterNs, &actual))
	assert.Equal(t, string(actual), altered)
	assert.Assert(t, hashState(t, q, hashTracker).Full)
	assert.DeepEqual(t, after, before)
	assert.Equal(t, afterNs, atNs)
	failed := changed
	failed.ProjectionSchema = strings.ReplaceAll(changed.ProjectionSchema, "-v2-", "-v3-")
	failure := errors.New("fingerprint failure")
	failed.ProjectedValues = func(proto.Message) ([]any, error) { return nil, failure }
	assert.ErrorIs(t, rt.ReconcileGeneratedTableContext(ctx, q, failed), failure)
	var schema string
	assert.NilError(t, q.QueryRowContext(ctx, `SELECT schema_hash FROM _proprdb_schema WHERE table_name = ?`, HashRecordTableName).Scan(&schema))
	assert.Equal(t, schema, changed.ProjectionSchema)
}

func TestHashProjectionConcurrentAcknowledgement(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "hash.sqlite")
	_, q, crud := mutationDatabase(t, path)
	row, err := crud.HashRecord.Insert(&HashRecord{})
	assert.NilError(t, err)
	assert.NilError(t, rt.RegisterDirtyKeys(ctx, q, HashRecordTableName, hashTracker, mutationConsumer))
	hashAcknowledge(t, q, hashTracker)
	stale := hashState(t, q, hashTracker)
	second, err := sql.Open(sqliteDriverName, path)
	assert.NilError(t, err)
	t.Cleanup(func() { assert.NilError(t, second.Close()) })
	_, err = NewHashRecordTable(rt.WrapDB(second)).UpdateByID(row.ID, &HashRecord{Enabled: true})
	assert.NilError(t, err)
	assert.ErrorIs(t, rt.AcknowledgeDirtyKeys(ctx, q, stale.Token), rt.ErrStaleDirtyKeys)
	assert.DeepEqual(t, hashState(t, q, hashTracker).IDs, []string{row.ID})
}

func TestHashProjectionUnknownRowsAndWriteFailure(t *testing.T) {
	ctx := context.Background()
	_, q, crud := mutationDatabase(t, keysMemoryPath)
	assert.NilError(t, rt.RegisterDirtyKeys(ctx, q, HashRecordTableName, hashTracker, mutationConsumer))
	hashAcknowledge(t, q, hashTracker)
	data := &HashRecord{Enabled: true}
	payload, err := rt.MarshalAnyJSON(data)
	assert.NilError(t, err)
	record, err := json.Marshal(rt.JSONLRecord{ID: mutationPersonID, AtNs: 77, Data: payload})
	assert.NilError(t, err)
	assert.NilError(t, rt.ReadBoundJSONLContext(ctx, q, nil, "peer", bytes.NewReader(record)))
	assert.Assert(t, !hashState(t, q, hashTracker).Dirty)
	assert.NilError(t, crud.HashRecord.DrainUnknownRows())
	assert.DeepEqual(t, hashState(t, q, hashTracker).IDs, []string{mutationPersonID})
	hashAcknowledge(t, q, hashTracker)
	data.Detail = &HashRecord_Detail{}
	current := data.Detail
	for range 102 {
		current.Child = &HashRecord_Detail{}
		current = current.Child
	}
	_, err = crud.HashRecord.UpdateByID(mutationPersonID, data)
	assert.Assert(t, err != nil)
	assert.Assert(t, !hashState(t, q, hashTracker).Dirty)
	stored, err := crud.HashRecord.Select("id = ?", mutationPersonID)
	assert.NilError(t, err)
	assert.Equal(t, len(stored), 1)
	assert.Assert(t, stored[0].Data.Detail == nil)
}
