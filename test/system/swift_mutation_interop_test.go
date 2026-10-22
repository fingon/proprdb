package genexample

import (
	"context"
	"os"
	"testing"

	rt "github.com/fingon/proprdb/rt"
	"gotest.tools/v3/assert"
)

func TestSwiftMutationInterop(t *testing.T) {
	path := os.Getenv("PROPRDB_SWIFT_MUTATION_DB")
	if path == "" {
		t.Skip("invoked by the Swift mutation integration test")
	}
	_, q, crud := mutationDatabase(t, path)
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(1))
	state := readAgeWindow(t, q, mutationConsumer)
	assert.Equal(t, state.Lower.Int64, int64(10))
	assert.Equal(t, state.Upper.Int64, int64(10))
	assert.DeepEqual(t, state.Intervals, []rt.DirtyInterval{{Lower: 10, Upper: 10}})
	assert.NilError(t, rt.AcknowledgeDirtyWindow(context.Background(), q, state.Token))
	keys := keyState(t, q, keysAge, mutationConsumer)
	assert.Assert(t, keys.Dirty && !keys.Full)
	assert.DeepEqual(t, keys.IDs, []string{mutationPersonID})
	assert.NilError(t, rt.AcknowledgeDirtyKeys(context.Background(), q, keys.Token))
	_, err := crud.Person.UpdateByID(mutationPersonID, &Person{Name: testPersonNameGrace, Age: 41})
	assert.NilError(t, err)
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(2))
}

func TestRustMutationInterop(t *testing.T) {
	path := os.Getenv("PROPRDB_RUST_MUTATION_DB")
	if path == "" {
		t.Skip("invoked by the Rust mutation integration test")
	}
	_, q, crud := mutationDatabase(t, path)
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(1))
	state := readAgeWindow(t, q, mutationConsumer)
	assert.DeepEqual(t, state.Intervals, []rt.DirtyInterval{{Lower: 10, Upper: 10}})
	assert.NilError(t, rt.AcknowledgeDirtyWindow(context.Background(), q, state.Token))
	_, err := crud.Person.UpdateByID(mutationPersonID, &Person{Name: testPersonNameGrace, Age: 41})
	assert.NilError(t, err)
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(2))
}
