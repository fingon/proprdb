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
	assert.NilError(t, rt.AcknowledgeDirtyWindow(context.Background(), q, state.Token))
	_, err := crud.Person.UpdateByID(mutationPersonID, &Person{Name: testPersonNameGrace, Age: 41})
	assert.NilError(t, err)
	assert.Equal(t, mutationGeneration(t, q, PersonTableName), int64(2))
}
