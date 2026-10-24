package proprdbrt

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"gotest.tools/v3/assert"
)

func TestHashProjectionVectors(t *testing.T) {
	data, err := os.ReadFile("../test/testdata/hash-projection-vectors.json")
	assert.NilError(t, err)
	var vectors []struct{ Name, Schema, Wire, Canonical, Hash string }
	assert.NilError(t, json.Unmarshal(data, &vectors))
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			wire, err := hex.DecodeString(v.Wire)
			assert.NilError(t, err)
			expected, err := hex.DecodeString(v.Canonical)
			assert.NilError(t, err)
			actual, err := CanonicalHashBytes(wire, v.Schema)
			assert.NilError(t, err)
			assert.DeepEqual(t, actual, expected)
			assert.Equal(t, hex.EncodeToString(binary.BigEndian.AppendUint64(nil, xxh3.Hash(actual))), v.Hash)
			message := &emptypb.Empty{}
			assert.NilError(t, proto.Unmarshal(wire, message))
			fingerprint, err := ProjectHash(message, v.Schema)
			assert.NilError(t, err)
			assert.Equal(t, hex.EncodeToString(fingerprint), v.Hash)
		})
	}
}

func TestHashProjectionMalformedInputs(t *testing.T) {
	const schema = `{"p":[{"p":[1],"f":[1,5,0,-1]}],"m":[]}`
	for _, tc := range []struct {
		name, schema string
		wire         []byte
	}{
		{"schema", `{`, nil},
		{"empty", `{}`, nil},
		{"path", `{"p":[{"p":[],"f":[1,5,0,-1]}]}`, nil},
		{"message reference", `{"p":[{"p":[1],"f":[1,11,1,0]}],"m":[]}`, nil},
		{"truncated tag", schema, []byte{128}},
		{"truncated value", schema, []byte{8, 128}},
		{"wrong wire", schema, []byte{10, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) { _, err := CanonicalHashBytes(tc.wire, tc.schema); assert.Assert(t, err != nil) })
	}
}
