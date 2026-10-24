package proprdb_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestProtocPluginsValidateHashProjections(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	assert.Assert(t, ok)
	root := filepath.Dir(filepath.Dir(file))
	temp := t.TempDir()
	const declaration = `option (com.github.fingon.proprdb.hash_projections) = {name: "support" fields: "nested"};`
	const fields = `Nested nested = 1; message Nested { option (com.github.fingon.proprdb.omit_table) = true; string label = 1; repeated int64 values = 2; map<string, Nested> children = 3; }`
	const tracker = `option (com.github.fingon.proprdb.dirty_keys) = {name: "support" watched_hashes: "support"};`
	for _, language := range []string{"proprdb", "proprdb-swift", "proprdb-rust"} {
		t.Run("plugin"+language, func(t *testing.T) {
			plugin := "protoc-gen-" + language
			binary := filepath.Join(temp, plugin)
			runCommand(t, root, nil, "go", "build", "-o", binary, "./cmd/"+plugin)
			for _, tc := range []struct{ name, body, want string }{
				{"complete recursive message", declaration + tracker + fields, ""},
				{"repeated", `option (com.github.fingon.proprdb.hash_projections) = {name: "support" fields: "nested.values"};` + tracker + fields, ""},
				{"map", `option (com.github.fingon.proprdb.hash_projections) = {name: "support" fields: "nested.children"};` + tracker + fields, ""},
				{"empty", `option (com.github.fingon.proprdb.hash_projections) = {name: "support"};` + fields, "requires fields"},
				{"bad name", `option (com.github.fingon.proprdb.hash_projections) = {name: "bad-name" fields: "nested"};` + fields, "name must match"},
				{"duplicate name", declaration + declaration + fields, "duplicate hash projection"},
				{"missing field", `option (com.github.fingon.proprdb.hash_projections) = {name: "support" fields: "nested.missing"};` + fields, "unknown hash field"},
				{"empty path", `option (com.github.fingon.proprdb.hash_projections) = {name: "support" fields: ""};` + fields, "unknown hash field"},
				{"repeated selection", `option (com.github.fingon.proprdb.hash_projections) = {name: "support" fields: "nested" fields: "nested"};` + fields, "overlapping hash fields"},
				{"overlap", `option (com.github.fingon.proprdb.hash_projections) = {name: "support" fields: "nested.label" fields: "nested"};` + fields, "overlapping hash fields"},
				{"traverse repeated", `option (com.github.fingon.proprdb.hash_projections) = {name: "support" fields: "nested.values.child"};` + fields, "must traverse singular messages"},
				{"traverse map", `option (com.github.fingon.proprdb.hash_projections) = {name: "support" fields: "nested.children.label"};` + fields, "must traverse singular messages"},
				{"unknown hash", tracker + fields, "unknown watched hash"},
				{"duplicate hash reference", declaration + `option (com.github.fingon.proprdb.dirty_keys) = {name: "support" watched_hashes: "support" watched_hashes: "support"};` + fields, "duplicate watched hash"},
				{"column collision", declaration + fields + `string _proprdb_hash_support = 2 [(com.github.fingon.proprdb.external) = true];`, "hash projection column collision"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					source := `syntax = "proto3"; package hashtest; import "proto/proprdb/options.proto"; option go_package = "hashtest/gen;gen"; message Record {` + tc.body + `}`
					path := filepath.Join(temp, "hash.proto")
					assert.NilError(t, os.WriteFile(path, []byte(source), 0o600))
					output, err := runCommandCapture(temp, nil, "protoc", "-I", temp, "-I", root, "--plugin="+plugin+"="+binary, "--"+language+"_out=paths=source_relative:"+temp, path)
					if tc.want == "" {
						assert.NilError(t, err, output)
						return
					}
					assert.Assert(t, err != nil)
					assert.Assert(t, strings.Contains(output, tc.want), output)
				})
			}
		})
	}
}
