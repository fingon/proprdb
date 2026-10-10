package proprdb_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

const (
	scalarValueField      = `string value = 1;`
	nestedMessageField    = "Nested nested = 1;"
	nestedValueProjection = `option (com.github.fingon.proprdb.external_paths) = "nested.value";`
	unprojectedIndexError = "must be marked"
	reservedColumnError   = "reserved column"
)

const (
	unknownPathFieldError    = "unknown field"
	repeatedPathError        = "repeated and map"
	duplicateProjectionError = "duplicate column"
)

func TestNestedProjectionValidation(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	assert.Assert(t, ok)
	repoRoot := filepath.Dir(filepath.Dir(currentFile))
	cases := []struct{ name, declaration, fields, errorText string }{
		{"id without projections", `option (com.github.fingon.proprdb.indexes) = {fields: "id"};`, "", ""},
		{"timestamp and id", `option (com.github.fingon.proprdb.indexes) = {fields: "at_ns" fields: "id"};`, "", ""},
		{"repeated id", `option (com.github.fingon.proprdb.indexes) = {fields: "id" fields: "id"};`, "", "duplicate field"},
		{"data remains unsupported", `option (com.github.fingon.proprdb.indexes) = {fields: "data"};`, "", unknownPathFieldError},
		{"timestamp without projections", `option (com.github.fingon.proprdb.indexes) = {fields: "at_ns"};`, "", ""},
		{"timestamp composite", nestedValueProjection + `option (com.github.fingon.proprdb.indexes) = {fields: "nested.value" fields: "at_ns"};`, nestedMessageField, ""},
		{"timestamp first", nestedValueProjection + `option (com.github.fingon.proprdb.indexes) = {fields: "at_ns" fields: "nested.value"};`, nestedMessageField, ""},
		{"repeated timestamp", `option (com.github.fingon.proprdb.indexes) = {fields: "at_ns" fields: "at_ns"};`, "", "duplicate field"},
		{"duplicate timestamp index", `option (com.github.fingon.proprdb.indexes) = {fields: "at_ns"}; option (com.github.fingon.proprdb.indexes) = {fields: "at_ns"};`, "", "duplicate index declaration"},
		{"timestamp case", `option (com.github.fingon.proprdb.indexes) = {fields: "AT_NS"};`, "", unknownPathFieldError},
		{"timestamp with unprojected field", `option (com.github.fingon.proprdb.indexes) = {fields: "at_ns" fields: "value"};`, scalarValueField, unprojectedIndexError},
		{"top-level scalar", `option (com.github.fingon.proprdb.external_paths) = "value";`, scalarValueField, ""},
		{"nested index", nestedValueProjection + `option (com.github.fingon.proprdb.indexes) = {fields: "nested.value"};`, nestedMessageField, ""},

		{"unknown", `option (com.github.fingon.proprdb.external_paths) = "nested.missing";`, nestedMessageField, unknownPathFieldError},
		{"empty segment", `option (com.github.fingon.proprdb.external_paths) = "nested..value";`, nestedMessageField, unknownPathFieldError},
		{"message leaf", `option (com.github.fingon.proprdb.external_paths) = "nested";`, nestedMessageField, "unsupported external field kind message"},
		{"scalar intermediate", `option (com.github.fingon.proprdb.external_paths) = "value.child";`, scalarValueField, "must be a message"},
		{"repeated intermediate", nestedValueProjection, `repeated Nested nested = 1;`, repeatedPathError},
		{"map intermediate", nestedValueProjection, `map<string, Nested> nested = 1;`, repeatedPathError},
		{"repeated leaf", `option (com.github.fingon.proprdb.external_paths) = "nested.items";`, nestedMessageField, repeatedPathError},
		{"unsupported scalar", `option (com.github.fingon.proprdb.external_paths) = "nested.large";`, nestedMessageField, "uint64 and fixed64"},
		{"duplicate path", `option (com.github.fingon.proprdb.external_paths) = "nested.value"; option (com.github.fingon.proprdb.external_paths) = "nested.value";`, nestedMessageField, duplicateProjectionError},
		{"flattening collision", nestedValueProjection, `Nested nested = 1; string nested_value = 2 [(com.github.fingon.proprdb.external) = true];`, duplicateProjectionError},
		{reservedColumnError, `option (com.github.fingon.proprdb.external_paths) = "id";`, `string id = 1;`, reservedColumnError},
		{"unprojected index", `option (com.github.fingon.proprdb.indexes) = {fields: "nested.value"};`, nestedMessageField, unknownPathFieldError},
		{"flattened alias is not a path", nestedValueProjection + `option (com.github.fingon.proprdb.indexes) = {fields: "nested_value"};`, nestedMessageField + `string nested_value = 2;`, unprojectedIndexError},
		{"case collision", nestedValueProjection, nestedMessageField + `string NESTED_VALUE = 2 [(com.github.fingon.proprdb.external) = true];`, duplicateProjectionError},
	}
	for _, target := range []string{"proprdb", "proprdb-swift", "proprdb-rust"} {
		t.Run(target, func(t *testing.T) {
			tempDir := t.TempDir()
			pluginPath := filepath.Join(tempDir, "protoc-gen-"+target)
			runCommand(t, repoRoot, nil, "go", "build", "-o", pluginPath, "./cmd/protoc-gen-"+target)
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					source := `syntax = "proto3"; package nestedtest; import "proto/proprdb/options.proto"; option go_package = "example.com/nestedtest;nestedtest"; message Record { ` + tc.declaration + tc.fields + ` } message Nested { option (com.github.fingon.proprdb.omit_table) = true; string value = 1; repeated int32 items = 2; uint64 large = 3; }`
					protoPath := filepath.Join(tempDir, "nested.proto")
					assert.NilError(t, os.WriteFile(protoPath, []byte(source), 0o600))
					output, err := runCommandCapture(tempDir, nil, "protoc", "-I", tempDir, "-I", repoRoot, "--plugin=protoc-gen-"+target+"="+pluginPath, "--"+target+"_out="+tempDir, protoPath)
					if tc.errorText == "" {
						assert.NilError(t, err, "%s", output)
						return
					}
					assert.Assert(t, err != nil)
					assert.Check(t, strings.Contains(output, tc.errorText), "%s", output)
				})
			}
		})
	}
}
