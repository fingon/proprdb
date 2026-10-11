package proprdb_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestProtocPluginsValidateDirtyWindows(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	assert.Assert(t, ok)
	root := filepath.Dir(filepath.Dir(currentFile))
	temp := t.TempDir()
	const projectedAge = `int64 age = 1 [(com.github.fingon.proprdb.external) = true];`
	const declaration = `option (com.github.fingon.proprdb.dirty_windows) = {name: "age" start_field: "age"};`
	for _, language := range []string{"", "-swift", "-rust"} {
		t.Run("plugin"+language, func(t *testing.T) {
			plugin := "protoc-gen-proprdb" + language
			binary := filepath.Join(temp, plugin)
			runCommand(t, root, nil, "go", "build", "-o", binary, "./cmd/"+plugin)
			for _, tc := range []struct{ name, body, want string }{
				{"point", declaration + projectedAge, ""},
				{"unprojected", declaration + "int64 age = 1;", "must already be projected"},
				{"unsigned", declaration + `uint32 age = 1 [(com.github.fingon.proprdb.external) = true];`, "must be a signed integer"},
				{"real", declaration + `double age = 1 [(com.github.fingon.proprdb.external) = true];`, "must be a signed integer"},
				{"duplicate", declaration + declaration + projectedAge, "duplicate dirty window"},
				{"name", `option (com.github.fingon.proprdb.dirty_windows) = {name: "bad-name" start_field: "age"};` + projectedAge, "dirty window name must match"},
				{"missing end", `option (com.github.fingon.proprdb.dirty_windows) = {name: "age" start_field: "age" end_field: "missing"};` + projectedAge, "must already be projected"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					file := filepath.Join(temp, "dirty.proto")
					content := `syntax = "proto3"; package dirtytest; import "proto/proprdb/options.proto"; option go_package = "dirtytest/gen;gen"; message Record {` + tc.body + "}"
					assert.NilError(t, os.WriteFile(file, []byte(content), 0o644))
					output, err := runCommandCapture(temp, nil, "protoc", "-I", temp, "-I", root, "--plugin="+plugin+"="+binary, "--proprdb"+language+"_out=paths=source_relative:"+temp, file)
					if tc.want == "" {
						assert.NilError(t, err)
						return
					}
					assert.Assert(t, err != nil)
					assert.Assert(t, strings.Contains(output, tc.want), output)
				})
			}
		})
	}
}
