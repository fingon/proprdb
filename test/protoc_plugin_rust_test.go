package proprdb_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/golden"
)

const rustPluginName = "protoc-gen-proprdb-rust"

func rustPlugin(t *testing.T) (repoRoot, tempDir, pluginPath string) {
	t.Helper()
	if _, err := exec.LookPath("protoc"); err != nil {
		t.Skipf("protoc not available: %v", err)
	}
	_, currentFile, _, ok := runtime.Caller(0)
	assert.Assert(t, ok)
	repoRoot = filepath.Dir(filepath.Dir(currentFile))
	tempDir = t.TempDir()
	pluginPath = filepath.Join(tempDir, rustPluginName)
	runCommand(t, repoRoot, nil, "go", "build", "-o", pluginPath, "./cmd/"+rustPluginName)
	return repoRoot, tempDir, pluginPath
}

func TestProtocRustPluginGolden(t *testing.T) {
	repoRoot, tempDir, pluginPath := rustPlugin(t)
	for _, name := range []string{"system", "rust"} {
		t.Run(name, func(t *testing.T) {
			runCommand(t, repoRoot, nil, "protoc", "-I", "test/fixtures", "-I", ".", "--plugin="+rustPluginName+"="+pluginPath, "--proprdb-rust_out=paths=source_relative:"+tempDir, "test/fixtures/"+name+".proto")
			content, err := os.ReadFile(filepath.Join(tempDir, name+".proprdb.pb.rs"))
			assert.NilError(t, err)
			golden.Assert(t, string(content), name+".proprdb.pb.rs.golden", golden.FlagUpdate())
			committed, err := os.ReadFile(filepath.Join(repoRoot, "test", "rust", "src", name+".proprdb.pb.rs"))
			assert.NilError(t, err)
			assert.Equal(t, string(content), string(committed))
		})
	}
}

func TestProtocRustPluginRejectsInvalidSchemas(t *testing.T) {
	repoRoot, tempDir, pluginPath := rustPlugin(t)
	for _, tc := range []struct{ name, body, want string }{
		{"index", `option (com.github.fingon.proprdb.indexes) = {fields: "name"}; string name = 1;`, "must be marked"},
		{"uint64", `uint64 count = 1 [(com.github.fingon.proprdb.external) = true];`, "cannot be projected"},
		{"repeated", `repeated string name = 1 [(com.github.fingon.proprdb.external) = true];`, "must be scalar"},
		{"reserved", `string id = 1 [(com.github.fingon.proprdb.external) = true];`, reservedColumnError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			protoPath := filepath.Join(tempDir, tc.name+".proto")
			content := `syntax = "proto3"; package rusttest; import "proto/proprdb/options.proto"; message Invalid {` + tc.body + `}`
			assert.NilError(t, os.WriteFile(protoPath, []byte(content), 0o644))
			output, err := runCommandCapture(tempDir, nil, "protoc", "-I", tempDir, "-I", repoRoot, "--plugin="+rustPluginName+"="+pluginPath, "--proprdb-rust_out=paths=source_relative:"+tempDir, protoPath)
			assert.Assert(t, err != nil)
			assert.Assert(t, strings.Contains(output, tc.want), output)
		})
	}
}

func TestProtocRustPluginAggregatesFiles(t *testing.T) {
	repoRoot, tempDir, pluginPath := rustPlugin(t)
	for _, files := range [][]string{{"system.proto", "rust.proto"}, {"rust.proto", "system.proto"}} {
		args := []string{"-I", filepath.Join(repoRoot, "test/fixtures"), "-I", repoRoot, "--plugin=" + rustPluginName + "=" + pluginPath, "--proprdb-rust_out=paths=source_relative:" + tempDir}
		args = append(args, files...)
		runCommand(t, tempDir, nil, "protoc", args...)
		content, err := os.ReadFile(filepath.Join(tempDir, "rust.proprdb.pb.rs"))
		assert.NilError(t, err)
		golden.Assert(t, string(content), "combined.proprdb.pb.rs.golden", golden.FlagUpdate())
	}
}
