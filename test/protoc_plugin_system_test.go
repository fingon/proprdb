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

func TestProtocPluginGolden(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("protoc"); err != nil {
		t.Skipf("protoc not available: %v", err)
	}

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("determine current file path")
	}
	repoRoot := filepath.Dir(filepath.Dir(currentFile))

	tempDir := t.TempDir()
	pluginPath := filepath.Join(tempDir, "protoc-gen-proprdb")
	generatedDir := filepath.Join(tempDir, "gen")
	err := os.MkdirAll(generatedDir, 0o755)
	assert.NilError(t, err)

	runCommand(t, repoRoot, nil, "go", "build", "-o", pluginPath, "./cmd/protoc-gen-proprdb")

	protoDir := filepath.Join(repoRoot, "test", "fixtures")
	protoFile := filepath.Join(protoDir, "system.proto")
	runCommand(
		t,
		tempDir,
		nil,
		"protoc",
		"-I", protoDir,
		"-I", repoRoot,
		"--plugin=protoc-gen-proprdb="+pluginPath,
		"--proprdb_out=paths=source_relative:"+generatedDir,
		protoFile,
	)

	generatedFile := filepath.Join(generatedDir, "system.proprdb.pb.go")
	content, err := os.ReadFile(generatedFile)
	assert.NilError(t, err)

	golden.Assert(t, string(content), "system.proprdb.pb.go.golden", golden.FlagUpdate())
}

func TestProtocPluginRejectsNonExternalIndexField(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("protoc"); err != nil {
		t.Skipf("protoc not available: %v", err)
	}

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("determine current file path")
	}
	repoRoot := filepath.Dir(filepath.Dir(currentFile))

	tempDir := t.TempDir()
	pluginPath := filepath.Join(tempDir, "protoc-gen-proprdb")
	generatedDir := filepath.Join(tempDir, "gen")
	err := os.MkdirAll(generatedDir, 0o755)
	assert.NilError(t, err)

	runCommand(t, repoRoot, nil, "go", "build", "-o", pluginPath, "./cmd/protoc-gen-proprdb")

	badProtoPath := filepath.Join(tempDir, "bad.proto")
	badProto := `syntax = "proto3";
package generatedtest.bad;
import "proto/proprdb/options.proto";
option go_package = "generatedtest/bad;bad";
message Person {
  option (com.github.fingon.proprdb.indexes) = {fields: "name"};
  string name = 1;
}`
	err = os.WriteFile(badProtoPath, []byte(badProto), 0o644)
	assert.NilError(t, err)

	output, runErr := runCommandCapture(tempDir, nil, "protoc",
		"-I", tempDir,
		"-I", repoRoot,
		"--plugin=protoc-gen-proprdb="+pluginPath,
		"--proprdb_out=paths=source_relative:"+generatedDir,
		badProtoPath,
	)
	assert.Check(t, runErr != nil)
	assert.Check(t, strings.Contains(output, "must be marked (com.github.fingon.proprdb.external)=true"))
}

func TestProtocPluginRejectsEmptyIndex(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("protoc"); err != nil {
		t.Skipf("protoc not available: %v", err)
	}

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("determine current file path")
	}
	repoRoot := filepath.Dir(filepath.Dir(currentFile))

	tempDir := t.TempDir()
	pluginPath := filepath.Join(tempDir, "protoc-gen-proprdb")
	generatedDir := filepath.Join(tempDir, "gen")
	err := os.MkdirAll(generatedDir, 0o755)
	assert.NilError(t, err)

	runCommand(t, repoRoot, nil, "go", "build", "-o", pluginPath, "./cmd/protoc-gen-proprdb")

	badProtoPath := filepath.Join(tempDir, "bad.proto")
	badProto := `syntax = "proto3";
package generatedtest.bad;
import "proto/proprdb/options.proto";
option go_package = "generatedtest/bad;bad";
message Person {
  option (com.github.fingon.proprdb.indexes) = {};
  string name = 1 [(com.github.fingon.proprdb.external) = true];
}`
	err = os.WriteFile(badProtoPath, []byte(badProto), 0o644)
	assert.NilError(t, err)

	output, runErr := runCommandCapture(tempDir, nil, "protoc",
		"-I", tempDir,
		"-I", repoRoot,
		"--plugin=protoc-gen-proprdb="+pluginPath,
		"--proprdb_out=paths=source_relative:"+generatedDir,
		badProtoPath,
	)
	assert.Check(t, runErr != nil)
	assert.Check(t, strings.Contains(output, "must include at least one field"))
}

func runCommand(t *testing.T, workDir string, extraEnv []string, name string, args ...string) {
	t.Helper()

	command := exec.Command(name, args...)
	command.Dir = workDir
	command.Env = append(os.Environ(), extraEnv...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("command failed: %s %s\n%s", name, strings.Join(args, " "), string(output))
	}
}

func runCommandCapture(workDir string, extraEnv []string, name string, args ...string) (string, error) {
	command := exec.Command(name, args...)
	command.Dir = workDir
	command.Env = append(os.Environ(), extraEnv...)
	output, err := command.CombinedOutput()
	return string(output), err
}
