package proprdb_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

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
	if err := os.MkdirAll(generatedDir, 0o755); err != nil {
		t.Fatalf("create generated dir: %v", err)
	}

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
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}

	golden.Assert(t, string(content), "system.proprdb.pb.go.golden", golden.FlagUpdate())
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
