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

func TestProtocSwiftPluginGolden(t *testing.T) {
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
	pluginPath := filepath.Join(tempDir, "protoc-gen-proprdb-swift")
	generatedDir := filepath.Join(tempDir, "gen")
	err := os.MkdirAll(generatedDir, 0o755)
	assert.NilError(t, err)

	runCommand(t, repoRoot, nil, "go", "build", "-o", pluginPath, "./cmd/protoc-gen-proprdb-swift")

	protoDir := filepath.Join(repoRoot, "test", "fixtures")
	protoFile := filepath.Join(protoDir, "system.proto")
	runCommand(
		t,
		tempDir,
		nil,
		"protoc",
		"-I", protoDir,
		"-I", repoRoot,
		"--plugin=protoc-gen-proprdb-swift="+pluginPath,
		"--proprdb-swift_out=paths=source_relative:"+generatedDir,
		protoFile,
	)

	generatedFile := filepath.Join(generatedDir, "system.proprdb.pb.swift")
	content, err := os.ReadFile(generatedFile)
	assert.NilError(t, err)

	golden.Assert(t, string(content), "system.proprdb.pb.swift.golden", golden.FlagUpdate())
}

func TestProtocSwiftPluginRejectsNonExternalIndexField(t *testing.T) {
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
	pluginPath := filepath.Join(tempDir, "protoc-gen-proprdb-swift")
	generatedDir := filepath.Join(tempDir, "gen")
	err := os.MkdirAll(generatedDir, 0o755)
	assert.NilError(t, err)

	runCommand(t, repoRoot, nil, "go", "build", "-o", pluginPath, "./cmd/protoc-gen-proprdb-swift")

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
		"--plugin=protoc-gen-proprdb-swift="+pluginPath,
		"--proprdb-swift_out=paths=source_relative:"+generatedDir,
		badProtoPath,
	)
	assert.Check(t, runErr != nil)
	assert.Check(t, strings.Contains(output, "must be marked (com.github.fingon.proprdb.external)=true"))
}

func TestProtocSwiftPluginSupportsProto3OptionalExternal(t *testing.T) {
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
	pluginPath := filepath.Join(tempDir, "protoc-gen-proprdb-swift")
	generatedDir := filepath.Join(tempDir, "gen")
	err := os.MkdirAll(generatedDir, 0o755)
	assert.NilError(t, err)

	runCommand(t, repoRoot, nil, "go", "build", "-o", pluginPath, "./cmd/protoc-gen-proprdb-swift")

	protoPath := filepath.Join(tempDir, "optional.proto")
	protoContent := `syntax = "proto3";
package generatedtest.optional;
import "proto/proprdb/options.proto";
option go_package = "generatedtest/optional;optional";
message Person {
  optional string nick = 1 [(com.github.fingon.proprdb.external) = true];
  int64 age = 2 [(com.github.fingon.proprdb.external) = true];
}`
	err = os.WriteFile(protoPath, []byte(protoContent), 0o644)
	assert.NilError(t, err)

	runCommand(
		t,
		tempDir,
		nil,
		"protoc",
		"-I", tempDir,
		"-I", repoRoot,
		"--plugin=protoc-gen-proprdb-swift="+pluginPath,
		"--proprdb-swift_out=paths=source_relative:"+generatedDir,
		protoPath,
	)

	generatedPath := filepath.Join(generatedDir, "optional.proprdb.pb.swift")
	generatedContent, err := os.ReadFile(generatedPath)
	assert.NilError(t, err)

	generatedText := string(generatedContent)
	assert.Check(t, strings.Contains(generatedText, `\"nick\" TEXT`))
	assert.Check(t, strings.Contains(generatedText, `\"age\" INTEGER NOT NULL DEFAULT 0`))
	assert.Check(t, strings.Contains(generatedText, `let PersonProjectionSchema = "nick:string:optional;age:int64"`))
	assert.Check(t, strings.Contains(generatedText, `if data.hasNick {`))
	assert.Check(t, strings.Contains(generatedText, `values.append(.null)`))
}

func TestProtocSwiftPluginUsesSwiftProtobufNames(t *testing.T) {
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
	pluginPath := filepath.Join(tempDir, "protoc-gen-proprdb-swift")
	generatedDir := filepath.Join(tempDir, "gen")
	assert.NilError(t, os.MkdirAll(generatedDir, 0o755))
	runCommand(t, repoRoot, nil, "go", "build", "-o", pluginPath, "./cmd/protoc-gen-proprdb-swift")

	protoPath := filepath.Join(tempDir, "names.proto")
	protoContent := `syntax = "proto3";
package generated_test.names;
import "proto/proprdb/options.proto";
option go_package = "generatedtest/names;names";
option swift_prefix = "ACME";
message Outer {
  message Inner {
    string description = 1 [(com.github.fingon.proprdb.external) = true];
  }
}`
	assert.NilError(t, os.WriteFile(protoPath, []byte(protoContent), 0o644))
	runCommand(
		t,
		tempDir,
		nil,
		"protoc",
		"-I", tempDir,
		"-I", repoRoot,
		"--plugin=protoc-gen-proprdb-swift="+pluginPath,
		"--proprdb-swift_out=paths=source_relative:"+generatedDir,
		protoPath,
	)

	generatedContent, err := os.ReadFile(filepath.Join(generatedDir, "names.proprdb.pb.swift"))
	assert.NilError(t, err)
	generatedText := string(generatedContent)
	assert.Check(t, strings.Contains(generatedText, `messageType: ACMEOuter.Inner.self`))
	assert.Check(t, strings.Contains(generatedText, `sqliteBindValue(data.description_p)`))
}

func TestProtocSwiftPluginSupportsPublicSynchronousAPI(t *testing.T) {
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
	pluginPath := filepath.Join(tempDir, "protoc-gen-proprdb-swift")
	generatedDir := filepath.Join(tempDir, "gen")
	err := os.MkdirAll(generatedDir, 0o755)
	assert.NilError(t, err)

	runCommand(t, repoRoot, nil, "go", "build", "-o", pluginPath, "./cmd/protoc-gen-proprdb-swift")

	protoDir := filepath.Join(repoRoot, "test", "fixtures")
	protoFile := filepath.Join(protoDir, "system.proto")
	runCommand(
		t,
		tempDir,
		nil,
		"protoc",
		"-I", protoDir,
		"-I", repoRoot,
		"--plugin=protoc-gen-proprdb-swift="+pluginPath,
		"--proprdb-swift_out=Visibility=Public,PublicSynchronousAPI=true,paths=source_relative:"+generatedDir,
		protoFile,
	)

	generatedFile := filepath.Join(generatedDir, "system.proprdb.pb.swift")
	content, err := os.ReadFile(generatedFile)
	assert.NilError(t, err)

	generatedText := string(content)
	assert.Check(t, strings.Contains(generatedText, `public let PersonTableName = "generatedtest_example_person"`))
	assert.Check(t, strings.Contains(generatedText, `public struct PersonRow: Equatable, Sendable {`))
	assert.Check(t, strings.Contains(generatedText, `public struct PersonTable {`))
	assert.Check(t, strings.Contains(generatedText, `public func initialize() throws {`))
	assert.Check(t, strings.Contains(generatedText, `public struct CRUD {`))
	assert.Check(t, strings.Contains(generatedText, `public struct PersonTableProxy: Sendable {`))
	assert.Check(t, strings.Contains(generatedText, `public func readJSONL(remote: String, text: String) throws {`))
}
