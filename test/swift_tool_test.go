package proprdb_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

const (
	swiftToolExecutable      = "swift"
	swiftToolCompiler        = "swiftc"
	swiftToolMacOS           = "Darwin"
	swiftToolLinux           = "Linux"
	swiftToolPlugin          = "protoc-gen-swift"
	swiftToolNativeOutput    = "native <build>"
	swiftToolContainerOutput = "podman <run>"
)

func TestSwiftToolWrapper(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	assert.Assert(t, ok)
	root := filepath.Dir(filepath.Dir(currentFile))
	mock, err := os.ReadFile(filepath.Join(root, "test", "testdata", "swift-tool-mock.sh"))
	assert.NilError(t, err)
	const sampleArgument = "argument with spaces"
	const payload = "protobuf-input\n"
	for _, tc := range []struct {
		name, platform, tool          string
		native, podman                bool
		buildExit, toolExit, wantExit int
		want                          string
	}{
		{name: "macOS native", platform: swiftToolMacOS, tool: swiftToolExecutable, native: true, podman: true, want: swiftToolNativeOutput},
		{name: "Linux native preferred", platform: swiftToolLinux, tool: swiftToolExecutable, native: true, podman: true, want: swiftToolNativeOutput},
		{name: "native compiler", platform: swiftToolLinux, tool: swiftToolCompiler, native: true, want: swiftToolNativeOutput},
		{name: "Linux fallback", platform: swiftToolLinux, tool: swiftToolExecutable, podman: true, want: swiftToolContainerOutput},
		{name: "container plugin", platform: swiftToolLinux, tool: swiftToolPlugin, podman: true, want: swiftToolContainerOutput},
		{name: "missing tools", platform: swiftToolLinux, tool: swiftToolExecutable, wantExit: 1, want: "Install Swift or Podman"},
		{name: "macOS never containerizes", platform: swiftToolMacOS, tool: swiftToolExecutable, podman: true, wantExit: 127, want: "swift: not found"},
		{name: "unsupported platform", platform: "Other", tool: swiftToolExecutable, wantExit: 1, want: "supported on macOS and Linux"},
		{name: "unsupported tool", platform: swiftToolLinux, tool: "other", wantExit: 2, want: "Unsupported Swift tool"},
		{name: "native failure", platform: swiftToolLinux, tool: swiftToolExecutable, native: true, toolExit: 13, wantExit: 13, want: swiftToolNativeOutput},
		{name: "container build failure", platform: swiftToolLinux, tool: swiftToolExecutable, podman: true, buildExit: 17, wantExit: 17, want: "image-built"},
		{name: "container execution failure", platform: swiftToolLinux, tool: swiftToolExecutable, podman: true, toolExit: 23, wantExit: 23, want: swiftToolContainerOutput},
	} {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			for _, tool := range []string{"dirname", "cksum"} {
				path, err := exec.LookPath(tool)
				assert.NilError(t, err)
				assert.NilError(t, os.Symlink(path, filepath.Join(directory, tool)))
			}
			tools := []string{"uname"}
			if tc.native {
				tools = append(tools, swiftToolExecutable, swiftToolCompiler)
			}
			if tc.podman {
				tools = append(tools, "podman")
			}
			for _, tool := range tools {
				assert.NilError(t, os.WriteFile(filepath.Join(directory, tool), mock, 0o700))
			}
			args := []string{tc.tool, "build", sampleArgument}
			wrapper := filepath.Join(root, "scripts", "swift-tool")
			if tc.tool == swiftToolPlugin {
				wrapper = filepath.Join(root, "scripts", swiftToolPlugin)
				args = args[1:]
			}
			command := exec.Command(wrapper, args...)
			command.Dir = root
			command.Env = append(os.Environ(), "PATH="+directory, "PROPRDB_TEST_OS="+tc.platform, "PROPRDB_TEST_EXIT="+strconv.Itoa(tc.toolExit), "PROPRDB_TEST_BUILD_EXIT="+strconv.Itoa(tc.buildExit))
			command.Stdin = strings.NewReader(payload)
			var stdout, stderr bytes.Buffer
			command.Stdout = &stdout
			command.Stderr = &stderr
			err := command.Run()
			if tc.wantExit == 0 {
				assert.NilError(t, err, stderr.String())
			} else {
				var exitError *exec.ExitError
				assert.Assert(t, errors.As(err, &exitError))
				assert.Equal(t, exitError.ExitCode(), tc.wantExit)
			}
			assert.Assert(t, strings.Contains(stdout.String()+stderr.String(), tc.want), "stdout=%s stderr=%s", stdout.String(), stderr.String())
			if tc.wantExit == 0 || tc.toolExit != 0 {
				assert.Assert(t, strings.Contains(stdout.String(), "<"+sampleArgument+">"))
				assert.Assert(t, strings.HasSuffix(stdout.String(), payload))
			}
			if tc.podman && !tc.native && tc.platform == swiftToolLinux && tc.buildExit == 0 && tc.tool != "other" {
				assert.Assert(t, !strings.Contains(stdout.String(), "image-built"))
				assert.Assert(t, strings.Contains(stdout.String(), "<--interactive>"))
				assert.Assert(t, strings.Contains(stdout.String(), "<--workdir> <"+root+">"))
				if tc.tool == swiftToolPlugin {
					assert.Assert(t, strings.Contains(stdout.String(), "/checkouts/swift-protobuf/.build/debug/protoc-gen-swift>"))
				}
			}
		})
	}
}
