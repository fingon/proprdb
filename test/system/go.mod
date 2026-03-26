module github.com/fingon/proprdb/test/system

go 1.25.6

require (
	github.com/fingon/proprdb v0.0.0
	github.com/mattn/go-sqlite3 v1.14.32
	google.golang.org/protobuf v1.36.11
	gotest.tools/v3 v3.5.2
)

require github.com/google/go-cmp v0.7.0 // indirect

replace github.com/fingon/proprdb => ../..
