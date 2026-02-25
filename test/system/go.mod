module github.com/fingon/proprdb/test/system

go 1.25.0

require (
	github.com/fingon/proprdb v0.0.0
	github.com/mattn/go-sqlite3 v1.14.32
	google.golang.org/protobuf v1.36.8
)

replace github.com/fingon/proprdb => ../..
