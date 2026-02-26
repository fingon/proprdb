#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2026 Markus Stenberg
#
# Last modified: Thu Feb 26 09:32:52 2026 mstenber
# Last modified: Wed Feb 25 13:32:14 2026 mstenber
# Edit time:     3 min
#
#

BINARIES=protoc-gen-proprdb

.PHONY: all
all: test $(BINARIES)

protoc-gen-proprdb: $(wildcard **/*.go)
	go build ./cmd/protoc-gen-proprdb

.PHONY: test
test:
	go test ./...
	cd test/system && go test ./...

.PHONY: lint
lint:
	go tool golangci-lint run
