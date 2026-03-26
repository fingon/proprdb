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

BINARIES=protoc-gen-proprdb protoc-gen-proprdb-swift
SWIFT_ENV=HOME=/tmp SWIFTPM_MODULECACHE_OVERRIDE=/tmp/swiftpm-module-cache CLANG_MODULE_CACHE_PATH=/tmp/clang-module-cache

.PHONY: all
all: test build

protoc-gen-proprdb: $(wildcard **/*.go)
	go build ./cmd/protoc-gen-proprdb

protoc-gen-proprdb-swift: $(wildcard **/*.go)
	go build ./cmd/protoc-gen-proprdb-swift

.PHONY: build
build: $(BINARIES) swift-build

.PHONY: swift-fixtures
swift-fixtures:
	go build ./cmd/protoc-gen-proprdb-swift
	mkdir -p test/swift/Sources/GeneratedSystem
	protoc -I test/fixtures -I . --swift_out=test/swift/Sources/GeneratedSystem --plugin=protoc-gen-proprdb-swift=./protoc-gen-proprdb-swift --proprdb-swift_out=paths=source_relative:test/swift/Sources/GeneratedSystem test/fixtures/system.proto

.PHONY: swift-test
swift-test: swift-fixtures
	cd test/swift && $(SWIFT_ENV) swift test

.PHONY: swift-build
swift-build: swift-fixtures
	cd test/swift && $(SWIFT_ENV) swift build

.PHONY: test
test:
	go test ./...
	cd test/system && go test ./...
	$(MAKE) swift-test

.PHONY: lint
lint:
	go tool golangci-lint run
