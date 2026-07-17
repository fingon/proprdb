#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2026 Markus Stenberg
#
# Last modified: Fri Jul 17 09:53:07 2026 mstenber
# Last modified: Wed Feb 25 13:32:14 2026 mstenber
# Edit time:     5 min
#
#

BINARIES=protoc-gen-proprdb protoc-gen-proprdb-swift
SWIFT_ENV=HOME=/tmp SWIFTPM_MODULECACHE_OVERRIDE=/tmp/swiftpm-module-cache CLANG_MODULE_CACHE_PATH=/tmp/clang-module-cache
SWIFT_ARGS?=--disable-sandbox

.PHONY: all
all: lint verify-generated test build

protoc-gen-proprdb: $(wildcard **/*.go)
	go build ./cmd/protoc-gen-proprdb

protoc-gen-proprdb-swift: $(wildcard **/*.go)
	go build ./cmd/protoc-gen-proprdb-swift

.PHONY: protoc-gen-proprdb protoc-gen-proprdb-swift build
build: $(BINARIES) swift-build

.PHONY: generate
generate: $(BINARIES)
	protoc -I test/fixtures -I . --go_out=test/system --go_opt=paths=source_relative --plugin=protoc-gen-proprdb=./protoc-gen-proprdb --proprdb_out=paths=source_relative:test/system test/fixtures/system.proto
	mkdir -p test/swift/Sources/GeneratedSystem
	protoc -I test/fixtures -I . --plugin=protoc-gen-proprdb-swift=./protoc-gen-proprdb-swift --proprdb-swift_out=paths=source_relative:test/swift/Sources/GeneratedSystem test/fixtures/system.proto
	go test ./test -update

.PHONY: verify-generated
verify-generated:
	git diff --exit-code -- test/system/system.proprdb.pb.go test/swift/Sources/GeneratedSystem/system.proprdb.pb.swift test/testdata

.PHONY: swift-fixtures
swift-fixtures:
	go build ./cmd/protoc-gen-proprdb-swift
	mkdir -p test/swift/Sources/GeneratedSystem
	protoc -I test/fixtures -I . --plugin=protoc-gen-proprdb-swift=./protoc-gen-proprdb-swift --proprdb-swift_out=paths=source_relative:test/swift/Sources/GeneratedSystem test/fixtures/system.proto

.PHONY: swift-test
swift-test: swift-fixtures
	cd test/swift && $(SWIFT_ENV) swift test $(SWIFT_ARGS)

.PHONY: swift-build
swift-build: swift-fixtures
	cd test/swift && $(SWIFT_ENV) swift build $(SWIFT_ARGS)

.PHONY: test
test:
	go test ./...
	cd test/system && go test ./...
	$(MAKE) swift-test

.PHONY: race
race:
	go test -race ./...
	cd test/system && go test -race ./...

.PHONY: check
check: lint verify-generated test build

.PHONY: lint
lint:
	go tool golangci-lint run

.PHONY: release-minor release-patch
release-minor:
	./scripts/release minor

release-patch:
	./scripts/release patch
