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

BINARIES=protoc-gen-proprdb protoc-gen-proprdb-swift protoc-gen-proprdb-rust
SWIFT_ENV=HOME=/tmp SWIFTPM_MODULECACHE_OVERRIDE=/tmp/swiftpm-module-cache CLANG_MODULE_CACHE_PATH=/tmp/clang-module-cache
SWIFT_ARGS?=--disable-sandbox
PROTOC_GEN_SWIFT=test/swift/.build/checkouts/swift-protobuf/.build/debug/protoc-gen-swift

.PHONY: all
all: lint verify-generated test build

protoc-gen-proprdb: $(wildcard **/*.go)
	go build ./cmd/protoc-gen-proprdb

protoc-gen-proprdb-swift: $(wildcard **/*.go)
	go build ./cmd/protoc-gen-proprdb-swift

protoc-gen-proprdb-rust: $(wildcard **/*.go)
	go build ./cmd/protoc-gen-proprdb-rust

.PHONY: protoc-gen-swift
protoc-gen-swift: test/swift/Package.resolved
	cd test/swift && $(SWIFT_ENV) swift package resolve $(SWIFT_ARGS)
	$(SWIFT_ENV) swift build --package-path test/swift/.build/checkouts/swift-protobuf --product protoc-gen-swift $(SWIFT_ARGS)

.PHONY: protoc-gen-proprdb protoc-gen-proprdb-swift protoc-gen-proprdb-rust build
build: $(BINARIES) swift-build rust-build

.PHONY: generate
generate: $(BINARIES) protoc-gen-swift
	protoc -I test/fixtures -I . --go_out=test/system --go_opt=paths=source_relative --plugin=protoc-gen-proprdb=./protoc-gen-proprdb --proprdb_out=paths=source_relative:test/system test/fixtures/system.proto
	mkdir -p test/swift/Sources/GeneratedSystem
	protoc -I test/fixtures -I . --plugin=protoc-gen-swift=$(PROTOC_GEN_SWIFT) --swift_out=Visibility=Public:test/swift/Sources/GeneratedSystem test/fixtures/system.proto
	protoc -I test/fixtures -I . --plugin=protoc-gen-proprdb-swift=./protoc-gen-proprdb-swift --proprdb-swift_out=Visibility=Public,paths=source_relative:test/swift/Sources/GeneratedSystem test/fixtures/system.proto
	$(MAKE) rust-fixtures
	go test ./test -update

.PHONY: verify-generated
verify-generated:
	go test ./test -run 'TestProtoc(Plugin|SwiftPlugin|RustPlugin)Golden'

.PHONY: swift-fixtures
swift-fixtures: protoc-gen-swift
	go build ./cmd/protoc-gen-proprdb-swift
	mkdir -p test/swift/Sources/GeneratedSystem
	protoc -I test/fixtures -I . --plugin=protoc-gen-swift=$(PROTOC_GEN_SWIFT) --swift_out=Visibility=Public:test/swift/Sources/GeneratedSystem test/fixtures/system.proto
	protoc -I test/fixtures -I . --plugin=protoc-gen-proprdb-swift=./protoc-gen-proprdb-swift --proprdb-swift_out=Visibility=Public,paths=source_relative:test/swift/Sources/GeneratedSystem test/fixtures/system.proto

.PHONY: swift-test
swift-test: swift-fixtures
	cd test/swift && $(SWIFT_ENV) swift test $(SWIFT_ARGS)

.PHONY: swift-build
swift-build: swift-fixtures
	cd test/swift && $(SWIFT_ENV) swift build $(SWIFT_ARGS)

.PHONY: rust-fixtures rust-test rust-build rust-lint
rust-fixtures: protoc-gen-proprdb-rust
	protoc -I test/fixtures -I . --plugin=protoc-gen-proprdb-rust=./protoc-gen-proprdb-rust --proprdb-rust_out=paths=source_relative:test/rust/src test/fixtures/system.proto
	protoc -I test/fixtures -I . --plugin=protoc-gen-proprdb-rust=./protoc-gen-proprdb-rust --proprdb-rust_out=paths=source_relative:test/rust/src test/fixtures/rust.proto

rust-test: rust-fixtures
	cargo test --workspace --locked

rust-build: rust-fixtures
	cargo build --workspace --locked

rust-lint:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --locked -- -D warnings

.PHONY: test
test:
	go test ./...
	cd test/system && go test ./...
	$(MAKE) swift-test
	$(MAKE) rust-test

.PHONY: race
race:
	go test -race ./...
	cd test/system && go test -race ./...

.PHONY: check
check: lint verify-generated test build

.PHONY: lint
lint: rust-lint
	go tool golangci-lint run

.PHONY: release-minor release-patch
release-minor:
	./scripts/release minor

release-patch:
	./scripts/release patch
