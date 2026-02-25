#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2026 Markus Stenberg
#
# Last modified: Wed Feb 25 14:56:33 2026 mstenber
# Last modified: Wed Feb 25 13:32:14 2026 mstenber
# Edit time:     2 min
#
#

.PHONY: test
test:
	go test ./...
	cd test/system && go test ./...

.PHONY: lint
lint:
	go tool golangci-lint run
