#
# Author: Markus Stenberg <fingon@iki.fi>
#
# Copyright (c) 2026 Markus Stenberg
#
# Last modified: Wed Feb 25 13:39:53 2026 mstenber
# Last modified: Wed Feb 25 13:32:14 2026 mstenber
# Edit time:     2 min
#
#

test:
	go test ./...

lint:
	go tool golangci-lint run
