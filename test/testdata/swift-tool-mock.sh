#!/bin/sh

set -eu

case "${0##*/}" in
uname)
    printf '%s\n' "$PROPRDB_TEST_OS"
    ;;
swift|swiftc)
    printf 'native'
    printf ' <%s>' "$@"
    printf '\n'
    /bin/cat
    exit "${PROPRDB_TEST_EXIT:-0}"
    ;;
podman)
    if [ "$1" = build ]; then
        printf 'image-built\n'
        exit "${PROPRDB_TEST_BUILD_EXIT:-0}"
    fi
    printf 'podman'
    printf ' <%s>' "$@"
    printf '\n'
    /bin/cat
    exit "${PROPRDB_TEST_EXIT:-0}"
    ;;
esac
