#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$SCRIPT_DIR"

UNFORMATTED_FILES=$(gofmt -l .)
if [ -n "$UNFORMATTED_FILES" ]; then
    printf 'Run gofmt on:\n%s\n' "$UNFORMATTED_FILES" >&2
    exit 1
fi

CACHE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/hw2-tests.XXXXXX")
trap 'rm -rf "$CACHE_DIR"' EXIT HUP INT TERM
export GOCACHE="$CACHE_DIR/go-build"
export GOMODCACHE="$CACHE_DIR/go-mod"
export GOPATH="$CACHE_DIR/go-path"

go test -race ./...
go vet ./...
