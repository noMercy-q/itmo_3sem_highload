#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$SCRIPT_DIR"

printf 'Format check\n'
UNFORMATTED_FILES=$(gofmt -l .)
if [ -n "$UNFORMATTED_FILES" ]; then
    printf 'Files should be formatted:\n%s\n' "$UNFORMATTED_FILES" >&2
    exit 1
fi

printf '\nRun tests with race detector\n'
printf 'First run with cold cache could take a while...\n'
go test -race -v -count=1 -timeout=60s ./...


printf '\nAll checks passed\n'
