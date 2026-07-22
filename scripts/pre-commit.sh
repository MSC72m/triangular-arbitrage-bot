#!/bin/bash
# Pre-commit hook: runs fmt, vet, and lint on staged Go files.
# Install: ln -sf ../../scripts/pre-commit.sh .git/hooks/pre-commit

set -euo pipefail

STAGED_GO=$(git diff --cached --name-only --diff-filter=ACM | grep '\.go$' || true)

if [ -z "$STAGED_GO" ]; then
  exit 0
fi

echo "--- Pre-commit: gofmt ---"
gofmt -s -w $STAGED_GO
git add $STAGED_GO

echo "--- Pre-commit: go vet ---"
go vet ./...

if command -v golangci-lint >/dev/null 2>&1; then
  echo "--- Pre-commit: golangci-lint ---"
  golangci-lint run
else
  echo "  (skip lint: golangci-lint not installed)"
fi
