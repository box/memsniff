#!/bin/bash

set -e

GITREV=$(git -C "$GITDIR" rev-parse --short HEAD)
LDFLAGS="-X \"main.GitRevision=$GITREV\""

go build -ldflags "$LDFLAGS" -o "$EXECUTABLE" "$PACKAGE"
