#!/bin/bash

trap 'rm -f "$TMPFILE"' EXIT

set -e

TMPFILE=$(mktemp -t requirements-XXXXXX.txt)

poetry export --with dev -f requirements.txt --without-hashes -o $TMPFILE

poetry run liccheck -r $TMPFILE
