#!/bin/bash

set -o errexit
set -o pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "$root/cache"

PROGENITOR=${PROGENITOR:-$root/../progenitor/target/debug/progenitor}

NAME=buildomat
VERSION=0.0.0

sf="$root/cache/openapi.json"

cd "$root"
rm -f "$sf"
cargo run -p "$NAME-server" -- -S "$sf"
"$PROGENITOR" -i "$sf" -o "$root/openapi" -n "$NAME-openapi" -v "$VERSION"
cargo fmt
