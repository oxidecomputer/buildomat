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
sed -i \
    -e 's/: &String/: \&str/' \
    -e 's/: &Option/: Option/g' \
    -e 's/&u32/u32/g' \
    -e 's/&i32/i32/g' \
    -e 's/&u64/u64/g' \
    -e 's/&i64/i64/g' \
    openapi/src/lib.rs
