#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

banner clippy
cargo clippy --locked --tests --workspace -- -Dwarnings

banner test
cargo test --locked --workspace
