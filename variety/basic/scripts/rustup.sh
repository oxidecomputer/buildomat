#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

printf ' * rust toolchain channel = "%s"\n' "$TC_CHANNEL"
printf ' * rust toolchain profile = "%s"\n' "$TC_PROFILE"

export RUSTUP_INIT_SKIP_PATH_CHECK=true

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    /bin/bash -s - -y \
    --no-modify-path \
    --default-toolchain "$TC_CHANNEL" \
    --profile "$TC_PROFILE"

rustup --version
cargo --version
rustc --version
