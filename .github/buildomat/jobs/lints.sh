#!/bin/bash
#:
#: name = "lints"
#: variety = "basic"
#: target = "helios-2.0"
#: rust_toolchain = true
#:

set -o errexit
set -o pipefail
set -o xtrace

banner fmt
cargo fmt --check
