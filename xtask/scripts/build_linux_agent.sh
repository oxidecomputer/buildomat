#!/bin/bash

#
# This job script is run inside a buildomat ephemeral VM.
#
if [[ -z $BUILDOMAT_JOB_ID ]]; then
	printf 'ERROR: this is supposed to be run under buildomat.\n' >&2
	exit 1
fi

set -o errexit
set -o pipefail
set -o xtrace

#
# Install basic build tools:
#
apt-get -y update
apt-get -y install build-essential pkg-config

#
# Install a stable Rust toolchain:
#
RUSTUP_INIT_SKIP_PATH_CHECK=yes \
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s - \
    --default-toolchain stable \
    --profile minimal \
    --no-modify-path \
    -y -q

. "$HOME/.cargo/env"

mkdir -p /work
mkdir -p /out

cd /work

#
# Unpack the templates and scripts we included when kicking off the job:
#
cpio -idv < '/input/src.cpio'

cargo build --features vendored-openssl --release --locked -p buildomat-agent

#
# Copy rather than moving, because we're on the same file system and gzip
# complains about a link count issue otherwise:
#
cp target/release/buildomat-agent /out/buildomat-agent-linux
chmod 0755 /out/buildomat-agent-linux

sha256sum /out/buildomat-agent-linux \
    >/out/buildomat-agent-linux.sha256.txt
gzip /out/buildomat-agent-linux
sha256sum /out/buildomat-agent-linux.gz \
    >/out/buildomat-agent-linux.gz.sha256.txt

find '/out' -type f -ls
