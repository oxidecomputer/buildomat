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
# Make sure we can locate the C compiler:
#
export PATH=/usr/bin:/usr/sbin:/sbin:/opt/ooce/sbin:/opt/ooce/bin

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
cp target/release/buildomat-agent /out/buildomat-agent
chmod 0755 /out/buildomat-agent

sha256sum /out/buildomat-agent \
    >/out/buildomat-agent.sha256.txt
gzip /out/buildomat-agent
sha256sum /out/buildomat-agent.gz \
    >/out/buildomat-agent.gz.sha256.txt

find '/out' -type f -ls
