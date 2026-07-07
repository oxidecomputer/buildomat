#!/bin/bash
#:
#: name = "pkg-agent-linux"
#: variety = "basic"
#: target = "ubuntu-18.04"
#: rust_toolchain = true
#: output_rules = [
#:	"=/out/buildomat-agent-linux.gz",
#:	"=/out/buildomat-agent-linux.gz.sha256.txt",
#:	"=/out/buildomat-agent-linux.sha256.txt",
#: ]

set -o errexit
set -o pipefail
set -o xtrace

pfexec mkdir -p /out
pfexec chown "$UID" /out

#
# Install basic build tools:
#
apt-get -y update
apt-get -y install build-essential pkg-config

#
# Build the agent with a static OpenSSL:
#
cargo build --features vendored-openssl --release --locked -p buildomat-agent

cp target/release/buildomat-agent /out/buildomat-agent-linux
sha256sum /out/buildomat-agent-linux \
    >/out/buildomat-agent-linux.sha256.txt

gzip /out/buildomat-agent-linux
sha256sum /out/buildomat-agent-linux.gz \
    >/out/buildomat-agent-linux.gz.sha256.txt
