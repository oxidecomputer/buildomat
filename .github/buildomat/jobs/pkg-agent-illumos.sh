#!/bin/bash
#:
#: name = "pkg-agent-illumos"
#: variety = "basic"
#: target = "helios-2.0-20240204"
#: rust_toolchain = true
#: output_rules = [
#:	"=/out/buildomat-agent.gz",
#:	"=/out/buildomat-agent.gz.sha256.txt",
#:	"=/out/buildomat-agent.sha256.txt",
#: ]

set -o errexit
set -o pipefail
set -o xtrace

pfexec mkdir -p /out
pfexec chown "$UID" /out

#
# Build the agent with a static OpenSSL:
#
cargo build --features vendored-openssl --release --locked -p buildomat-agent

cp target/release/buildomat-agent /out/buildomat-agent
sha256sum /out/buildomat-agent >/out/buildomat-agent.sha256.txt

gzip /out/buildomat-agent
sha256sum /out/buildomat-agent.gz >/out/buildomat-agent.gz.sha256.txt
