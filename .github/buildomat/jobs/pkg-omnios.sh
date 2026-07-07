#!/bin/bash
#:
#: name = "pkg-omnios"
#: variety = "basic"
#: target = "omnios-r151046"
#: rust_toolchain = true
#: output_rules = [
#: ]
#:
#: [dependencies.illumos]
#: job = "pkg-agent-illumos"
#:
#: [dependencies.linux]
#: job = "pkg-agent-linux"

set -o errexit
set -o pipefail
set -o xtrace

pfexec mkdir -p /out
pfexec chown "$UID" /out

#
# Build all services:
#
cargo build --release --locked

#
# Produce pkg(7) packages containing the server software, and the agent binaries
# that have been built by other jobs for our various target platforms.
#
cd pkg
gmake \
    INPUT_AGENT_ILLUMOS=/input/illumos/out/buildomat-agent \
    INPUT_AGENT_LINUX=/input/linux/out/buildomat-agent-linux \
    ARCHIVE=/out/buildomat.p5p \
    archive
sha256sum /out/buildomat.p5p >/out/buildomat.p5p.sha256.txt
