#!/bin/bash

set -o errexit
set -o pipefail

GITHUB_TOKEN=$(bmat store get GITHUB_TOKEN)

cat >$HOME/.netrc <<EOF
machine github.com
login x-access-token
password $GITHUB_TOKEN

machine api.github.com
login x-access-token
password $GITHUB_TOKEN

EOF
