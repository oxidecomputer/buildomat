#!/bin/bash

dir=$(cd "$(dirname "$0")/.." && pwd)

mkdir -p "$dir/github-schema"
rm -f "$dir/github-schema/webhooks.json"
curl -sSfL -o "$dir/github-schema/webhooks.json" \
    'https://unpkg.com/@octokit/webhooks-schemas/schema.json'
