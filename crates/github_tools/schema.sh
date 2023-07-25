#!/bin/bash

dir=$(cd "$(dirname "$0")/.." && pwd)

mkdir -p "$dir/schema"
rm -f "$dir/schema/webhooks.json"
curl -sSfL -o "$dir/schema/webhooks.json" \
    'https://unpkg.com/@octokit/webhooks-schemas/schema.json'
