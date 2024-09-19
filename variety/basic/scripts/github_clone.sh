#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

mkdir -p "/work/$GITHUB_REPOSITORY"
git clone "https://github.com/$GITHUB_REPOSITORY" "/work/$GITHUB_REPOSITORY"
cd "/work/$GITHUB_REPOSITORY"
git fetch origin "$GITHUB_SHA"
if [[ -n $GITHUB_BRANCH ]]; then
	current=$(git branch --show-current)
	if [[ $current != $GITHUB_BRANCH ]]; then
		git branch -f "$GITHUB_BRANCH" "$GITHUB_SHA"
		git checkout -f "$GITHUB_BRANCH"
	fi
fi
git reset --hard "$GITHUB_SHA"
