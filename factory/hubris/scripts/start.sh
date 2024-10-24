#!/usr/bin/env bash

set -o errexit
set -o pipefail

#
# We run under SMF, which does not set HOME in the environment.
#
export HOME=$(getent passwd "$UID" | awk -F: '{ print $6 }')

url=$(svcprop -p buildomat/url "$SMF_FMRI")
strap=$(svcprop -p buildomat/strap "$SMF_FMRI")

top="$HOME/.buildomat/hubris-agent"
mkdir -p "$top"

tmpdir="$top/tmp"
rm -rf "$tmpdir"
mkdir "$tmpdir"

function os_release {
	if [[ ! -f /etc/os-release ]]; then
		printf '\n'
	else
		local r=$( ( . /etc/os-release ; eval "echo \$$1" ) )
		printf '%s\n' "${r// /+}"
	fi
}

#
# Give the server some hints as to what OS we're running so that it can give us
# the most appropriate agent binary:
#
q="?kernel=$(uname -s)"
q+="&proc=$(uname -p)"
q+="&mach=$(uname -m)"
q+="&plat=$(uname -i)"
q+="&id=$(os_release ID)"
q+="&id_like=$(os_release ID_LIKE)"
q+="&version_id=$(os_release VERSION_ID)"

while :; do
	rm -f "$tmpdir/agent"
	rm -f "$tmpdir/agent.gz"

	#
	# First, try the gzip-compressed agent URL:
	#
	if curl -sSf -o "$tmpdir/agent.gz" "$url/file/agent.gz$q"; then
		if ! gunzip < "$tmpdir/agent.gz" > "$tmpdir/agent"; then
			sleep 1
			continue
		fi
	#
	# If that doesn't work, fall back to the old uncompressed URL:
	#
	elif ! curl -sSf -o "$tmpdir/agent" "$url/file/agent$q"; then
		sleep 1
		continue
	fi

	chmod +rx "$tmpdir/agent"
	if ! "$tmpdir/agent" install -U "$top" "$url" "$strap"; then
		sleep 1
		continue
	fi
	break
done

exit 0
