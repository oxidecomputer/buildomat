#!/usr/bin/env bash

set -o errexit
set -o pipefail

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
	rm -f /var/tmp/agent
	rm -f /var/tmp/agent.gz

	#
	# First, try the gzip-compressed agent URL:
	#
	if curl -sSf -o /var/tmp/agent.gz '%URL%/file/agent.gz'"$q"; then
		if ! gunzip < /var/tmp/agent.gz > /var/tmp/agent; then
			sleep 1
			continue
		fi
	#
	# If that doesn't work, fall back to the old uncompressed URL:
	#
	elif ! curl -sSf -o /var/tmp/agent '%URL%/file/agent'"$q"; then
		sleep 1
		continue
	fi

	chmod +rx /var/tmp/agent
	if ! /var/tmp/agent install -N '%NODENAME%' '%URL%' '%STRAP%'; then
		sleep 1
		continue
	fi
	break
done

exit 0
