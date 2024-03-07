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
	if ! curl -sSf -o /var/tmp/agent '%URL%/file/agent'"$q"; then
		sleep 1
		continue
	fi
	chmod +rx /var/tmp/agent
	if ! /var/tmp/agent install '%URL%' '%STRAP%'; then
		sleep 1
		continue
	fi
	break
done

exit 0
