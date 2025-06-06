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

if [[ $(uname -s) == SunOS ]]; then
	#
	# The Internet at the office does not have consistently fantastic
	# peering, so make sure we're putting our best foot forward with TCP:
	#
	ipadm set-prop -p send_buf=512000 tcp || true
	ipadm set-prop -p recv_buf=512000 tcp || true
	ipadm set-prop -p congestion_control=cubic tcp || true
fi

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
	if ! /var/tmp/agent install '%URL%' '%STRAP%'; then
		sleep 1
		continue
	fi
	break
done

exit 0
