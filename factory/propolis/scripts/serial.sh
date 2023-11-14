#!/bin/bash

set -o pipefail
set -o xtrace

. /lib/svc/share/smf_include.sh

while :; do
	#
	# Just pass serial data as-is from propolis to the factory socket.
	# This is somewhat gross, but also didn't require writing any software
	# just at the moment.
	#
	nc -v -d -D -U /vm/ttya </dev/null |
	    nc -v -D -U /serial/sock >/dev/null
	sleep 0.1
done
