#!/bin/bash

set -o pipefail
set -o xtrace

. /lib/svc/share/smf_include.sh

if ! cd /vm; then
	exit "$SMF_EXIT_ERR_FATAL"
fi

#
# The virtual machine is configured to exit on reboot or shutdown.  We will
# write the exit code into a marker file and then disable ourselves.
#
/software/propolis-standalone config.toml
rc=$?

printf '%d\n' "$rc" > /vm/exit_code.txt
/usr/sbin/svcadm disable "$SMF_FMRI"
sleep 5
