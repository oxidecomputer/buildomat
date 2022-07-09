#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

kern="$(uname -s)"

case "$kern" in
SunOS)
	groupadd -g 12345 build
	useradd -u 12345 -g build -d /home/build -s /bin/bash \
	    -c 'build' -P 'Primary Administrator' build

	zfs create -o mountpoint=/work rpool/work
	;;
Linux)
	groupadd -g 12345 build
	useradd -u 12345 -g build -d /home/build -s /bin/bash \
	    -c 'build' build

	#
	# Simulate pfexec and the 'Primary Administrator' role with sudo:
	#
	echo 'build ALL=(ALL:ALL) NOPASSWD:ALL' > /etc/sudoers.d/build
	chmod 0440 /etc/sudoers.d/build
	cat >/bin/pfexec <<-'EOF'
	#!/bin/bash
	exec /bin/sudo -- "$@"
	EOF
	chmod 0755 /bin/pfexec

	mkdir -p /work
	;;
*)
	printf 'ERROR: unknown OS: %s\n' "$kern" >&2
	exit 1
	;;
esac

mkdir -p /home/build
chown build:build /home/build /work
chmod 0700 /home/build /work
