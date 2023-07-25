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

	#
	# Some illumos images use autofs by default for /home, which is not
	# what we want here.
	#
	if home_fs=$(awk '$2 == "/home" { print $3 }' /etc/mnttab) &&
	    [[ "$home_fs" == autofs ]]; then
		sed -i -e '/^\/home/d' /etc/auto_master
		automount -v
	fi
	;;
Linux)
	#
	# The stock Ubuntu images we're using in AWS are often missing
	# some basic conveniences:
	#
	apt-get -y update
	apt-get -y install sysvbanner build-essential

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

	#
	# Simulate ptime to some extent:
	#
	cat >/bin/ptime <<-'EOF'
	#!/bin/bash
	verbose=no
	while getopts m c; do
		case "$c" in
		m)
			verbose=yes
			;;
		?)
			printf 'Usage: %s [-m] command args...\n' "$0" >&2
			exit 1
		esac
	done
	shift "$(( OPTIND - 1 ))"
	args=()
	if [[ $verbose == yes ]]; then
		args+=( '-v' )
	fi
	exec /usr/bin/time "${args[@]}" "$@"
	EOF
	chmod 0755 /bin/ptime

	#
	# Ubuntu 18.04 had a genuine pre-war separate /bin directory!
	#
	if [[ ! -L /bin ]]; then
		for prog in ptime pfexec; do
			ln -s "../../bin/$prog" "/usr/bin/$prog"
		done
	fi

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
