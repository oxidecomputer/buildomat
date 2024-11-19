#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

kern="$(uname -s)"

build_user='build'
build_uid=12345

work_dir='/work'
input_dir='/input'

if [[ $UID == $build_uid ]]; then
	#
	# Most workers allow tasks to run as root, and thus to have total
	# control of the system.  This works for factories that generate an
	# empeheral environment (e.g., a virtual machine or a physical machine
	# booted from the network) where the environment can be destroyed
	# at the end of the job.
	#
	# If we were unable to get superuser privileges here, we must be
	# operating in an environment created by a factory that requires jobs
	# be run unprivileged.  In that case, the factory must have done all of
	# the environment setup that we require for the target; e.g., creating
	# /work, installing any required commands into a system directory, etc.
	#
	printf 'INFO: running unprivileged!\n'

	#
	# Make sure we can write to directories that we use in the job.
	#
	for d in "$HOME" "$input_dir" "$work_dir"; do
		#
		# Create and remove a file in each required directory:
		#
		fp="$d/.buildomat.write.trial"
		if rm -f "$fp" && touch "$fp" && rm "$fp"; then continue
		fi

		printf 'ERROR: directory "%s" not available?\n' "$d" >&2
		exit 1
	done

	exit 0
fi

case "$kern" in
SunOS)
	groupadd -g "$build_uid" "$build_user"
	useradd -u "$build_uid" -g "$build_user" -d /home/build -s /bin/bash \
	    -c "$build_user" -P 'Primary Administrator' "$build_user"

	zfs create -o mountpoint="$work_dir" rpool/work

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

	groupadd -g "$build_uid" "$build_user"
	useradd -u "$build_uid" -g "$build_user" -d /home/build -s /bin/bash \
	    -c "$build_user" "$build_user"

	#
	# Simulate pfexec and the 'Primary Administrator' role with sudo:
	#
	echo "$build_user ALL=(ALL:ALL) NOPASSWD:ALL" > /etc/sudoers.d/build
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

	mkdir -p "$work_dir"
	;;
*)
	printf 'ERROR: unknown OS: %s\n' "$kern" >&2
	exit 1
	;;
esac

mkdir -p /home/build
chown "$build_user":"$build_user" /home/build "$work_dir"
chmod 0700 /home/build "$work_dir"
