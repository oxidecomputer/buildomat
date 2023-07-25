#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

#
# /var/tmp in the ramdisk will obviously fill up very quickly, so replace it
# with a tmpfs:
#
/usr/sbin/mount -F tmpfs -O swap /var/tmp

#
# /opt/buildomat will contain the agent, so, another tmpfs!
#
mkdir -p /opt/buildomat
/usr/sbin/mount -F tmpfs -O swap /opt/buildomat
chmod 0755 /opt/buildomat

#
# Set machine name.
#
echo '%HOST%' >/etc/nodename
hostname '%HOST%'

#
# Set up IP networking:
#
/sbin/ipadm create-if igb0
/sbin/ipadm create-addr -T dhcp -1 igb0/dhcp
/usr/sbin/svcadm restart svc:/network/service:default

#
# Create a ramdisk-backed ZFS pool to hold work.  Give it the name rpool so
# that existing tools that create, say, "rpool/work" will get roughly what they
# expect.
#
/usr/sbin/ramdiskadm -a rpool 8g
/sbin/zpool create -O compression=on rpool /dev/ramdisk/rpool
/sbin/zfs create -o mountpoint=/home rpool/home

#
# Contact the factory to signal that we've booted to this point.
#
while :; do
	if curl -X POST -sSf "%BASEURL%/signal/%HOST%?key=%KEY%"; then
		break
	fi

	sleep 1
done

#
# Defer buildomat agent installation until after multi-user startup:
#
rm -f /var/tmp/buildomat-install.sh
cat >/var/tmp/buildomat-install.sh <<'EOF'
#!/bin/bash

while :; do
	date -uR
	df -h

	rm -f /var/tmp/agent
	if ! curl -sSf -o /var/tmp/agent '%COREURL%/file/agent'; then
		sleep 1
		continue
	fi

	chmod +rx /var/tmp/agent
	if ! /var/tmp/agent install '%COREURL%' '%STRAP%'; then
		sleep 1
		continue
	fi

	break
done
EOF
chmod +x /var/tmp/buildomat-install.sh

rm -f /var/svc/manifest/site/buildomat-install.xml
cat >/var/svc/manifest/site/buildomat-install.xml <<'EOF'
<?xml version="1.0"?>
<!DOCTYPE service_bundle SYSTEM "/usr/share/lib/xml/dtd/service_bundle.dtd.1">

<service_bundle type='manifest' name='buildomat-install'>

<service name='site/buildomat-install' type='service' version='1'>
  <create_default_instance enabled='true' />
  <single_instance />

  <!-- Just wait for the big one. -->
  <dependency name='multi-user-server' grouping='require_all' restart_on='none'
    type='service'>
    <service_fmri value='svc:/milestone/multi-user-server:default' />
  </dependency>

  <exec_method type='method' name='start'
    exec='/var/tmp/buildomat-install.sh' timeout_seconds='3600' />

  <exec_method type='method' name='stop' exec=':true' timeout_seconds='3' />

  <property_group name='startd' type='framework'>
    <propval name='duration' type='astring' value='transient' />
  </property_group>

  <stability value='Unstable' />

  <template>
    <common_name>
      <loctext xml:lang='C'>install buildomat agent</loctext>
    </common_name>
  </template>
</service>

</service_bundle>
EOF

svcadm restart svc:/system/manifest-import:default

echo postboot complete
