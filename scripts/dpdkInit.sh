#!/bin/bash

# This script sets up everything needed to use DpdkDriver on a host in
# the Stanford RAMCloud cluster. Requirements:
#    * This script must run as root
#    * It must be run in a RAMCloud directory, which must contain a
#      "dpdk" subdirectory, in which DPDK must have been properly built.


if [ ! -f dpdk/x86_64-native-linuxapp-gcc/kmod/igb_uio.ko ]
then
    echo "Driver file dpdk/x86_64-native-linuxapp-gcc/kmod/igb_uio.ko" \
         "doesn't exist." \
         "Make sure you are in the right directory and have built DPDK."
    exit 1
fi

# Enable hugepage support.
echo 1024 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
mkdir -p /mnt/huge
mount -t hugetlbfs nodev /mnt/huge
chmod 777 /mnt/huge

# Load the UIO driver and bind it to the eth1 port (check the device
# with "dpdk/tools/dpdk_nic_bind.py --status": select the numbers for
# eth1.
modprobe uio
insmod dpdk/x86_64-native-linuxapp-gcc/kmod/igb_uio.ko
dpdk/tools/dpdk_nic_bind.py --bind=igb_uio 04:00.0
chmod 666 /dev/uio0
