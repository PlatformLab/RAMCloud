#!/bin/bash

# Utility script that automates the process of fetching a stable dpdk release,
# configuring its compilation options, and building the dpdk libraries.

DPDK_OPTIONS="CONFIG_RTE_BUILD_COMBINE_LIBS=y"

# It seems easier to get static DPDK library to work based on our experience.
# For example, as of 04/2018, we haven't been able to get MLX4 driver to work
# with DPDK shared libraries on the CloudLab m510 cluster.
DPDK_OPTIONS+=" CONFIG_RTE_BUILD_SHARED_LIB=n"

if [ "$MLNX_DPDK" != "y" ];
then
    # Use DPDK community release.
    DPDK_VER="16.11"
    DPDK="dpdk-${DPDK_VER}"
    DPDK_SRC="http://dpdk.org/browse/dpdk/snapshot/${DPDK}.tar.gz"
else
    # Use MLNX_DPDK release.
    DPDK_VER="16.11_2.3"
    DPDK="MLNX_DPDK_${DPDK_VER}"
    DPDK_SRC="http://www.mellanox.com/downloads/Drivers/${DPDK}.tar.gz"
fi

if [ ! -d ./deps ]; then mkdir deps; fi

if [ ! -d ./deps/${DPDK} ];
then
    cd deps;
    wget --no-clobber ${DPDK_SRC}
    tar zxvf ${DPDK}.tar.gz
    cd ..
fi
ln -sfn deps/${DPDK} dpdk

# Build the libraries, assuming an x86_64 linux target, and a gcc-based
# toolchain. Compile position-indepedent code, which will be linked by
# RAMCloud code, and produce a unified object archive file.
TARGET=x86_64-native-linuxapp-gcc
NUM_JOBS=`grep -c '^processor' /proc/cpuinfo`
if [ "$NUM_JOBS" -gt 2 ]; then
    let NUM_JOBS=NUM_JOBS-2
fi

cd dpdk && make config T=$TARGET O=$TARGET
cd $TARGET && make clean
make $DPDK_OPTIONS EXTRA_CFLAGS=-fPIC -j$NUM_JOBS
