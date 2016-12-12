#!/bin/bash

# Utility script that automates the process of fetching a stable dpdk release,
# configuring its compilation options, and building the dpdk libraries.

DPDK_VER="16.07"

if [ ! -d ./deps ]; then mkdir deps; fi

if [ ! -d ./deps/dpdk-${DPDK_VER} ];
then
	cd deps;
	wget --no-clobber http://dpdk.org/browse/dpdk/snapshot/dpdk-${DPDK_VER}.tar.gz
	tar zxvf dpdk-${DPDK_VER}.tar.gz
	cd ..
fi
ln -sfn deps/dpdk-${DPDK_VER} dpdk

# Build the libraries, assuming an x86_64 linux target, and a gcc-based
# toolchain. Compile position-indepedent code, which will be linked by
# RAMCloud code, and produce a unified object archive file.
TARGET=x86_64-native-linuxapp-gcc
NUM_JOBS=`grep -c '^processor' /proc/cpuinfo`
if [ "$NUM_JOBS" -gt 2 ]; then
    let NUM_JOBS=NUM_JOBS-2
fi

DPDK_OPTIONS="CONFIG_RTE_BUILD_SHARED_LIB=y CONFIG_RTE_BUILD_COMBINE_LIBS=y"
cd dpdk && make config T=$TARGET O=$TARGET
cd $TARGET && make clean && make $DPDK_OPTIONS EXTRA_CFLAGS=-fPIC -j$NUM_JOBS
