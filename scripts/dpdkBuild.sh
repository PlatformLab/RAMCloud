#!/bin/bash

# Utility script that automates the process of fetching a stable dpdk release,
# configuring its compilation options, and building the dpdk libraries.

DPDK_VER="1.8.0"

if [ ! -d ./deps ]; then mkdir deps; fi

if [ ! -d ./deps/dpdk-${DPDK_VER} ];
then
	cd deps;
	wget http://dpdk.org/browse/dpdk/snapshot/dpdk-${DPDK_VER}.tar.gz
	tar zxvf dpdk-${DPDK_VER}.tar.gz
	cd ..
	ln -s deps/dpdk-${DPDK_VER} dpdk
fi

# Configure the build process to produce a unified object archive file.
sed -i s/CONFIG_RTE_BUILD_COMBINE_LIBS=n/CONFIG_RTE_BUILD_COMBINE_LIBS=y/ dpdk/config/common_linuxapp

# Build the libraries, assuming an x86_64 linux target, and a gcc-based
# toolchain. Compile position-indepedent code, which will be linked by
# RAMCloud code.
cd dpdk && make config T=x86_64-native-linuxapp-gcc && CPU_CFLAGS="-fPIC" make
