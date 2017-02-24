# This makefile system follows the structuring conventions
# recommended by Peter Miller in his excellent paper:
#
#       Recursive Make Considered Harmful
#       http://aegis.sourceforge.net/auug97.pdf


# The following line allows developers to change the default values for make
# variables in the file private/MakefragPrivateTop.
include $(wildcard private/MakefragPrivateTop)

DEBUG ?= yes
DEBUG_OPT ?= no
YIELD ?= no
SSE ?= sse4.2
ARCH ?= native
COMPILER ?= gnu
CCACHE ?= no
GLIBCXX_USE_CXX11_ABI ?= no
LINKER ?= default
SANITIZER ?= none
VALGRIND ?= no
ONLOAD_DIR ?= /usr/local/openonload-201405

## Create a separate build directory for each git branch and for each arch
OBJSUFFIX := $(shell git symbolic-ref -q HEAD | \
	       sed -e s,refs/heads/,.,)

OBJDIR	:= obj$(OBJSUFFIX)

TOP	:= $(shell echo $${PWD-`pwd`})
GTEST_DIR ?= $(TOP)/gtest

# Determines whether or not RAMCloud is built with support for LogCabin as an
# ExternalStorage implementation.
LOGCABIN ?= no
ifeq ($(LOGCABIN),yes)
LOGCABIN_LIB ?= logcabin/build/liblogcabin.a
LOGCABIN_DIR ?= logcabin
else
LOGCABIN_LIB :=
LOGCABIN_DIR :=
endif

# Determines whether or not RAMCloud is built with support for ZooKeeper as an
# ExternalStorage implementation.
ZOOKEEPER ?= yes
ifeq ($(ZOOKEEPER),yes)
ZOOKEEPER_LIB ?= -lzookeeper_mt
ZOOKEEPER_DIR ?= /usr/share/zookeeper
else
ZOOKEEPER_LIB :=
ZOOKEEPER_DIR :=
endif

BASECFLAGS := -g
ifeq ($(DEBUG),yes)
ifeq ($(DEBUG_OPT),yes)
OPTFLAG := -Og
endif
DEBUGFLAGS := -DTESTING=1 -fno-builtin
else
OPTFLAG := -O3
DEBUGFLAGS := -DNDEBUG -Wno-unused-variable
endif

# Starting from GCC 5.1, libstdc++ introduced a new library ABI. To maintain
# backwards compatibility, the _GLIBCXX_USE_CXX11_ABI macro is used to select
# whether the declarations in the library headers use the old or new ABI.
ifeq ($(GLIBCXX_USE_CXX11_ABI),yes)
BASECFLAGS += -D_GLIBCXX_USE_CXX11_ABI=1
else
BASECFLAGS += -D_GLIBCXX_USE_CXX11_ABI=0
endif

COMFLAGS := $(BASECFLAGS) $(OPTFLAG) -fno-strict-aliasing \
	        -MD -m$(SSE) \
	        $(DEBUGFLAGS)
ifeq ($(COMPILER),gnu)
COMFLAGS += -march=$(ARCH)
endif
ifeq ($(LINKER),gold)
LDFLAGS += -fuse-ld=gold
else ifeq ($(LINKER),bfd)
LDFLAGS += -fuse-ld=bfd
endif
# Google sanitizers are not compatible with each other, so only apply one at a
# time.
ifeq ($(SANITIZER),address)
COMFLAGS += -DASAN -fsanitize=address -fno-omit-frame-pointer
LDFLAGS += -fsanitize=address
else ifeq ($(SANITIZER),memory)
# MSan is by far a unique feature in Clang and not available to GCC as of
# version 5.3
else ifeq ($(SANITIZER),thread)
COMFLAGS += -DTSAN -fsanitize=thread -fno-omit-frame-pointer -fPIE
LDFLAGS += -fsanitize=thread -pie
else ifeq ($(SANITIZER),undefined)
COMFLAGS += -DUBSAN -fsanitize=undefined -fno-omit-frame-pointer
LDFLAGS += -fsanitize=undefined
endif
ifeq ($(VALGRIND),yes)
COMFLAGS += -DVALGRIND
endif
ifeq ($(LOGCABIN),yes)
COMFLAGS += -DENABLE_LOGCABIN
endif
ifeq ($(ZOOKEEPER),yes)
COMFLAGS += -DENABLE_ZOOKEEPER
endif
TEST_INSTALL_FLAGS =

COMWARNS := -Wall -Wformat=2 -Wextra \
            -Wwrite-strings -Wno-unused-parameter -Wmissing-format-attribute
CWARNS   := $(COMWARNS) -Wmissing-prototypes -Wmissing-declarations -Wshadow \
		-Wbad-function-cast
CXXWARNS := $(COMWARNS) -Wno-non-template-friend -Woverloaded-virtual \
		-Wcast-qual \
		-Wcast-align -Wconversion
ifeq ($(COMPILER),gnu)
CXXWARNS += -Weffc++
endif
# Too many false positives list:
# -Wunreachable-code
# Failed deconstructor inlines are generating noise
# -Winline

LIBS := $(EXTRALIBS) $(LOGCABIN_LIB) $(ZOOKEEPER_LIB) \
	-lpcrecpp -lboost_program_options \
	-lprotobuf -lrt -lboost_filesystem -lboost_system \
	-lpthread -lssl -lcrypto
ifeq ($(DEBUG),yes)
# -rdynamic generates more useful backtraces when you have debugging symbols
LIBS += -rdynamic
endif

INCLUDES := -I$(TOP)/src \
            -I$(TOP)/$(OBJDIR) \
            -I$(GTEST_DIR)/include \
            -I/usr/local/openonload-201405/src/include \
             $(NULL)
ifeq ($(LOGCABIN),yes)
INCLUDES := $(INCLUDES) -I$(LOGCABIN_DIR)/include
endif

CC ?= gcc
CXX ?= g++
AR ?= ar
PERL ?= perl
PYTHON ?= python
LINT := $(PYTHON) cpplint.py --filter=-runtime/threadsafe_fn,-readability/streams,-whitespace/blank_line,-whitespace/braces,-whitespace/comments,-runtime/arrays,-build/include_what_you_use,-whitespace/semicolon
PRAGMAS := ./pragmas.py
NULL := # useful for terminating lists of files
PROTOC ?= protoc
EPYDOC ?= epydoc
EPYDOCFLAGS ?= --simple-term -v
DOXYGEN ?= doxygen

# Using ccache is as simple as prefixing the compilation commands with `ccache`.
ifeq ($(CCACHE),yes)
CC := ccache $(CC)
CXX := ccache $(CXX)
endif

# Directory for installation: various subdirectories such as include and
# bin will be created by "make install".
INSTALL_DIR ?= install

# Check if OnLoad is installed on the system. OnLoad is required to build
# SolarFlare driver code.
ONLOAD_VERSION := $(shell $(ONLOAD_DIR)/scripts/onload --version 2>/dev/null)
ifdef ONLOAD_VERSION
	ONLOAD = yes
	ONLOAD_LIB := -L$(ONLOAD_DIR)/build/gnu_x86_64/lib/ciul/ -L$(ONLOAD_DIR)/build/gnu_x86_64/lib/citools/ -lcitools1 -lciul1
	LIBS += $(ONLOAD_LIB)
	COMFLAGS += -DONLOAD
endif

# Test whether Infiniband support is available. Avoids using $(COMFLAGS)
# (particularly, -MD) which results in bad interactions with mergedeps.
INFINIBAND = $(shell $(CXX) -std=c++11 $(INCLUDES) src/HaveInfiniband.cc \
                         $(LIBS) -libverbs -o /dev/null >/dev/null 2>&1 \
                         && echo yes || echo no)

ifeq ($(INFINIBAND),yes)
COMFLAGS += -DINFINIBAND
LIBS += -libverbs
endif

# DPDK definitions:
#
# Uncomment the variable definition below (or specify DPDK=yes on the make
# command line, or set the variable in MakefragPrivateTop) to build RAMCloud
# with a DPDK driver for BasicTransport. You may also need to modify other
# variables below.
# To compile the DPDK library:
# * Download the desired version (RAMCloud currently works with versions
#   2.0 and higher).
# * Invoke "make install T=x86_64-native-linuxapp-gcc" in the top-level
#   directory.
# * Change to the directory "x86_64-native-linuxapp-gcc".
# * Edit the file ".config" in that directory to change the value of the
#   CONFIG_RTE_BUILD_SHARED_LIB variable to "y" (you can also leave it
#   "n" to build without shared libraries, in which case you will need to
#   modify other variables below here).
# * Then recompile: "make clean; make"
# DPDK ?= yes
ifeq ($(DPDK),yes)

# Uncomment the line below and modify its value (or set the value in
# MakefragPrivateTop) to hold the path to the top-level DPDK directory
# (the parent of the "x86_64-native-linuxapp-gcc" directory). Or, leave
# this variable undefined if DPDK is installed in the standard system
# locations.
# DPDK_DIR ?= dpdk

# Change the definition below if you compiled DPDK without shared libraries.
# Note: this configuration is not well tested and may not work.
DPDK_SHARED ?= yes

DPDK_TARGET  ?= x86_64-native-linuxapp-gcc
COMFLAGS    += -DDPDK

ifeq ($(DPDK_DIR),)
# DPDK is installed as part of the system
ifeq ($(DPDK_SHARED),no)
$(error DPDK_SHARED should be yes when DPDK is installed on the system)
endif
DPDK_SHARED := yes
DPDK_LIB_DIR ?= /usr/lib64
COMFLAGS += -I /usr/include/dpdk
else
# Link with the libraries in the DPDK SDK under DPDK_DIR
ifeq ($(wildcard $(DPDK_DIR)),)
$(error DPDK_DIR variable points to an invalid location)
endif
ifeq ($(wildcard $(DPDK_DIR)/$(DPDK_TARGET)),)
$(error $(DPDK_DIR)/$(DPDK_TARGET) not found. Have you built DPDK?)
endif
COMFLAGS   += -I$(DPDK_DIR)/$(DPDK_TARGET)/include
DPDK_LIB_DIR := $(DPDK_DIR)/$(DPDK_TARGET)/lib
LIBS       += -L$(DPDK_LIB_DIR)
endif

ifeq ($(DPDK_SHARED),yes)
# Link with shared libraries
# The lines below include libraries if they exist (some DPDK releases have
# them, some don't).
DPDK_MALLOC := $(shell test -e $(DPDK_LIB_DIR)/librte_malloc.so \
	&& echo '-lrte_malloc')
DPDK_VERTIO_UIO := $(shell test -e $(DPDK_LIB_DIR)/librte_pmd_virtio_uio.so \
	&& echo '-lrte_pmd_virtio_uio')
DPDK_VERTIO := $(shell test -e $(DPDK_LIB_DIR)/librte_pmd_virtio.so \
	&& echo '-lrte_pmd_virtio')
DPDK_SHLIBS := -lrte_ethdev -lrte_net -lrte_mbuf -lrte_mempool -lrte_ring \
	-lrte_kvargs -lrte_eal -lrte_pmd_e1000 -lrte_pmd_ixgbe \
	-lrte_pmd_ring $(DPDK_MALLOC) $(DPDK_VIRTIO) $(DPDK_VERTIO_UIO)
DPDK_RPATH := -Wl,-rpath,$(abspath $(DPDK_LIB_DIR))
# -ldl required because librte_eal refers to dlopen()
LIBS += $(DPDK_SHLIBS) $(DPDK_RPATH) -ldl
else
# Link with static libraries
# DPDK must have been compiled with CONFIG_RTE_BUILD_COMBINE_LIBS=y and -fPIC
DPDK_AR_LIBS := $(DPDK_LIB_DIR)/libdpdk.a
## --whole-archive is required to link the pmd objects.
LIBS := -Wl,--whole-archive $(DPDK_AR_LIBS) -Wl,--no-whole-archive -ldl $(LIBS)
endif

# End of DPDK definitions
# =======
endif

ifeq ($(YIELD),yes)
COMFLAGS += -DYIELD=1
endif

CFLAGS_BASE := $(COMFLAGS) -std=gnu11 $(INCLUDES)
CFLAGS_SILENT := $(CFLAGS_BASE)
CFLAGS_NOWERROR := $(CFLAGS_BASE) $(CWARNS)
# CFLAGS := $(CFLAGS_BASE) $(CWARNS)
CFLAGS := $(CFLAGS_BASE) -Werror $(CWARNS)

CXXFLAGS_BASE := $(COMFLAGS) -std=c++11 $(INCLUDES)
CXXFLAGS_SILENT := $(CXXFLAGS_BASE) $(EXTRACXXFLAGS)
CXXFLAGS_NOWERROR := $(CXXFLAGS_BASE) $(CXXWARNS) $(EXTRACXXFLAGS)
# CXXFLAGS := $(CXXFLAGS_BASE) $(CXXWARNS) $(EXTRACXXFLAGS) $(PERF)
CXXFLAGS := $(CXXFLAGS_BASE) -Werror $(CXXWARNS) $(EXTRACXXFLAGS) $(PERF)

ifeq ($(COMPILER),intel)
CXXFLAGS = $(CXXFLAGS_BASE) $(CXXWARNS)
endif

# run-cc:
# Compile a C source file to an object file.
# Uses the GCCWARN pragma setting defined within the C source file.
# The first parameter $(1) should be the output filename (*.o)
# The second parameter $(2) should be the input filename (*.c)
# The optional third parameter $(3) is any additional options compiler options.
define run-cc
@GCCWARN=$$( $(PRAGMAS) -q GCCWARN $(2) ); \
case $$GCCWARN in \
0) \
	echo $(CC) $(CFLAGS_SILENT) $(3) -c -o $(1) $(2); \
	$(CC) $(CFLAGS_SILENT) $(3) -c -o $(1) $(2); \
	;; \
5) \
	echo $(CC) $(CFLAGS_NOWERROR) $(3) -c -o $(1) $(2); \
	$(CC) $(CFLAGS_NOWERROR) $(3) -c -o $(1) $(2); \
	;; \
9) \
	echo $(CC) $(CFLAGS) $(3) -c -o $(1) $(2); \
	$(CC) $(CFLAGS) $(3) -c -o $(1) $(2); \
	;; \
esac
endef

# run-cxx:
# Compile a C++ source file to an object file.
# Uses the GCCWARN pragma setting defined within the C source file.
# The first parameter $(1) should be the output filename (*.o)
# The second parameter $(2) should be the input filename (*.cc)
# The optional third parameter $(3) is any additional options compiler options.
define run-cxx
@GCCWARN=$$( $(PRAGMAS) -q GCCWARN $(2) ); \
case $$GCCWARN in \
0) \
	echo $(CXX) $(CXXFLAGS_SILENT) $(3) -c -o $(1) $(2); \
	$(CXX) $(CXXFLAGS_SILENT) $(3) -c -o $(1) $(2); \
	;; \
5) \
	echo $(CXX) $(CXXFLAGS_NOWERROR) $(3) -c -o $(1) $(2); \
	$(CXX) $(CXXFLAGS_NOWERROR) $(3) -c -o $(1) $(2); \
	;; \
9) \
	echo $(CXX) $(CXXFLAGS) $(3) -c -o $(1) $(2); \
	$(CXX) $(CXXFLAGS) $(3) -c -o $(1) $(2); \
	;; \
esac
endef

all:

tests: test
test: python-test

.SUFFIXES:

include src/Makefrag
include src/MakefragClient
include src/MakefragServer
include src/MakefragCoordinator
include src/MakefragTest
include apps/MakefragApp
include nanobenchmarks/MakefragNano
include src/misc/Makefrag
include bindings/python/Makefrag

# The following line allows developers to create private make rules
# in the file private/MakefragPrivate.  The recommended approach is
# for you to keep all private files (personal development hacks,
# test scripts, etc.) in the "private" subdirectory.
include $(wildcard private/MakefragPrivate)

clean: tests-clean docs-clean tags-clean install-clean java-clean
	rm -rf $(OBJDIR)/.deps $(OBJDIR)/*

check:
	$(LINT) $$(./pragmas.py -f CPPLINT:5 $$(find $(TOP)/src $(TOP)/apps $(TOP)/nanobenchmarks '(' -name '*.cc' -or -name '*.h' -or -name '*.c' ')' -not -path '$(TOP)/src/btree/*' -not -path '$(TOP)/src/btreeRamCloud/*'))

# This magic automatically generates makefile dependencies
# for header files included from C source files we compile,
# and keeps those dependencies up-to-date every time we recompile.
# See 'mergedep.pl' for more information.
OBJDIRS += .
$(OBJDIR)/.deps: $(foreach dir, $(OBJDIRS), $(wildcard $(OBJDIR)/$(dir)/*.d))
	@mkdir -p $(@D)
	$(PERL) mergedep.pl $@ $^

-include $(OBJDIR)/.deps

always:
	@:

doc: docs
# Get the branch name and SHA from git and put that into the doxygen mainpage
docs:
	@DOCSID=`git branch --no-color | grep "*" | cut -f2 -d" "` ;\
	DOCSID=$$DOCSID-`cat ".git/$$( git symbolic-ref HEAD )" | cut -c1-6` ;\
	(echo "PROJECT_NUMBER = \"Version [$$DOCSID]\""; \
	 echo "INPUT = src bindings README.md $(OBJDIR)"; \
	 echo "INCLUDE_PATH = $(OBJDIR)"; ) | cat Doxyfile - | $(DOXYGEN) -

docs-clean: python-docs-clean
	rm -rf docs/doxygen/

tags:
	find . -type f | grep -v "\.git" | grep -v docs | xargs etags
	find . -type f | grep -v "\.git" | grep -v docs | xargs ctags

tags-clean:
	rm -f TAGS tags

# The following target is useful for debugging Makefiles; it
# prints the value of a make variable.
print-%:
	@echo $* = $($*)

INSTALL_BINS := \
    $(APPOBJDIR)/client \
    $(OBJDIR)/coordinator \
    $(APPOBJDIR)/ensureServers \
    $(OBJDIR)/server \
    $(NULL)

INSTALL_LIBS := \
    $(OBJDIR)/libramcloud.so \
    $(OBJDIR)/libramcloud.a \
    $(NULL)

# Rebuild the Java bindings
java: $(INSTALL_LIBS)
	bindings/java/gradlew --project-dir bindings/java
java-clean:
	bindings/java/gradlew --project-dir bindings/java clean

# The header files below are those that must be installed in order to
# compile RAMCloud applications. Please try to keep this list as short
# as possible.
INSTALL_INCLUDES := \
    src/Atomic.h \
    src/BoostIntrusive.h \
    src/Buffer.h \
    src/ClientException.h \
    src/CodeLocation.h \
    src/CoordinatorClient.h \
    src/CoordinatorRpcWrapper.h \
    src/Crc32C.h \
    src/Exception.h \
    src/Fence.h \
    src/Key.h \
    src/IndexRpcWrapper.h \
    src/LinearizableObjectRpcWrapper.h \
    src/LogEntryTypes.h \
    src/Logger.h \
    src/LogMetadata.h \
    src/MasterClient.h \
    src/Minimal.h \
    src/Object.h \
    src/ObjectBuffer.h \
    src/ObjectRpcWrapper.h \
    src/OptionParser.h \
    src/PerfStats.h \
    src/RamCloud.h \
    src/RpcLevel.h \
    src/RpcWrapper.h \
    src/RpcTracker.h \
    src/RejectRules.h \
    src/ServerId.h \
    src/ServerIdRpcWrapper.h \
    src/ServerMetrics.h \
    src/ServiceMask.h \
    src/SpinLock.h \
    src/Status.h \
    src/TestLog.h \
    src/Transport.h \
    src/Tub.h \
    src/WireFormat.h \
    $(OBJDIR)/Histogram.pb.h \
    $(OBJDIR)/Indexlet.pb.h \
    $(OBJDIR)/LogMetrics.pb.h \
    $(OBJDIR)/MasterRecoveryInfo.pb.h \
    $(OBJDIR)/RecoveryPartition.pb.h \
    $(OBJDIR)/RpcLevelData.h \
    $(OBJDIR)/ServerConfig.pb.h \
    $(OBJDIR)/ServerList.pb.h \
    $(OBJDIR)/ServerStatistics.pb.h \
    $(OBJDIR)/SpinLockStatistics.pb.h \
    $(OBJDIR)/TableConfig.pb.h \
    $(OBJDIR)/Tablets.pb.h \
    $(NULL)

INSTALLED_BINS := $(patsubst $(OBJDIR)/%, $(INSTALL_DIR)/bin/%, $(INSTALL_BINS))
INSTALLED_LIBS := $(patsubst $(OBJDIR)/%, $(INSTALL_DIR)/lib/%, $(INSTALL_LIBS))

install: all java
	mkdir -p $(INSTALL_DIR)/bin
	cp $(INSTALL_BINS) $(INSTALL_DIR)/bin
	mkdir -p $(INSTALL_DIR)/include/ramcloud
	cp $(INSTALL_INCLUDES) $(INSTALL_DIR)/include/ramcloud
	mkdir -p $(INSTALL_DIR)/lib/ramcloud
	cp $(INSTALL_LIBS) $(INSTALL_DIR)/lib/ramcloud
	cp bindings/java/build/install/ramcloud/lib/* $(INSTALL_DIR)/lib/ramcloud

install-clean:
	rm -rf install

logcabin:
	cd logcabin; \
	scons

startZoo:
	if [ ! -e $(ZOOKEEPER_DIR)/data/zookeeper_server.pid ]; then \
	        $(ZOOKEEPER_DIR)/bin/zkServer.sh start; fi

stopZoo:
	$(ZOOKEEPER_DIR)/bin/zkServer.sh stop

.PHONY: all always clean check doc docs docs-clean install tags tags-clean \
	test tests logcabin startZoo stopZoo
