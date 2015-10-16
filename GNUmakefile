# This makefile system follows the structuring conventions
# recommended by Peter Miller in his excellent paper:
#
#       Recursive Make Considered Harmful
#       http://aegis.sourceforge.net/auug97.pdf


# The following line allows developers to change the default values for make
# variables in the file private/MakefragPrivateTop.
include $(wildcard private/MakefragPrivateTop)

DEBUG ?= yes
YIELD ?= no
SSE ?= sse4.2
COMPILER ?= gnu
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
ZOOKEEPER_DIR ?= /usr/local/zookeeper-3.4.5
else
ZOOKEEPER_LIB :=
ZOOKEEPER_DIR :=
endif

ifeq ($(DEBUG),yes)
BASECFLAGS := -g
OPTFLAG	 :=
DEBUGFLAGS := -DTESTING=1 -fno-builtin
else
BASECFLAGS := -g
OPTFLAG := -O3
DEBUGFLAGS := -DNDEBUG -Wno-unused-variable -Wno-maybe-uninitialized
endif

COMFLAGS := $(BASECFLAGS) $(OPTFLAG) -fno-strict-aliasing \
	        -MD -m$(SSE) \
	        $(DEBUGFLAGS)
ifeq ($(COMPILER),gnu)
COMFLAGS += -march=core2
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
INFINIBAND = $(shell $(CXX) $(INCLUDES) $(EXTRACXXFLAGS) $(LIBS) -libverbs \
                         -o /dev/null src/HaveInfiniband.cc \
                         >/dev/null 2>&1 \
                         && echo yes || echo no)

ifeq ($(INFINIBAND),yes)
COMFLAGS += -DINFINIBAND
LIBS += -libverbs
endif

# Determines whether or not to build RAMCloud with DPDK support, such as
# a DPDK driver for FastTransport. Note: DPDK must be present at "./dpdk"
# (either directly or via a symbolic link). If you run the script
# scripts/dpdkBuild.sh, it will install DPDK in an appropriate way.
DPDK ?= no
ifeq ($(DPDK),yes)
INCLUDES += -Idpdk/build/include
# Note: --whole-archive is necessary to make sure that all of the facilities
# of the library are available for dynamic linking later.
LIBS += -Wl,--whole-archive dpdk/build/lib/libintel_dpdk.a -Wl,--no-whole-archive -ldl
# Note: __STDC_LIMIT_MACROS definition below is needed to avoid
# compilation errors in DPDK header files.
COMFLAGS += -DDPDK -Dtypeof=__typeof__
endif

ifeq ($(YIELD),yes)
COMFLAGS += -DYIELD=1
endif

CFLAGS_BASE := $(COMFLAGS) -std=gnu0x $(INCLUDES)
CFLAGS_SILENT := $(CFLAGS_BASE)
CFLAGS_NOWERROR := $(CFLAGS_BASE) $(CWARNS)
# CFLAGS := $(CFLAGS_BASE) $(CWARNS)
CFLAGS := $(CFLAGS_BASE) -Werror $(CWARNS)

CXXFLAGS_BASE := $(COMFLAGS) -std=c++0x $(INCLUDES)
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
	$(LINT) $$(./pragmas.py -f CPPLINT:5 $$(find $(TOP)/src '(' -name '*.cc' -or -name '*.h' -or -name '*.c' ')' -not -path '$(TOP)/src/btree/*' -not -path '$(TOP)/src/btreeRamCloud/*'))

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
	 echo "INPUT = src bindings README $(OBJDIR)"; \
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

# Rebuild the Java bindings
java: $(OBJDIR)/libramcloud.a
	cd bindings/java; ./gradlew
java-clean:
	cd bindings/java; ./gradlew clean

INSTALL_BINS := \
    $(OBJDIR)/client \
    $(OBJDIR)/coordinator \
    $(OBJDIR)/ensureServers \
    $(OBJDIR)/libramcloud.so \
    $(OBJDIR)/server \
    $(NULL)

INSTALL_LIBS := \
    $(OBJDIR)/libramcloud.a \
    $(NULL)

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
