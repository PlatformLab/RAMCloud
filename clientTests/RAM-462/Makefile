TOP=../..

include $(wildcard private/MakefragPrivateTop)

DEBUG ?= yes
YIELD ?= no
SSE ?= sse4.2
COMPILER ?= gnu
VALGRIND ?= yes

## Create a separate build directory for each git branch and for each arch
OBJSUFFIX := $(shell git symbolic-ref -q HEAD | \
	       sed -e s,refs/heads/,.,)

OBJDIR	:= obj$(OBJSUFFIX)

RAMLIBS := $(TOP)/$(OBJDIR)/libramcloud.a

EXTRALIBS=-v -L$(TOP)/$(OBJDIR) \
	$(TOP)/$(OBJDIR)/OptionParser.o

#	-Wl,-Bstatic -lramcloud \
#	$(RAMLIBS) \
#	-Wl,-Bdynamic 

GTEST_DIR ?= $(TOP)/gtest
LOGCABIN_DIR := $(TOP)/logcabin

ifeq ($(DEBUG),yes)
BASECFLAGS := -g
OPTFLAG	 :=
DEBUGFLAGS := -DTESTING=1 -fno-builtin
else
BASECFLAGS :=
OPTFLAG := -O3
DEBUGFLAGS := -DNDEBUG -Wno-unused-variable
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

# LIBS := $(EXTRALIBS) -lpcrecpp -lboost_program_options -lprotobuf -lrt 
LIBS := $(EXTRALIBS) -lpcrecpp -lboost_program_options -lprotobuf -lrt \
        -lpthread -lssl -lcrypto

ifeq ($(DEBUG),yes)
# -rdynamic generates more useful backtraces when you have debugging symbols
LIBS += -rdynamic
endif

INCLUDES := -I$(TOP)/src -I$(TOP)/$(OBJDIR) \
	-I$(GTEST_DIR)/include -I$(LOGCABIN_DIR)

CC ?= gcc
CXX ?= g++
AR ?= ar
PERL ?= perl
PYTHON ?= python
LINT := $(PYTHON) cpplint.py \
	--filter=-runtime/threadsafe_fn,-readability/streams,-whitespace/blank_line,-whitespace/braces,-whitespace/comments,-runtime/arrays,-build/include_what_you_use,-whitespace/semicolon
PRAGMAS := ./pragmas.py
NULL := # useful for terminating lists of files
PROTOC ?= protoc
EPYDOC ?= epydoc
EPYDOCFLAGS ?= --simple-term -v
DOXYGEN ?= doxygen

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

ifeq ($(YIELD),yes)
COMFLAGS += -DYIELD=1
endif

CFLAGS_BASE := $(COMFLAGS) -std=gnu11 $(INCLUDES)
CFLAGS_SILENT := $(CFLAGS_BASE)
CFLAGS_NOWERROR := $(CFLAGS_BASE) $(CWARNS)
CFLAGS := $(CFLAGS_BASE) -Werror $(CWARNS)

CXXFLAGS_BASE := $(COMFLAGS) -std=c++11 $(INCLUDES) $(LIBS)
CXXFLAGS_SILENT := $(CXXFLAGS_BASE) $(EXTRACXXFLAGS)
CXXFLAGS_NOWERROR := $(CXXFLAGS_BASE) $(CXXWARNS) $(EXTRACXXFLAGS)
CXXFLAGS := $(CXXFLAGS_BASE) -Werror $(CXXWARNS) $(EXTRACXXFLAGS)


ifeq ($(COMPILER),intel)
CXXFLAGS = $(CXXFLAGS_BASE) $(CXXWARNS)
endif

all:	RAM462-Test

RAM462-Test: ram462test

ram462test: ram462test.cc
	$(CXX) $(CXXFLAGS) $< \
	$(RAMLIBS) -libverbs \
	-o $@

run:
	(cd ../..; export LD_LIBRARY_PATH=$(LD_LIBRARY_PATH):$(OBJDIR); \
	scripts/cluster.py --verbose --timeout=1000 --client=clientTests/RAM-462/ram462test )

clean:
	/bin/rm -f ram462test ram462test.d *.o *~

check:
	echo OBJDIR=$(OBJDIR)
	echo INCLUDES=$(INCLUDES)
	echo CXXFLAGS=$(CXXFLAGS)
	echo LDFLAGS=$(LDFLAGS)
