# This makefile system follows the structuring conventions
# recommended by Peter Miller in his excellent paper:
#
#       Recursive Make Considered Harmful
#       http://aegis.sourceforge.net/auug97.pdf

## Create a separate build directory for each git branch and for each arch
OBJSUFFIX := $(shell git symbolic-ref -q HEAD | \
	       sed -e s,refs/heads/,.,)

OBJDIR	:= obj$(OBJSUFFIX)

TOP	:= $(shell echo $${PWD-`pwd`})

BASECFLAGS :=
#OPTFLAG	 := -O3
COMFLAGS := $(BASECFLAGS) -g $(OPTFLAG) -fno-strict-aliasing \
	        -fno-builtin -MD
COMWARNS := -Wall -Wformat=2 -Wextra -Wmissing-noreturn \
            -Wwrite-strings -Wno-unused-parameter -Wmissing-format-attribute \
            -Wswitch-default
CWARNS   := $(COMWARNS) -Wmissing-prototypes -Wmissing-declarations -Wshadow \
		-Wbad-function-cast
CXXWARNS := $(COMWARNS) -Wno-non-template-friend -Woverloaded-virtual \
		-Wconversion -Wcast-qual -Winline \
		-Weffc++ -Wswitch-enum -Wcast-align
# Too many false positives list:
# -Wunreachable-code
LIBS := -lrt
# -lrt required for temporary semaphore. See RC_CLIENT_SHARED and RAM-39.
INCLUDES := -I$(TOP)/src


CFLAGS	:= $(COMFLAGS) $(CWARNS) -std=gnu99 $(LIBS) $(INCLUDES)
CXXFLAGS    := $(COMFLAGS) $(CXXWARNS) -std=c++98 $(LIBS) $(INCLUDES)

CC := gcc
CXX := g++
AR := ar
PERL := perl
LINT := python cpplint.py --filter=-runtime/threadsafe_fn,-readability/streams,-whitespace/blank_line,-whitespace/braces,-whitespace/comments,-runtime/arrays
PRAGMAS := ./pragmas.py

# run-cc:
# Compile a C source file to an object file.
# The first parameter $(1) should be the output filename (*.o)
# The second parameter $(2) should be the input filename (*.c)
# The optional third parameter $(3) is any additional options compiler options.
define run-cc
$(CC) $(CFLAGS) $(3) -c -o $(1) $(2)
endef

# run-cxx:
# Compile a C++ source file to an object file.
# The first parameter $(1) should be the output filename (*.o)
# The second parameter $(2) should be the input filename (*.cc)
# The optional third parameter $(3) is any additional options compiler options.
define run-cxx
$(CXX) $(CXXFLAGS) $(3) -c -o $(1) $(2);
endef

all:

tests: test
test: python-test

.SUFFIXES:

include src/Makefrag
include src/MakefragServer
include src/MakefragBackup
include src/MakefragClient
include src/MakefragTest
include bindings/python/Makefrag

clean: tests-clean docs-clean
	rm -rf $(OBJDIR)/.deps $(OBJDIR)/*

# Lazy rule so this doesn't happen unless make check is invoked
CHKFILES = $(shell find $(TOP)/src -name '*.cc' -or -name '*.h' -or -name '*.c')
check:
	$(LINT) $(CHKFILES)

install: client-lib-install

# This magic automatically generates makefile dependencies
# for header files included from C source files we compile,
# and keeps those dependencies up-to-date every time we recompile.
# See 'mergedep.pl' for more information.
$(OBJDIR)/.deps: $(foreach dir, $(OBJDIRS), $(wildcard $(OBJDIR)/$(dir)/*.d))
	@mkdir -p $(@D)
	$(PERL) mergedep.pl $@ $^

-include $(OBJDIR)/.deps

always:
	@:

doc: docs
docs: python-docs
	doxygen Doxyfile

docs-clean: python-docs-clean
	rm -rf docs/doxygen/

.PHONY: all always clean check doc docs docs-clean test tests install
