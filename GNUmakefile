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
	       -Wall -MD
COMWARNS := -Wformat=2 -Wextra -Wmissing-noreturn \
            -Wwrite-strings -Wno-unused-parameter -Wmissing-format-attribute \
            -Wswitch-default -fno-builtin
CWARNS   := $(COMWARNS) -Wmissing-prototypes -Wmissing-declarations -Wshadow \
		-Wbad-function-cast
CXXWARNS := $(COMWARNS) -Wno-non-template-friend -Woverloaded-virtual \
		-Wconversion -Wcast-qual -Winline \
		-Weffc++ -Wswitch-enum -Wcast-align
# Too many false positives list:
# -Wunreachable-code
INCLUDES := -I$(TOP)/src


CFLAGS	:= $(COMFLAGS) $(CWARNS) -std=gnu99 $(INCLUDES)
CXXFLAGS    := $(COMFLAGS) $(CXXWARNS) -std=c++98 $(INCLUDES)

CC := gcc
CXX := g++
AR := ar
PERL := perl
LINT := python cpplint.py --filter=-runtime/threadsafe_fn,-readability/streams,-whitespace/blank_line,-whitespace/braces,-whitespace/comments

all:

tests: test
test:

.SUFFIXES:

include src/Makefrag
include src/shared/Makefrag
include src/server/Makefrag
include src/backup/Makefrag
include src/client/Makefrag
include src/tests/Makefrag

clean:
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

.PHONY: all always clean check test tests install
