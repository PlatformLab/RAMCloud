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

COMWARNS := -Wformat=2 -Wextra -Wmissing-noreturn \
            -Wwrite-strings -Wno-unused-parameter -Wmissing-format-attribute \
            -Wswitch-default -fno-builtin
CWARNS   := $(COMWARNS) -Wmissing-prototypes -Wmissing-declarations -Wshadow \
		-Wbad-function-cast
CXXWARNS := $(COMWARNS) -Wno-non-template-friend -Woverloaded-virtual \
	 	-Wconversion -Wcast-qual -Wunreachable-code  -Winline \
		-Weffc++ -Wswitch-enum -Wcast-align
OPTFLAG := -O2
INCLUDES := -I$(TOP)/src


CFLAGS	:= $(OPTFLAG) $(CWARNS) -Werror -std=c99 $(INCLUDES)
CXXFLAGS    := $(OPTFLAG) $(CXXWARNS) -Werror -std=c++0x $(INCLUDES)

CC := gcc
CXX := g++
PERL := perl
LINT := python cpplint.py

all:

.SUFFIXES:

include src/server/Makefrag

clean:
	rm -rf $(OBJDIR)/.deps $(OBJDIR)/*

CHKFILES := $(shell find $(TOP)/src -name '*.cc' -or -name '*.h' -or -name '*.c')
check:
	$(LINT) $(CHKFILES)

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

.PHONY: all always clean check
