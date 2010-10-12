# This makefile system follows the structuring conventions
# recommended by Peter Miller in his excellent paper:
#
#       Recursive Make Considered Harmful
#       http://aegis.sourceforge.net/auug97.pdf

DEBUG := yes

## Create a separate build directory for each git branch and for each arch
OBJSUFFIX := $(shell git symbolic-ref -q HEAD | \
	       sed -e s,refs/heads/,.,)

OBJDIR	:= obj$(OBJSUFFIX)

TOP	:= $(shell echo $${PWD-`pwd`})

BASECFLAGS :=
OPTFLAG	 := -O3
ifeq ($(DEBUG),yes)
DEBUGFLAGS :=
else
DEBUGFLAGS := -DNDEBUG -Wno-unused-variable
endif
COMFLAGS := $(BASECFLAGS) -g $(OPTFLAG) -fno-strict-aliasing \
	        -fno-builtin -MD
COMWARNS := -Wall -Wformat=2 -Wextra \
            -Wwrite-strings -Wno-unused-parameter -Wmissing-format-attribute \
            $(DEBUGFLAGS)
CWARNS   := $(COMWARNS) -Wmissing-prototypes -Wmissing-declarations -Wshadow \
		-Wbad-function-cast
CXXWARNS := $(COMWARNS) -Wno-non-template-friend -Woverloaded-virtual \
		-Wcast-qual \
		-Weffc++ -Wcast-align
# Too many false positives list:
# -Wunreachable-code
# Broken due to implicit promotion to int in g++ 4.4.4
# -Wconversion
# Failed deconstructor inlines are generating noise
# -Winline
LIBS := -lpcrecpp -lboost_program_options
INCLUDES := -I$(TOP)/src


CFLAGS_BASE := $(COMFLAGS) -std=gnu99 $(LIBS) $(INCLUDES)
CFLAGS_NOWERROR := $(CFLAGS_BASE) $(CWARNS)
CFLAGS := $(CFLAGS_BASE) -Werror $(CWARNS)

CXXFLAGS_BASE := $(COMFLAGS) -std=c++98 $(LIBS) $(INCLUDES)
CXXFLAGS_NOWERROR := $(CXXFLAGS_BASE) $(CXXWARNS)
CXXFLAGS := $(CXXFLAGS_BASE) -Werror $(CXXWARNS)

CC := gcc
CXX := g++
AR := ar
PERL := perl
LINT := python cpplint.py --filter=-runtime/threadsafe_fn,-readability/streams,-whitespace/blank_line,-whitespace/braces,-whitespace/comments,-runtime/arrays,-build/include_what_you_use
PRAGMAS := ./pragmas.py
NULL := # useful for terminating lists of files

# run-cc:
# Compile a C source file to an object file.
# Uses the GCCWARN pragma setting defined within the C source file.
# The first parameter $(1) should be the output filename (*.o)
# The second parameter $(2) should be the input filename (*.c)
# The optional third parameter $(3) is any additional options compiler options.
define run-cc
@GCCWARN=$$( $(PRAGMAS) -q GCCWARN $(2) ); \
if [ $$GCCWARN -eq 5 ]; then \
	echo $(CC) $(CFLAGS_NOWERROR) $(3) -c -o $(1) $(2); \
	$(CC) $(CFLAGS_NOWERROR) $(3) -c -o $(1) $(2); \
else \
	echo $(CC) $(CFLAGS) $(3) -c -o $(1) $(2); \
	$(CC) $(CFLAGS) $(3) -c -o $(1) $(2); \
fi

endef

# run-cxx:
# Compile a C++ source file to an object file.
# Uses the GCCWARN pragma setting defined within the C source file.
# The first parameter $(1) should be the output filename (*.o)
# The second parameter $(2) should be the input filename (*.cc)
# The optional third parameter $(3) is any additional options compiler options.
define run-cxx
@GCCWARN=$$( $(PRAGMAS) -q GCCWARN $(2) ); \
if [ $$GCCWARN -eq 5 ]; then \
	echo $(CXX) $(CXXFLAGS_NOWERROR) $(3) -c -o $(1) $(2); \
	$(CXX) $(CXXFLAGS_NOWERROR) $(3) -c -o $(1) $(2); \
else \
	echo $(CXX) $(CXXFLAGS) $(3) -c -o $(1) $(2); \
	$(CXX) $(CXXFLAGS) $(3) -c -o $(1) $(2); \
fi
endef

define filter-pragma
for f in $(3); do \
	if [ $$( $(PRAGMAS) -q $(1) $$f ) -eq $(2) ]; then \
		echo $$f; \
	fi \
done
endef

all:

tests: test
test: all python-test mockpy-test

.SUFFIXES:

include src/Makefrag
include src/MakefragClient
include src/MakefragServer
include src/MakefragBackup
include src/MakefragTest
include src/misc/Makefrag
include bindings/python/Makefrag

# The following line allows developers to create private make rules
# in the file private/MakefragPrivate.  The recommended approach is
# for you to keep all private files (personal development hacks,
# test scripts, etc.) in the "private" subdirectory.
include $(wildcard private/MakefragPrivate)

clean: tests-clean docs-clean tags-clean
	rm -rf $(OBJDIR)/.deps $(OBJDIR)/*

# Lazy rule so this doesn't happen unless make check is invoked
CHKFILES = $(shell find $(TOP)/src -name '*.cc' -or -name '*.h' -or -name '*.c')
CHKFILES := $(shell $(call filter-pragma,CPPLINT,5,$(CHKFILES)))
check:
	$(LINT) $(CHKFILES)

install: client-lib-install

uninstall: client-lib-uninstall

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
docs: python-docs
	doxygen Doxyfile

docs-clean: python-docs-clean
	rm -rf docs/doxygen/

tags: 
	find . -type f | grep -v "\.git" | xargs etags
	find . -type f | grep -v "\.git" | xargs ctags

tags-clean:
	rm -f TAGS tags

# The following target is useful for debugging Makefiles; it
# prints the value of a make variable.
print-%:
	@echo $* = $($*)

.PHONY: all always clean check doc docs docs-clean tags tags-clean test tests install uninstall
