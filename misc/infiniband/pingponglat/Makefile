# cribbed from OFA
TESTS = rc_lat ud_lat

all: ${TESTS}

CFLAGS += -Wall -std=c99 -g -D_GNU_SOURCE -O2
#The following seems to help GNU make on some platforms
LOADLIBES += 
LDFLAGS +=

${TESTS}: LOADLIBES += -libverbs

${RDMACM_TESTS}: %: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $< $(LOADLIBES) $(LDLIBS) -o $@
${MCAST_TESTS}: %: %.c ${MCAST_FILES}
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $< $(LOADLIBES) $(LDLIBS) -o $@
${TESTS}: %: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) $(LDFLAGS) $< $(LOADLIBES) $(LDLIBS) -o $@

clean:
	$(foreach fname,${TESTS}, rm -f ${fname})
.DELETE_ON_ERROR:
.PHONY: all clean
