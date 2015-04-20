/* Copyright (c) 2009 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file 
 * Some definitions for stuff declared in Common.h.
 */

#include <ctype.h>
#include <cxxabi.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "Common.h"
#include "Buffer.h"
#include "Memory.h"
#include "ShortMacros.h"

namespace RAMCloud {

uint64_t mockPMCValue = 0lu;
uint64_t mockRandomValue = 0lu;
uint64_t debugXXX = 0lu;
double  debugYYY = 0;

/// A safe version of sprintf.
string
format(const char* format, ...)
{
    va_list ap;
    va_start(ap, format);
    string s = vformat(format, ap);
    va_end(ap);
    return s;
}

/// A safe version of vprintf.
string
vformat(const char* format, va_list ap)
{
    string s;

    // We're not really sure how big of a buffer will be necessary.
    // Try 1K, if not the return value will tell us how much is necessary.
    int bufSize = 1024;
    while (true) {
        char buf[bufSize];
        // vsnprintf trashes the va_list, so copy it first
        va_list aq;
        __va_copy(aq, ap);
        int r = vsnprintf(buf, bufSize, format, aq);
        assert(r >= 0); // old glibc versions returned -1
        if (r < bufSize) {
            s = buf;
            break;
        }
        bufSize = r + 1;
    }

    return s;
}

uint64_t
_generateRandom()
{
    // Internal scratch state used by random_r 128 is the same size as
    // initstate() uses for regular random(), see manpages for details.
    // statebuf is malloc'ed and this memory is leaked, it could be a __thread
    // buffer, but after running into linker issues with large thread local
    // storage buffers, we thought better.
    enum { STATE_BYTES = 128 };
    static __thread char* statebuf;
    // random_r's state, must be handed to each call, and seems to refer to
    // statebuf in some undocumented way.
    static __thread random_data buf;

    if (statebuf == NULL) {
        int fd = open("/dev/urandom", O_RDONLY);
        if (fd < 0)
            throw FatalError(HERE, "Couldn't open /dev/urandom", errno);
        unsigned int seed;
        ssize_t bytesRead = read(fd, &seed, sizeof(seed));
        close(fd);
        assert(bytesRead == sizeof(seed));
        statebuf = static_cast<char*>(Memory::xmalloc(HERE, STATE_BYTES));
        initstate_r(seed, statebuf, STATE_BYTES, &buf);
    }

    // Each call to random returns 31 bits of randomness,
    // so we need three to get 64 bits of randomness.
    static_assert(RAND_MAX >= (1 << 31), "RAND_MAX too small");
    int32_t lo, mid, hi;
    random_r(&buf, &lo);
    random_r(&buf, &mid);
    random_r(&buf, &hi);
    uint64_t r = (((uint64_t(hi) & 0x7FFFFFFF) << 33) | // NOLINT
                  ((uint64_t(mid) & 0x7FFFFFFF) << 2)  | // NOLINT
                  (uint64_t(lo) & 0x00000003)); // NOLINT
    return r;
}

/**
 * Make generateRandom model RandomNumberGenerator.
 * This makes it easy to mock the calls for randomness during testing
 * since generateRandom supports mock values.
 *
 * \param n
 *      Limits the result to the range [0, n).
 * \return
 *      Returns a pseudo-random number in the range [0, n).
 */
uint32_t
randomNumberGenerator(uint32_t n)
{
    return static_cast<uint32_t>(generateRandom()) % n;
}

/**
 * Pin the calling thread to a particular CPU.
 * \param cpu
 *      The number of the CPU on which to execute, starting from 0.
 * \return
 *      Whether the operation succeeded.
 */
bool
pinToCpu(uint32_t cpu)
{
    cpu_set_t cpus;
    CPU_ZERO(&cpus);
    CPU_SET(cpu, &cpus);
    int r = sched_setaffinity(0, sizeof(cpus), &cpus);
    if (r < 0) {
        LOG(ERROR, "server: Couldn't pin to core %d: %s",
            cpu, strerror(errno));
        return false;
    }
    return true;
}

/**
 * Obtain the total amount of system memory in bytes as reported by
 * /proc/meminfo on Linux.
 * \return
 *      The number of bytes of memory, else 0 on error.
 */
uint64_t
getTotalSystemMemory()
{
    char buf[256];
    FILE *fp;
    uint64_t totalBytes = 0;

    fp = fopen("/proc/meminfo", "r");
    if (fp == NULL)
        return 0;

    while (fgets(buf, sizeof(buf), fp) != NULL) {
        if (strncmp(buf, "MemTotal:", strlen("MemTotal:")) == 0) {
            char *countStr, *units, *savePtr;

            strtok_r(buf, " \t", &savePtr);
            countStr = strtok_r(NULL, " \t", &savePtr);
            totalBytes = strtoull(countStr, NULL, 10);

            // Linux appears to return all memory info in kilobytes, but
            // check to be sure.
            units = strtok_r(NULL, " \t\n", &savePtr);
            if (strcmp(units, "kB") != 0) {
                totalBytes = 0;
                continue;
            }

            totalBytes *= 1024;
            break;
        }
    }

    fclose(fp);

    return totalBytes;
}

/**
 * Helper function to call __cxa_demangle. Has internal linkage.
 * Handles the C-style memory management required.
 * Returns a std::string with the long human-readable name of the
 * type.
 * \param name
 *      The "name" of the type that needs to be demangled.
 * \throw FatalError
 *      The short internal type name could not be converted.
 */
string demangle(const char* name) {
    int status;
    char* res = abi::__cxa_demangle(name,
                                    NULL,
                                    NULL,
                                    &status);
    if (status != 0) {
        throw RAMCloud::
            FatalError(HERE,
                       "cxxabi.h's demangle() could not demangle type");
    }
    // contruct a string with a copy of the C-style string returned.
    string ret(res);
    // __cxa_demangle would have used realloc() to allocate memory
    // which should be freed now.
    free(res);
    return ret;
}

/**
 * Pin all current and future memory pages in memory so that the OS does not
 * swap them to disk. All RAMCloud server main files should call this.
 *
 * Note that future mapping operations (e.g. mmap, stack expansion, etc)
 * may fail if their memory cannot be pinned due to resource limits. Thus the
 * check below may not capture all possible failures up front. It's probably
 * best to call this at the end of initialisation (after most large allocations
 * have been made). This is also a good idea because pinning slows down mmap
 * probing in #LargeBlockOfMemory.
 */
void pinAllMemory() {
    int r = mlockall(MCL_CURRENT | MCL_FUTURE);
    if (r != 0) {
        LOG(WARNING, "Could not lock all memory pages (%s), so the OS might "
                     "swap memory later. Check your user's \"ulimit -l\" and "
                     "adjust /etc/security/limits.conf as necessary.",
                     strerror(errno));
    }
}

} // namespace RAMCloud
