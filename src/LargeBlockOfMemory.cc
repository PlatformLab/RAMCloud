/* Copyright (c) 2011 Stanford University
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

#include "LargeBlockOfMemory.h"

namespace RAMCloud {

namespace LargeBlockOfMemoryInternal {
#if ASAN
    // On Linux/x86_64, the default `SHADOW_OFFSET` of AddressSanitizer
    // is 0x7FFF8000 (< 2G), so there is no enough memory in the `LowMem`
    // region for mmap. As a workaround, set the starting probe base to
    // be the lowest gigabyte-aligned address in the `HighMem` region:
    // [0x10007fff8000, 0x7fffffffffff].
    // See:
    //  https://github.com/google/sanitizers/wiki/AddressSanitizerAlgorithm
    //  https://llvm.org/svn/llvm-project/compiler-rt/trunk/lib/asan/asan-
    //  _mapping.h
    uint64_t nextProbeBase = (uint64_t)16386 << 30;
#elif TSAN
    // Similarly, the starting probe base also has to be set higher in order
    // for ThreadSanitizer to work properly.
    // For more info about the address layout of TSan, see:
    //  https://github.com/google/sanitizers/wiki/ThreadSanitizerAlgorithm
    //  https://llvm.org/svn/llvm-project/compiler-rt/trunk/lib/tsan/rtl/tsan-
    //  _platform.h
    uint64_t nextProbeBase = 0x7f0000000000UL;
#else
    uint64_t nextProbeBase = (uint64_t)1 << 30;
#endif
}

}
