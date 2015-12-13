/* Copyright (c) 2010-2015 Stanford University
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

// RAMCloud pragma [CPPLINT=0]

#ifndef RAMCLOUD_CRC32C_H
#define RAMCLOUD_CRC32C_H

#include "Buffer.h"
#include "Exception.h"

/// Lookup tables for software CRC32C implementation.
namespace Crc32CSlicingBy8 {
    extern const uint32_t crc_tableil8_o32[256];
    extern const uint32_t crc_tableil8_o40[256];
    extern const uint32_t crc_tableil8_o48[256];
    extern const uint32_t crc_tableil8_o56[256];
    extern const uint32_t crc_tableil8_o64[256];
    extern const uint32_t crc_tableil8_o72[256];
    extern const uint32_t crc_tableil8_o80[256];
    extern const uint32_t crc_tableil8_o88[256];
}

namespace RAMCloud {

/// See #Crc32C().
static inline uint32_t
intelCrc32C(uint32_t crc, const void* buffer, uint64_t bytes)
{
#if __SSE4_2__
#define CRC32Q __builtin_ia32_crc32di /* 8 bytes */
#define CRC32L __builtin_ia32_crc32si /* 4 bytes */
#define CRC32W __builtin_ia32_crc32hi /* 2 bytes */
#define CRC32B __builtin_ia32_crc32qi /* 1 byte */
    const uint64_t* p64 = static_cast<const uint64_t*>(buffer);
    uint64_t remainder = bytes;
    uint64_t chunk32 = 0;
    uint64_t chunk8 = 0;

    // Do unrolled 32-byte chunks first, 8-bytes at a time.
    chunk32 = remainder >> 5;
    remainder &= 31;
    while (chunk32-- > 0) {
        crc = downCast<uint32_t>(CRC32Q(crc, p64[0]));
        crc = downCast<uint32_t>(CRC32Q(crc, p64[1]));
        crc = downCast<uint32_t>(CRC32Q(crc, p64[2]));
        crc = downCast<uint32_t>(CRC32Q(crc, p64[3]));
        p64 += 4;
    }

    // Next, do any remaining 8-byte chunks.
    chunk8 = remainder >> 3;
    remainder &= 7;
    while (chunk8-- > 0) {
        crc = downCast<uint32_t>(CRC32Q(crc, p64[0]));
        p64++;
    }

    // Next, do any remaining 2-byte chunks.
    const uint16_t* p16 = reinterpret_cast<const uint16_t*>(p64);
    uint64_t chunk2 = remainder >> 1;
    remainder &= 1;
    while (chunk2-- > 0) {
        crc = downCast<uint32_t>(CRC32W(crc, p16[0]));
        p16++;
    }

    // Finally, do any remaining byte.
    if (remainder) {
        const uint8_t* p8 = reinterpret_cast<const uint8_t*>(p16);
        crc = downCast<uint32_t>(CRC32B(crc, p8[0]));
    }
#undef CRC32B
#undef CRC32W
#undef CRC32L
#undef CRC32Q
#else
    throw FatalError(HERE, "SSE 4.2 was not enabled at compile-time");
#endif /* __SSE4_2__ */
    return crc;
}

/// See #Crc32C().
static inline uint32_t
softwareCrc32C(uint32_t crc, const void* data, uint64_t length)
{
    // This following header applies to this function only. The LICENSE file
    // referred to in the first header block was not found in the archive.
    // This is from http://evanjones.ca/crc32c.html.

    // Copyright 2008,2009,2010 Massachusetts Institute of Technology.
    // All rights reserved. Use of this source code is governed by a
    // BSD-style license that can be found in the LICENSE file.
    //
    // Implementations adapted from Intel's Slicing By 8 Sourceforge Project
    // http://sourceforge.net/projects/slicing-by-8/

    // Copyright (c) 2004-2006 Intel Corporation - All Rights Reserved
    //
    // This software program is licensed subject to the BSD License, 
    // available at http://www.opensource.org/licenses/bsd-license.html

    using namespace Crc32CSlicingBy8; // NOLINT
    const char* p_buf = (const char*) data;

    // Handle leading misaligned bytes
    size_t initial_bytes = (sizeof(int32_t) - (intptr_t)p_buf) & (sizeof(int32_t) - 1);
    if (length < initial_bytes) initial_bytes = length;
    for (size_t li = 0; li < initial_bytes; li++) {
        crc = crc_tableil8_o32[(crc ^ *p_buf++) & 0x000000FF] ^ (crc >> 8);
    }

    length -= initial_bytes;
    size_t running_length = length & ~(sizeof(uint64_t) - 1);
    size_t end_bytes = length - running_length; 

    for (size_t li = 0; li < running_length/8; li++) {
        crc ^= *(const uint32_t*) p_buf;
        p_buf += 4;
        uint32_t term1 = crc_tableil8_o88[crc & 0x000000FF] ^
                crc_tableil8_o80[(crc >> 8) & 0x000000FF];
        uint32_t term2 = crc >> 16;
        crc = term1 ^
              crc_tableil8_o72[term2 & 0x000000FF] ^ 
              crc_tableil8_o64[(term2 >> 8) & 0x000000FF];
        term1 = crc_tableil8_o56[(*(const uint32_t *)p_buf) & 0x000000FF] ^
                crc_tableil8_o48[((*(const uint32_t *)p_buf) >> 8) & 0x000000FF];

        term2 = (*(const uint32_t *)p_buf) >> 16;
        crc = crc ^ term1 ^
                crc_tableil8_o40[term2  & 0x000000FF] ^
                crc_tableil8_o32[(term2 >> 8) & 0x000000FF];
        p_buf += 4;
    }

    for (size_t li=0; li < end_bytes; li++) {
        crc = crc_tableil8_o32[(crc ^ *p_buf++) & 0x000000FF] ^ (crc >> 8);
    }

    return crc;
}

/**
 * Compute a CRC32C (i.e. CRC32 with the Castagnoli polynomial, which
 * is used in iSCSI, among other protocols).
 *
 * This also presets the value to -1 and inverts it after calculation to better
 * detect leading and trailing bits (see, e.g., Wikipedia).
 *
 * This function uses the "crc32" instruction found in Intel Nehalem and later
 * processors. On processors without that instruction, it calculates the same
 * function much more slowly in software (just under 400 MB/sec in software vs
 * just under 2000 MB/sec in hardware on Westmere boxes).
 */
class Crc32C {
  public:
    /**
     * Type returned by #getResult(). Use this rather than using the integer
     * type directly to make it easier to swap out checksum classes.
     */
    typedef uint32_t ResultType;

    Crc32C(bool forceSoftware=false)
        : useHardware(!forceSoftware && haveHardware)
        , result(-1)
    {
    }

    /**
     * Assignment simply copies the accumulated result thus far.
     */
    Crc32C&
    operator=(const Crc32C& other)
    {
        result = other.result;
        return *this;
    }

    /**
     * Update the accumulated checksum.
     * \param[in] buffer
     *      A pointer to the memory to be checksummed.
     * \param[in] bytes
     *      The number of bytes of memory to checksum.
     * \return
     *      A reference to this instance for chaining calls.
     */
    Crc32C&
    update(const void* buffer, uint32_t bytes)
    {
        result = useHardware ? intelCrc32C(result, buffer, bytes)
                             : softwareCrc32C(result, buffer, bytes);
        return *this;
    }

    /**
     * Update the accumulated checksum.
     * \param[in] buffer
     *      Buffer describing the data to be checksummed.
     * \param[in] offset 
     *      Offset within the buffer to begin checksumming at.
     * \param[in] bytes
     *      The number of bytes in the buffer to checksum.
     * \return
     *      A reference to this instance for chaining calls.
     */
    Crc32C&
    update(Buffer& buffer, uint32_t offset, uint32_t bytes)
    {
        Buffer::Iterator it(&buffer, offset, bytes);
        while (!it.isDone()) {
            update(it.getData(), it.getLength());
            it.next();
        }
        return *this;
    }

    /**
     * Update the accumulated checksum.
     * \param[in] buffer
     *      Buffer describing the data to be checksummed. All bytes within the
     *      buffer will be checksummed.
     * \return
     *      A reference to this instance for chaining calls.
     */
    Crc32C&
    update(Buffer& buffer)
    {
        return update(buffer, 0, buffer.size());
    }

    /**
     * Return the accumulated checksum.
     */
    ResultType getResult() const {
        return ~result;
    }

  PRIVATE:
    /// Whether this machine has Intel's CRC32C instruction.
    static bool haveHardware;

    /// Whether this checksum instance should use Intel's CRC32C instruction.
    bool useHardware;

    /// The accumulated checksum before inversion.
    uint32_t result;
};

} // namespace RAMCloud

#endif // !RAMCLOUD_CRC32C_H
