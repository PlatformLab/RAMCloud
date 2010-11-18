/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_CRC32C_H
#define RAMCLOUD_CRC32C_H

#include <inttypes.h>

namespace RAMCloud {

/*
 * Note that we use hex opcodes to avoid issues with older versions
 * of the GNU assembler that do not recognise the instruction.
 */

// crc32q %rdx, %rcx
#define CRC32Q(_crc, _p64, _off) \
    __asm__ __volatile__(".byte 0xf2, 0x48, 0x0f, 0x38, 0xf1, 0xca" : \
        "=c"(_crc) : "c"(_crc), "d"(*(_p64 + _off)))

// crc32l %edx, %ecx
#define CRC32L(_crc, _p32, _off) \
    __asm__ __volatile__(".byte 0xf2, 0x0f, 0x38, 0xf1, 0xca" : \
        "=c"(_crc) : "c"(_crc), "d"(*(_p32 + _off)))

// crc32w %dx, %ecx
#define CRC32W(_crc, _p16, _off) \
    __asm__ __volatile__(".byte 0x66, 0xf2, 0x0f, 0x38, 0xf1, 0xca" : \
        "=c"(_crc) : "c"(_crc), "d"(*(_p16 + _off)))

// crc32b %dl, %ecx
#define CRC32B(_crc, _p8, _off) \
    __asm__ __volatile__(".byte 0xf2, 0x0f, 0x38, 0xf0, 0xca" : \
        "=c"(_crc) : "c"(_crc), "d"(*(_p8 + _off)))

/**
 * Compute a CRC32C (i.e. CRC32 with the Castagnoli polynomial, which
 * is used in iSCSI, among other protocols).
 *
 * This function uses the "crc32" instruction found in Intel Nehalem
 * and later processors.
 *
 * \param[in] crc
 *      CRC to accumulate. The return value of this function can be
 *      passed to future calls as this parameter to update a CRC
 *      with multiple invocations. 
 * \param[in] buffer
 *      A pointer to the memory to be checksummed.
 * \param[in] bytes
 *      The number of bytes of memory to checksum.
 * \return
 *      The CRC32C associated with the input parameters.
 */
static inline uint32_t
Crc32C(uint32_t crc, const void* buffer, uint64_t bytes)
{
    const uint64_t* p64 = static_cast<const uint64_t*>(buffer);
    uint64_t remainder = bytes;
    uint64_t chunk32 = 0;
    uint64_t chunk8 = 0;

    // Do unrolled 32-byte chunks first, 8-bytes at a time.
    chunk32 = remainder >> 5;
    remainder &= 31;
    while (chunk32-- > 0) {
        CRC32Q(crc, p64, 0);
        CRC32Q(crc, p64, 1);
        CRC32Q(crc, p64, 2);
        CRC32Q(crc, p64, 3);
        p64 += 4;
    }

    // Next, do any remaining 8-byte chunks.
    chunk8 = remainder >> 3;
    remainder &= 7;
    while (chunk8-- > 0) {
        CRC32Q(crc, p64, 0);
        p64++;
    }

    // Next, do any remaining 2-byte chunks.
    const uint16_t* p16 = reinterpret_cast<const uint16_t*>(p64);
    uint64_t chunk2 = remainder >> 1;
    remainder &= 1;
    while (chunk2-- > 0) {
        CRC32W(crc, p16, 0);
        p16++;
    }

    // Finally, do any remaining byte.
    if (remainder) {
        const uint8_t* p8 = reinterpret_cast<const uint8_t*>(p16);
        CRC32B(crc, p8, 0);
    }

    return crc;
}

} // namespace RAMCloud

#endif // !RAMCLOUD_CRC32C_H
