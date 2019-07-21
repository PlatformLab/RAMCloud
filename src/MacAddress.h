/* Copyright (c) 2011-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */


#ifndef RAMCLOUD_MACADDRESS_H
#define RAMCLOUD_MACADDRESS_H

#include "Driver.h"

namespace RAMCloud {

/**
 * A container for an Ethernet hardware address.
 */
struct MacAddress : public Driver::Address {
    enum Random { RANDOM };

    /**
     * Create a new address from 6 bytes.
     * \param raw
     *      The raw bytes.
     */
    explicit MacAddress(const uint8_t raw[6])
    {
        copy(address, raw);
    }

    MacAddress(const MacAddress& other)
        : MacAddress(other.address) {}

    explicit MacAddress(const char* macStr);
    explicit MacAddress(Random _);

    bool equal(const MacAddress& other) const
    {
        return (*(const uint32_t*)(address + 0) ==
                *(const uint32_t*)(other.address + 0)) &&
               (*(const uint16_t*)(address + 4) ==
                *(const uint16_t*)(other.address + 4));
    }

    static void
    copy(uint8_t dst[6], const uint8_t src[6])
    {
        // Hand-optimized version of memcpy(dst, src, 6).
        *((uint32_t*)(dst + 0)) = *((const uint32_t*)(src + 0)); // NOLINT
        *((uint16_t*)(dst + 4)) = *((const uint16_t*)(src + 4)); // NOLINT
    }

    uint64_t getHash() const;
    string toString() const;

    /// The raw bytes of the MAC address.
    uint8_t address[6];
};

} // end RAMCloud

#endif  // RAMCLOUD_MACADDRESS_H
