/* Copyright (c) 2011-2016 Stanford University
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

#include "MacAddress.h"

namespace RAMCloud {

/**
 * Create a new address from a string representation.
 * \param macStr
 *      A MAC address like "01:23:45:67:89:ab". Uppercase is also allowed.
 * \throw Exception
 *      The format of the given \a macStr is invalid. If this is thrown,
 *      the contents of the address will be left unmodified.
 */
MacAddress::MacAddress(const char* macStr)
{
    unsigned int bytes[6];
    int r = sscanf(macStr, "%02x:%02x:%02x:%02x:%02x:%02x", // NOLINT
                   &bytes[0], &bytes[1], &bytes[2],
                   &bytes[3], &bytes[4], &bytes[5]);
    if (r != 6 || macStr[17] != '\0')
        throw Exception(HERE, format("Bad MAC address: %s", macStr));
    for (uint32_t i = 0; i < 6; ++i)
        address[i] = downCast<uint8_t>(bytes[i]);
}

/**
 * Generate a random MAC address.
 * Guaranteed to not be a multicast address and in the locally administered mac
 * address range, so you end up with 46 bits of randomness.
 */
MacAddress::MacAddress(Random _)
{
    uint64_t r = generateRandom();
    for (uint32_t i = 0; i < 6; ++i) {
        address[i] = r & 0xFF;
        r >>= 8;
    }
    // set locally administered mac address, unicast
    address[0] &= 0xFC;
    address[0] |= 0x02;
}

uint64_t
MacAddress::getHash() const
{
    // The following code implements the djb2 hash function found at:
    // http://www.cse.yorku.ca/~oz/hash.html.
    uint64_t hash = 5381;
    for (uint8_t c : address) {
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    return hash;
}

inline string
MacAddress::toString() const
{
    char buf[18];
    snprintf(buf, sizeof(buf), "%02x:%02x:%02x:%02x:%02x:%02x",
             address[0], address[1], address[2],
             address[3], address[4], address[5]);
    return buf;
}

} // namespace RAMCloud
