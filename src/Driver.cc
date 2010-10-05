/* Copyright (c) 2010 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "Driver.h"

namespace RAMCloud {

/// Virtual destructor needed since this serves as an abstract base class.
Driver::~Driver()
{
}

/**
 * Give memory resources back to this driver.
 *
 * \param payload
 *      The address of the resources to give back.
 * \param len
 *      The length in bytes of the resources to give back.
 */
void
Driver::release(char *payload, uint32_t len)
{
}

/// Construct a received containing no data and unassociated with a Driver.
Driver::Received::Received()
    :  sender(NULL)
    , driver(0)
    , len(0)
    , payload(0)
{
}

/**
 * If this Received is associated with a Driver and its payload
 * hasn't been stolen then release the payload data to the Driver
 * allowing it to reclaim the resources.
 */
Driver::Received::~Received()
{
    if (driver && payload)
        driver->release(payload, len);
}

/**
 * Returns a pointer offset bytes into the Received payload if the
 * payload is long enough to fit the requested range of bytes.
 *
 * \param offset
 *      An offset in the the Received payload return a pointer to.
 * \param length
 *      The number of bytes the caller may access starting at offset
 *      into the payload.
 * \return
 *      A pointer inside the Received payload or NULL if any part of
 *      the specified range is beyond the length of the payload.
 */
void*
Driver::Received::getRange(uint32_t offset, uint32_t length)
{
    if (offset + length > len)
        return NULL;
    return static_cast<void*>(payload + offset);
}

/**
 * Return a pointer to the raw data received from the Driver, obligating
 * the caller to return the resources to the Driver using Driver::release()
 * when the resources are no longer in use.
 *
 * This is generally used with PayloadChunk to allow Driver allocated memory
 * to be placed in Buffers and returned to the Driver when the Buffer is no
 * longer in use.
 *
 * \param[out] len
 *      Populated with the length of the packet data.
 * \return
 *      A pointer to the raw packet data.
 */
char*
Driver::Received::steal(uint32_t *len)
{
    char *p = payload;
    payload = NULL;
    *len = this->len;
    this->len = 0;
    return p;
}

} // namespace RAMCloud
