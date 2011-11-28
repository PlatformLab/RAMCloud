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

/**
 * \file
 * This file defines a 64-bit unique identifier for RAMCloud server processes.
 */

#ifndef RAMCLOUD_SERVERID_H
#define RAMCLOUD_SERVERID_H

namespace RAMCloud {

/**
 * This class defines the 64-bit identifier used to reference a specific
 * instance of a RAMCloud server process. While ServiceLocators essentially
 * identify a physical server machine and some arbitrary process running
 * on it, ServerIds identify an exact process that has registered itself
 * with the coordinator. If that process crashes and another is restarted
 * with the same ServiceLocator, then the ServerId will be different.
 *
 * ServerIds are useful in that they're:
 *  1) Concise - They fit in a single 64-bit register.
 *  2) Precise - There is no ambiguity in who you're communicating with
 *               when opening a Transport::Session based on ServerId.
 *
 * Since ServerIds don't contain any network information they can't be used
 * by themselves to route RPCs. Instead, ServerIds map to ServiceLocators.
 * To make lookups especially efficient, ServerIds are designed to be densely
 * allocated. Each ServerId consists of a 32-bit index part and a 32-bit
 * generation number. When the coordinator allocates ServerIds, it uses the
 * lowest free index and increments the generation number for that index.
 * If that server dies, the index is freed, but they generation number may
 * never be used again for that slot. This dense allocation scheme means
 * that looking up based on a ServerId can be done by indexing into a vector
 * and checking the generation number, rather than going through a hash table.
 * Admittedly the performance gain is small, but then so is the implementation
 * complexity.
 */
class ServerId {
  PUBLIC:
    /**
     * The default constructor creates an invalid ServerId. Useful when
     * allocating arrays of these that should be uninitialised by default.
     */
    ServerId()
        : serverId(INVALID_SERVERID_U64)
    {
    }

    /**
     * Given a ServerId in uint64_t form, construct a wrapper ServerId object.
     * This can be used to extract the index and generation numbers from a
     * serialised identifier.
     */
    explicit ServerId(uint64_t id)
        : serverId(id)
    {
    }

    /**
     * Construct a ServerId that has the given index and generation numbers.
     *
     * \param indexNumber
     *      The index number of this ServerId. This is the reusable portion
     *      of the address space.
     *
     * \param generationNumber
     *      The generation number for this ServerId's index. If this ServerId
     *      represents a new server, then the generationNumber should be higher
     *      than any previous ServerId with the same indexNumber.
     */
    ServerId(uint32_t indexNumber, uint32_t generationNumber)
        : serverId(static_cast<uint64_t>(generationNumber) << 32 |
                   indexNumber)
    {
    }

    /**
     * Obtain the ServerId's uint64_t serialised form.
     */
    uint64_t
    getId() const
    {
        return serverId;
    }

    /**
     * Obtain the index number for this ServerId.
     */
    uint32_t
    indexNumber() const
    {
        return serverId & 0xffffffffUL;
    }

    /**
     * Obtain the generation number for this ServerId.
     */
    uint32_t
    generationNumber() const
    {
        return downCast<uint32_t>(serverId >> 32);
    }

    /**
     * Test the equality of two ServerIds.
     */
    bool
    operator==(const ServerId& other) const
    {
        return serverId == other.serverId;
    }

    /**
     * Test the inequality of two ServerIds.
     */
    bool
    operator!=(const RAMCloud::ServerId& other) const
    {
        return !operator==(other);
    }

    /**
     * Assignment of one ServerId to another.
     */
    ServerId&
    operator=(const RAMCloud::ServerId& other)
    {
        serverId = other.serverId;
        return *this;
    }

    /// Integer representing an invalid ServerId. This value must never be
    /// allocated to a server process.
    enum { INVALID_SERVERID_U64 = (uint64_t)-1 };   // NOLINT

    /// The invalid ServerId object to compare against to test validity.
    static const ServerId INVALID_SERVERID;

  PRIVATE:
    /// The uint64_t representation of this ServerId.
    uint64_t serverId;
};

} // namespace RAMCloud

#endif // !RAMCLOUD_SERVERID_H
