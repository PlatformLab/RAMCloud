/* Copyright (c) 2015-2016 Stanford University
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

#include "ParticipantList.h"

namespace RAMCloud {

/**
 * Construct a ParticipantList from a one that came off the wire.
 *
 * \param participants
 *      Pointer to a contiguous participant list that must stay in scope for the
 *      life of this object.
 * \param participantCount
 *      Number of participants in the list.
 * \param clientLeaseId
 *      Id of the client lease associated with this transaction.
 * \param clientTransactionId
 *      Client provided identifier for this transaction.
 */
ParticipantList::ParticipantList(WireFormat::TxParticipant* participants,
                                 uint32_t participantCount,
                                 uint64_t clientLeaseId,
                                 uint64_t clientTransactionId)
    : header(clientLeaseId, clientTransactionId, participantCount)
    , participants(participants)
{
}

/**
 * Construct an ParticipantList using information in the log, which includes the
 * ParticipantList header. This form of the constructor is typically used for
 * extracting information out of the log (e.g. perform log cleaning).
 *
 * \param buffer
 *      Buffer referring to a complete ParticipantList in the log. It is the
 *      caller's responsibility to make sure that the buffer passed in
 *      actually contains a full ParticipantList. If it does not, then behavior
 *      is undefined.
 * \param offset
 *      Starting offset in the buffer where the object begins.
 */
ParticipantList::ParticipantList(Buffer& buffer, uint32_t offset)
    : header(*buffer.getOffset<Header>(offset))
    , participants(NULL)
{
    participants = (WireFormat::TxParticipant*)buffer.getRange(
            offset + sizeof32(header),
            sizeof32(WireFormat::TxParticipant) * header.participantCount);
}

/**
 * Append the full ParticipantList to a buffer.
 *
 * \param buffer
 *      The buffer to which to append a serialized version of this
 *      ParticipantList.
 */
void
ParticipantList::assembleForLog(Buffer& buffer)
{
    header.checksum = computeChecksum();
    buffer.appendExternal(&header, sizeof32(Header));
    buffer.appendExternal(participants, sizeof32(WireFormat::TxParticipant)
                                                * header.participantCount);
}

/**
 * Compute a checksum on the ParticipantList and determine whether or not it
 * matches what is stored in the object.
 *
 * \return
 *      True if the checksum looks OK; false otherwise.
 */
bool
ParticipantList::checkIntegrity()
{
    return computeChecksum() == header.checksum;
}

/**
 * Compute the ParticipantList's checksum and return it.
 */
uint32_t
ParticipantList::computeChecksum()
{
    Crc32C crc;
    // first compute the checksum on the header excluding the checksum field
    crc.update(this,
               downCast<uint32_t>(sizeof(header) -
               sizeof(header.checksum)));
    // compute the checksum of the list itself
    crc.update(participants,
           sizeof32(WireFormat::TxParticipant) *
           header.participantCount);

    return crc.getResult();
}

} // namespace RAMCloud
