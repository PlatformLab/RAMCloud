/* Copyright (c) 2012 Stanford University
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

#include "MultiIncrement.h"
#include "Object.h"
#include "ShortMacros.h"

namespace RAMCloud {

// Default RejectRules to use if none are provided by the caller: rejects
// nothing.
static RejectRules defaultRejectRules;

/**
 * Constructor for MultiIncrement objects: initiates one or more RPCs for a
 * multiIncrement operation, but returns once the RPCs have been initiated,
 * without waiting for any of them to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this operation.
 * \param requests
 *      Each element in this array describes one object to be incremented.
 * \param numRequests
 *      Number of elements in \c requests.
 */
MultiIncrement::MultiIncrement(RamCloud* ramcloud,
                               MultiIncrementObject* const requests[],
                               uint32_t numRequests)
    : MultiOp(ramcloud, type,
                  reinterpret_cast<MultiOpObject* const *>(requests),
                  numRequests)
{
    startRpcs();
}

/**
 * Append a given MultiIncrementObject to a buffer.
 *
 * It is the responsibility of the caller to ensure that the
 * MultiOpObject passed in is actually a MultiIncrementObject.
 *
 * \param request
 *      MultiIncrementObject request to append
 * \param buf
 *      Buffer to append to
 */
void
MultiIncrement::appendRequest(MultiOpObject* request, Buffer* buf)
{
    MultiIncrementObject* req =
        reinterpret_cast<MultiIncrementObject*>(request);

    // Add the current object to the list of those being
    // written by this RPC.
    buf->emplaceAppend<WireFormat::MultiOp::Request::IncrementPart>(
            req->tableId,
            req->keyLength,
            req->incrementInt64,
            req->incrementDouble,
            req->rejectRules ? *req->rejectRules :
                               defaultRejectRules);

    buf->appendCopy(req->key, req->keyLength);
}

/**
 * Read the MultiIncrement response in the buffer given an offset
 * and put the response into a MultiIncrementObject. This modifies
 * the offset as necessary and checks for missing data.
 *
 * It is the responsibility of the caller to ensure that the
 * MultiOpObject passed in is actually a MultiIncrementObject.
 *
 * \param request
 *      MultiIncrementObject where the interpreted response goes
 * \param buf
 *      Buffer to read the response from
 * \param respOffset
 *      Offset into the buffer for the current position
 *              which will be modified as this method reads.
 *
 * \return
 *      true if there is missing data
 */
bool
MultiIncrement::readResponse(MultiOpObject* request,
                             Buffer* buf,
                             uint32_t* respOffset)
{
    MultiIncrementObject* req =
        reinterpret_cast<MultiIncrementObject*>(request);

    const WireFormat::MultiOp::Response::IncrementPart* part =
        buf->getOffset<
            WireFormat::MultiOp::Response::IncrementPart>(*respOffset);
    if (part == NULL) {
        TEST_LOG("missing Response::Part");
        return true;
    }
    *respOffset += sizeof32(*part);

    req->status = part->status;
    req->version = part->version;
    req->newValue.asDouble = part->newValue.asDouble;
    req->newValue.asInt64 = part->newValue.asInt64;

    return false;
}

} // end RAMCloud
