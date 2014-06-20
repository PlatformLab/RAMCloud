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

#include "MultiWrite.h"
#include "Object.h"
#include "ShortMacros.h"

namespace RAMCloud {

// Default RejectRules to use if none are provided by the caller: rejects
// nothing.
static RejectRules defaultRejectRules;

/**
 * Constructor for MultiWrite objects: initiates one or more RPCs for a
 * multiWrite operation, but returns once the RPCs have been initiated,
 * without waiting for any of them to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this operation.
 * \param requests
 *      Each element in this array describes one object to write.
 * \param numRequests
 *      Number of elements in \c requests.
 */
MultiWrite::MultiWrite(RamCloud* ramcloud,
                       MultiWriteObject* const requests[],
                       uint32_t numRequests)
    : MultiOp(ramcloud, type,
                  reinterpret_cast<MultiOpObject* const *>(requests),
                  numRequests)
{
    startRpcs();
}

/**
 * Append a given MultiWriteObject to a buffer.
 *
 * It is the responsibility of the caller to ensure that the
 * MultiOpObject passed in is actually a MultiWriteObject.
 *
 * \param request
 *      MultiWriteObject request to append
 * \param buf
 *      Buffer to append to
 */
void
MultiWrite::appendRequest(MultiOpObject* request, Buffer* buf)
{
    MultiWriteObject* req = reinterpret_cast<MultiWriteObject*>(request);

    // Add the current object to the list of those being
    // written by this RPC.

    uint32_t keysAndValueLength = 0;
    WireFormat::MultiOp::Request::WritePart* writeHdr =
            buf->emplaceAppend<WireFormat::MultiOp::Request::WritePart>(
                req->tableId,
                keysAndValueLength,
                req->rejectRules ? *req->rejectRules :
                                  defaultRejectRules);
    if (req->numKeys == 1) {
        Key primaryKey(req->tableId, req->key, req->keyLength);
        Object::appendKeysAndValueToBuffer(primaryKey, req->value,
                                           req->valueLength, *buf,
                                           &keysAndValueLength);
    } else {
        // req->key will be NULL in this case. THe primary key will instead
        // be the first entry in req->keyInfo.
        Object::appendKeysAndValueToBuffer(req->tableId, req->numKeys,
                    req->keyInfo, req->value, req->valueLength, *buf,
                    &keysAndValueLength);
    }
    // update the length value in the header
    writeHdr->length = keysAndValueLength;
}

/**
 * Read the MultiWrite response in the buffer given an offset
 * and put the response into a MultiWriteObject. This modifies
 * the offset as necessary and checks for missing data.
 *
 * It is the responsibility of the caller to ensure that the
 * MultiOpObject passed in is actually a MultiWriteObject.
 *
 * \param request
 *      MultiWriteObject where the interpreted response goes
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
MultiWrite::readResponse(MultiOpObject* request,
                         Buffer* buf,
                         uint32_t* respOffset)
{
    MultiWriteObject* req = reinterpret_cast<MultiWriteObject*>(request);

    const WireFormat::MultiOp::Response::WritePart* part =
        buf->getOffset<
            WireFormat::MultiOp::Response::WritePart>(*respOffset);
    if (part == NULL) {
        TEST_LOG("missing Response::Part");
        return true;
    }
    *respOffset += sizeof32(*part);

    req->status = part->status;
    req->version = part->version;

    return false;
}

} // end RAMCloud

