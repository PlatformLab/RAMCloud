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

#include "MultiRemove.h"
#include "ShortMacros.h"

namespace RAMCloud {

// Default RejectRules to use if none are provided by the caller: rejects
// nothing.
static RejectRules defaultRejectRules;

/**
 * Constructor for MultiRemove objects: initiates one or more RPCs for a
 * multiRemove operation, but returns once the RPCs have been initiated,
 * without waiting for any of them to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this operation.
 * \param requests
 *      Each element in this array describes one object to remove.
 * \param numRequests
 *      Number of elements in \c requests.
 */
MultiRemove::MultiRemove(RamCloud* ramcloud,
                         MultiRemoveObject* const requests[],
                         uint32_t numRequests)
        : MultiOp(ramcloud, type,
                  reinterpret_cast<MultiOpObject* const *>(requests),
                  numRequests)
{
    startRpcs();
}
/**
 * Append a given MultiRemoveObject to a buffer.
 *
 * It is the responsibility of the caller to ensure that the
 * MultiOpObject passed in is actually a MultiRemoveObject.
 *
 * \param request
 *      MultiRemoveObject request to append.
 * \param buf
 *      Buffer to append to.
 */
void
MultiRemove::appendRequest(MultiOpObject* request, Buffer* buf)
{
    MultiRemoveObject* req = reinterpret_cast<MultiRemoveObject*>(request);

    // Add the current object to the list of those being
    // fetched by this RPC.
    buf->emplaceAppend<WireFormat::MultiOp::Request::RemovePart>(
            req->tableId,
            req->keyLength,
            req->rejectRules ? *req->rejectRules :
                               defaultRejectRules);
    buf->appendCopy(req->key, req->keyLength);
}

/**
 * Read the MultiRemove response in the buffer given an offset
 * and put the response into a MultiRemoveObject. This modifies
 * the offset as necessary and checks for missing data.
 *
 * It is the responsibility of the caller to ensure that the
 * MultiOpObject passed in is actually a MultiRemoveObject.
 *
 * \param request
 *      MultiRemoveObject where the interpreted response goes.
 * \param response
 *      Buffer to read the response from.
 * \param respOffset
 *      offset into the buffer for the current position
 *      which will be modified as this method reads.
 *
 * \return
 *      true if there is missing data
 */
bool
MultiRemove::readResponse(MultiOpObject* request,
                          Buffer* response,
                          uint32_t* respOffset)
{
    MultiRemoveObject* req = reinterpret_cast<MultiRemoveObject*>(request);

    const WireFormat::MultiOp::Response::RemovePart* part =
        response->getOffset<
            WireFormat::MultiOp::Response::RemovePart>(*respOffset);
    if (part == NULL) {
        TEST_LOG("missing Response::Part");
        return true;
    }
    *respOffset += sizeof32(*part);

    req->status = part->status;
    req->version = part->version;

    return false;
}

} // RAMCloud

