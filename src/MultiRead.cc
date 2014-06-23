/* Copyright (c) 2012-2014 Stanford University
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

#include "MultiRead.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Constructor for MultiRead objects: initiates one or more RPCs for a
 * multiRead operation, but returns once the RPCs have been initiated,
 * without waiting for any of them to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this operation.
 * \param requests
 *      Each element in this array describes one object to read and
 *      where to put the value.
 * \param numRequests
 *      Number of elements in \c requests.
 */
MultiRead::MultiRead(RamCloud* ramcloud,
                     MultiReadObject* const requests[],
                     uint32_t numRequests)
        : MultiOp(ramcloud, type,
                  reinterpret_cast<MultiOpObject* const *>(requests),
                  numRequests)
{
    for (uint32_t i = 0; i < numRequests; i++) {
        requests[i]->value->destroy();
    }

    startRpcs();
}
/**
 * Append a given MultiReadObject to a buffer.
 *
 * It is the responsibility of the caller to ensure that the
 * MultiOpObject passed in is actually a MultiReadObject.
 *
 * \param request
 *      MultiReadObject request to append
 * \param buf
 *      Buffer to append to
 */
void
MultiRead::appendRequest(MultiOpObject* request, Buffer* buf)
{
    MultiReadObject* req = reinterpret_cast<MultiReadObject*>(request);

    // Add the current object to the list of those being
    // fetched by this RPC.
    buf->emplaceAppend<WireFormat::MultiOp::Request::ReadPart>(
            req->tableId, req->keyLength);
    buf->appendCopy(req->key, req->keyLength);
}

/**
 * Read the MultiRead response in the buffer given an offset
 * and put the response into a MultiReadObject. This modifies
 * the offset as necessary and checks for missing data.
 *
 * It is the responsibility of the caller to ensure that the
 * MultiOpObject passed in is actually a MultiReadObject.
 *
 * \param request
 *      MultiReadObject where the interpreted response goes
 * \param response
 *      Buffer to read the response from
 * \param respOffset
 *      offset into the buffer for the current position
 *              which will be modified as this method reads.
 *
 * \return
 *      true if there is missing data
 */
bool
MultiRead::readResponse(MultiOpObject* request,
                         Buffer* response,
                         uint32_t* respOffset)
{
    MultiReadObject* req = static_cast<MultiReadObject*>(request);
    const WireFormat::MultiOp::Response::ReadPart* part =
        response->getOffset<
                WireFormat::MultiOp::Response::ReadPart>(*respOffset);

    if (part == NULL) {
        TEST_LOG("missing Response::Part");
        return true;
    }

    req->status = part->status;
    *respOffset += sizeof32(*part);

    if (part->status == STATUS_OK) {
        if (response->size() < *respOffset + part->length) {
            TEST_LOG("missing object data");
            return true;
        }

        req->value->construct();
        void* data = req->value->get()->alloc(part->length);
        response->copy(*respOffset, part->length, data);
        req->version = part->version;
        *respOffset += part->length;
    }

    return false;
}

} // RAMCloud

