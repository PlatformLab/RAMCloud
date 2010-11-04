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

#include "Client.h"
#include "ClientException.h"

namespace RAMCloud {

/**
 * Generate an appropriate exception when an RPC response arrives that
 * is too short to hold the full response header expected for this RPC.
 * If this method is invoked it means the server found a problem before
 * dispatching to a type-specific handler (e.g. the server didn't understand
 * the RPC's type, or basic authentication failed).
 *
 * \param response
 *      Contains the full response message from the server.
 *
 * \exception ClientException
 *      This method always throws an exception; it never returns.
 *      The exact type of the exception will depend on the status
 *      value present in the packet (if any).
 */
void
Client::throwShortResponseError(Buffer& response)
{
    const RpcResponseCommon* common = response.getStart<RpcResponseCommon>();
    if (common != NULL) {
        counterValue = common->counterValue;
        if (common->status == STATUS_OK) {
            // This makes no sense: the server claims to have handled
            // the RPC correctly, but it didn't return the right size
            // response for this RPC; report an error.
            status = STATUS_RESPONSE_FORMAT_ERROR;
        } else {
            status = common->status;
        }
    } else {
        // The packet wasn't even long enough to hold a standard header.
        status = STATUS_RESPONSE_FORMAT_ERROR;
    }
    ClientException::throwException(HERE, status);
}
} // namespace RAMCloud
