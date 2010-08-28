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

/**
 * \file
 * Implementation of RAMCloud::ClientException.
 */

#include <ClientException.h>

namespace RAMCloud {

/**
 * Construct an exception that reflects a particular completion status.
 *
 * \param status
 *      Identifies a problem that occurred in a RAMCloud request.
 */
ClientException::ClientException(Status status)
        : status(status)
{
    // Constructor is empty.
}

/**
 * Destructor for ClientExceptions.
 */
ClientException::~ClientException()
{
    // Destructor is empty.
}

/**
 * Given a Status value, generate a ClientException appropriate for that
 * status and throw it. This method never returns.
 *
 * \param status
 *      Identifies a problem that occurred in a RAMCloud request.
 *
 * \exception ClientException
 *      The class of the generated exception will be a subclass of
 *      ClientException, depending on the value of status.
 */
void
ClientException::throwException(Status status)
{
    switch (status) {
        case STATUS_OK:
            // Not clear that this case really makes sense (throw
            // an exception to indicate success?) but it is here
            // for completeness.
            throw Success();
        case STATUS_TABLE_DOESNT_EXIST:
            throw TableDoesntExistException();
        case STATUS_OBJECT_DOESNT_EXIST:
            throw ObjectDoesntExistException();
        case STATUS_OBJECT_EXISTS:
            throw ObjectExistsException();
        case STATUS_WRONG_VERSION:
            throw WrongVersionException();
        case STATUS_NO_TABLE_SPACE:
            throw NoTableSpaceException();
        case STATUS_MESSAGE_TOO_SHORT:
            throw MessageTooShortError();
        case STATUS_UNIMPLEMENTED_REQUEST:
            throw UnimplementedRequestError();
        case STATUS_REQUEST_FORMAT_ERROR:
            throw RequestFormatError();
        case STATUS_RESPONSE_FORMAT_ERROR:
            throw ResponseFormatError();
        case STATUS_COULDNT_CONNECT:
            throw CouldntConnectException();
        default:
            throw InternalError(status);
    }
}

/**
 * Return a human-readable string describing an exception.
 *
 * \return
 *      See above.
 */
const char*
ClientException::toString()
{
    return statusToString(status);
}

/**
 * Return the symbolic name for the completion status
 * represented by this exception.  The symbolic name is the C++
 * name for this status, as defined by the Status enum.
 *
 * \return
 *      See above.
 */
const char*
ClientException::toSymbol()
{
    return statusToSymbol(status);
}

}  // namespace RAMCloud
