/* Copyright (c) 2010-2014 Stanford University
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

#include "ClientException.h"

namespace RAMCloud {

/**
 * Construct an exception that reflects a particular completion status.
 *
 * \param where
 *      Pass #HERE here.
 * \param status
 *      Identifies a problem that occurred in a RAMCloud request.
 */
ClientException::ClientException(const CodeLocation& where, Status status)
        : status(status)
        , where(where)
        , whatCache()
{
    // Constructor is empty.
}

ClientException::ClientException(const ClientException& other)
        : status(other.status)
        , where(other.where)
        , whatCache()
{
    // Constructor is empty.
}

ClientException&
ClientException::operator=(const ClientException& other)
{
    status = other.status;
    where = other.where;
    whatCache.release();
    return *this;
}

/**
 * Destructor for ClientExceptions.
 */
ClientException::~ClientException() throw()
{
    // Destructor is empty.
}

/**
 * Given a Status value, generate a ClientException appropriate for that
 * status and throw it. This method never returns.
 *
 * \param where
 *      Pass #HERE here.
 * \param status
 *      Identifies a problem that occurred in a RAMCloud request.
 *
 * \exception ClientException
 *      The class of the generated exception will be a subclass of
 *      ClientException, depending on the value of status.
 */
void
ClientException::throwException(const CodeLocation& where, Status status)
{
    switch (status) {
        case STATUS_OK:
            // Not clear that this case really makes sense (throw
            // an exception to indicate success?) but it is here
            // for completeness.
            throw Success(where);
        case STATUS_UNKNOWN_TABLET:
            throw UnknownTabletException(where);
        case STATUS_TABLE_DOESNT_EXIST:
            throw TableDoesntExistException(where);
        case STATUS_OBJECT_DOESNT_EXIST:
            throw ObjectDoesntExistException(where);
        case STATUS_OBJECT_EXISTS:
            throw ObjectExistsException(where);
        case STATUS_WRONG_VERSION:
            throw WrongVersionException(where);
        case STATUS_NO_TABLE_SPACE:
            throw NoTableSpaceException(where);
        case STATUS_MESSAGE_TOO_SHORT:
            throw MessageTooShortError(where);
        case STATUS_UNIMPLEMENTED_REQUEST:
            throw UnimplementedRequestError(where);
        case STATUS_REQUEST_FORMAT_ERROR:
            throw RequestFormatError(where);
        case STATUS_RESPONSE_FORMAT_ERROR:
            throw ResponseFormatError(where);
        case STATUS_COULDNT_CONNECT:
            throw CouldntConnectException(where);
        case STATUS_BACKUP_BAD_SEGMENT_ID:
            throw BackupBadSegmentIdException(where);
        case STATUS_BACKUP_OPEN_REJECTED:
            throw BackupOpenRejectedException(where);
        case STATUS_BACKUP_SEGMENT_OVERFLOW:
            throw BackupSegmentOverflowException(where);
        case STATUS_BACKUP_MALFORMED_SEGMENT:
            throw BackupMalformedSegmentException(where);
        case STATUS_SEGMENT_RECOVERY_FAILED:
            throw SegmentRecoveryFailedException(where);
        case STATUS_RETRY:
            throw RetryException(where);
        case STATUS_SERVICE_NOT_AVAILABLE:
            throw ServiceNotAvailableException(where);
        case STATUS_TIMEOUT:
            throw TimeoutException(where);
        case STATUS_SERVER_NOT_UP:
            throw ServerNotUpException(where);
         case STATUS_INVALID_OBJECT:
            throw InvalidObjectException(where);
        case STATUS_TABLET_DOESNT_EXIST:
            throw TabletDoesntExistException(where);
        case STATUS_INTERNAL_ERROR:
            throw InternalError(where, status);
        case STATUS_PARTITION_BEFORE_READ:
            throw PartitionBeforeReadException(where);
        case STATUS_WRONG_SERVER:
            throw WrongServerException(where);
        case STATUS_CALLER_NOT_IN_CLUSTER:
            throw CallerNotInClusterException(where);
        case STATUS_REQUEST_TOO_LARGE:
            throw RequestTooLargeException(where);
        case STATUS_UNKNOWN_INDEXLET:
            throw UnknownIndexletException(where);
        case STATUS_UNKNOWN_INDEX:
            throw UnknownIndexException(where);
        case STATUS_INVALID_PARAMETER:
            throw InvalidParameterException(where);
        default:
            throw InternalError(where, status);
    }
}

/**
 * Return a human-readable string describing an exception.
 *
 * \return
 *      See above.
 */
const char*
ClientException::toString() const
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
ClientException::toSymbol() const
{
    return statusToSymbol(status);
}

string
ClientException::str() const
{
    return format("%s thrown at %s", toString(), where.str().c_str());
}

const char*
ClientException::what() const throw()
{
    if (whatCache)
        return whatCache.get();
    string s(str());
    char* cStr = new char[s.length() + 1];
    whatCache.reset(const_cast<const char*>(cStr));
    memcpy(cStr, s.c_str(), s.length() + 1);
    return cStr;
}

}  // namespace RAMCloud
