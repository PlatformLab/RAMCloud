/* Copyright (c) 2010-2015 Stanford University
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
 * Defines all of the exceptions that are visible to RAMCloud clients.
 * There is one exception class for each Status value, plus additional
 * super-classes corresponding to related groups of exceptions, such as
 * RejectRule failures or internal errors.
 */

#ifndef RAMCLOUD_CLIENTEXCEPTION_H
#define RAMCLOUD_CLIENTEXCEPTION_H

#include "Exception.h"
#include "Status.h"

namespace RAMCloud {

/**
 * The base class for all exceptions that can be generated within
 * clients by the RAMCloud library. Exceptions correspond to Status
 * values. Within the server, any of these exceptions may be thrown
 * at any time, which will abort the request and reflect the exception
 * back to the client.
 */
class ClientException : public std::exception {
  public:
    ClientException(const CodeLocation& where, Status status);
    ClientException(const ClientException& other);
    ClientException& operator=(const ClientException& other);
    virtual ~ClientException() throw();
    static void throwException(const CodeLocation& where, Status status)
        __attribute__((noreturn));
    const char* toString() const;
    const char* toSymbol() const;
    string str() const;
    const char* what() const throw();

    /**
     * Describes a problem that prevented normal completion of a
     * RAMCloud operation.
     */
    Status status;

    CodeLocation where;
  private:
    mutable std::unique_ptr<const char[]> whatCache;
};

/**
 * Superclass covering all exceptions that can be generated as a
 * result of checking RejectRules.
 */
class RejectRulesException : public ClientException {
  public:
    RejectRulesException(const CodeLocation& where, Status status)
        : ClientException(where, status) {}
};

/**
 * Superclass covering all exceptions that correspond to internal
 * errors within the RAMCloud system.
 */
class InternalError : public ClientException {
  public:
    InternalError(const CodeLocation& where, Status status)
        : ClientException(where, status) {}
};

// The following macro is used for convenience in defining a large
// number of different exceptions, one corresponding to each different
// Status value.

#define DEFINE_EXCEPTION(name, status, superClass)          \
class name : public superClass {                            \
  public:                                                   \
    explicit name(const CodeLocation& where)                \
        : superClass(where, status) { }                     \
};

/**
 * This exception is used by the server to ask the client to retry an
 * RPC at a later time; the exception should never be visible to client
 * applications (it is handled in the RPC system).
 */
class RetryException : public ClientException {
  public:
    /**
     * Construct a RetryException.
     * \param where
     *     Identifies the code location at which the exception was thrown.
     * \param minDelayMicros
     *     Lower-bound on how long the client should wait before retrying
     *     the RPC, in microseconds.
     * \param maxDelayMicros
     *     Upper bound on the client delay, in microseconds: the client
     *     should pick a random number between minDelayMicros and
     *     maxDelayMicros and wait that many microseconds before retrying
     *     the RPC.
     * \param message
     *     Human-readable message describing the reason for the exception;
     *     this is likely to get logged on the client side. NULL means there
     *     is no message.
     */
    RetryException(const CodeLocation& where, uint32_t minDelayMicros = 0,
            uint32_t maxDelayMicros = 0, const char* message = NULL)
        : ClientException(where, STATUS_RETRY)
        , minDelayMicros(minDelayMicros)
        , maxDelayMicros(maxDelayMicros)
        , message(message)
    {}

    RetryException(const RetryException& other)
        : ClientException(other)
        , minDelayMicros(other.minDelayMicros)
        , maxDelayMicros(other.maxDelayMicros)
        , message(other.message)
    {}
    RetryException& operator=(const RetryException& other)
    {
        ClientException::operator=(other);
        minDelayMicros = other.minDelayMicros;
        maxDelayMicros = other.maxDelayMicros;
        message= other.message;
        return *this;
    }

    /// Copies of the constructor arguments.
    uint32_t minDelayMicros;
    uint32_t maxDelayMicros;
    const char* message;
};

// Not clear that there should be an exception for successful
// completion, but it is here for completeness.
DEFINE_EXCEPTION(Success,
                 STATUS_OK,
                 ClientException)
DEFINE_EXCEPTION(UnknownTabletException,
                 STATUS_UNKNOWN_TABLET,
                 ClientException)
DEFINE_EXCEPTION(TableDoesntExistException,
                 STATUS_TABLE_DOESNT_EXIST,
                 ClientException)
DEFINE_EXCEPTION(ObjectDoesntExistException,
                 STATUS_OBJECT_DOESNT_EXIST,
                 RejectRulesException)
DEFINE_EXCEPTION(ObjectExistsException,
                 STATUS_OBJECT_EXISTS,
                 RejectRulesException)
DEFINE_EXCEPTION(WrongVersionException,
                 STATUS_WRONG_VERSION,
                 RejectRulesException)
DEFINE_EXCEPTION(NoTableSpaceException,
                 STATUS_NO_TABLE_SPACE,
                 ClientException)
DEFINE_EXCEPTION(MessageTooShortError,
                 STATUS_MESSAGE_TOO_SHORT,
                 InternalError)
DEFINE_EXCEPTION(UnimplementedRequestError,
                 STATUS_UNIMPLEMENTED_REQUEST,
                 InternalError)
DEFINE_EXCEPTION(RequestFormatError,
                 STATUS_REQUEST_FORMAT_ERROR,
                 InternalError)
DEFINE_EXCEPTION(ResponseFormatError,
                 STATUS_RESPONSE_FORMAT_ERROR,
                 InternalError)
DEFINE_EXCEPTION(CouldntConnectException,
                 STATUS_COULDNT_CONNECT,
                 ClientException)
DEFINE_EXCEPTION(BackupBadSegmentIdException,
                 STATUS_BACKUP_BAD_SEGMENT_ID,
                 ClientException)
DEFINE_EXCEPTION(BackupOpenRejectedException,
                 STATUS_BACKUP_OPEN_REJECTED,
                 ClientException)
DEFINE_EXCEPTION(BackupSegmentOverflowException,
                 STATUS_BACKUP_SEGMENT_OVERFLOW,
                 ClientException)
DEFINE_EXCEPTION(BackupMalformedSegmentException,
                 STATUS_BACKUP_MALFORMED_SEGMENT,
                 ClientException)
DEFINE_EXCEPTION(SegmentRecoveryFailedException,
                 STATUS_SEGMENT_RECOVERY_FAILED,
                 ClientException)
DEFINE_EXCEPTION(ServiceNotAvailableException,
                 STATUS_SERVICE_NOT_AVAILABLE,
                 ClientException)
DEFINE_EXCEPTION(TimeoutException,
                 STATUS_TIMEOUT,
                 ClientException)
DEFINE_EXCEPTION(ServerNotUpException,
                 STATUS_SERVER_NOT_UP,
                 ClientException)
DEFINE_EXCEPTION(InvalidObjectException,
                 STATUS_INVALID_OBJECT,
                 ClientException)
DEFINE_EXCEPTION(TabletDoesntExistException,
                 STATUS_TABLET_DOESNT_EXIST,
                 ClientException)
DEFINE_EXCEPTION(PartitionBeforeReadException,
                 STATUS_PARTITION_BEFORE_READ,
                 ClientException)
DEFINE_EXCEPTION(WrongServerException,
                 STATUS_WRONG_SERVER,
                 ClientException)
DEFINE_EXCEPTION(CallerNotInClusterException,
                 STATUS_CALLER_NOT_IN_CLUSTER,
                 ClientException)
DEFINE_EXCEPTION(RequestTooLargeException,
                 STATUS_REQUEST_TOO_LARGE,
                 ClientException)
DEFINE_EXCEPTION(UnknownIndexletException,
                 STATUS_UNKNOWN_INDEXLET,
                 ClientException)
DEFINE_EXCEPTION(UnknownIndexException,
                 STATUS_UNKNOWN_INDEX,
                 ClientException)
DEFINE_EXCEPTION(InvalidParameterException,
                 STATUS_INVALID_PARAMETER,
                 ClientException)
DEFINE_EXCEPTION(StaleRpcException,
                 STATUS_STALE_RPC,
                 ClientException)
DEFINE_EXCEPTION(ExpiredLeaseException,
                 STATUS_EXPIRED_LEASE,
                 ClientException)
DEFINE_EXCEPTION(TxOpAfterCommit,
                 STATUS_TX_OP_AFTER_COMMIT,
                 ClientException)

} // namespace RAMCloud

#endif // RAMCLOUD_CLIENTEXCEPTION_H
