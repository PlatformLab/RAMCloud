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
 * Defines all of the exceptions that are visible to RAMCloud clients.
 * There is one exception class for each Status value, plus additional
 * super-classes corresponding to related groups of exceptions, such as
 * RejectRule failures or internal errors.
 */

#ifndef RAMCLOUD_CLIENTEXCEPTION_H
#define RAMCLOUD_CLIENTEXCEPTION_H

#include "Common.h"
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

// Not clear that there should be an exception for successful
// completion, but it is here for completeness.
DEFINE_EXCEPTION(Success,
                 STATUS_OK,
                 ClientException)
DEFINE_EXCEPTION(UnknownTableException,
                 STATUS_UNKNOWN_TABLE,
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
DEFINE_EXCEPTION(BackupSegmentAlreadyOpenException,
                 STATUS_BACKUP_SEGMENT_ALREADY_OPEN,
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
DEFINE_EXCEPTION(RetryException,
                 STATUS_RETRY,
                 ClientException)
DEFINE_EXCEPTION(ServiceNotAvailableException,
                 STATUS_SERVICE_NOT_AVAILABLE,
                 ClientException)
DEFINE_EXCEPTION(TimeoutException,
                 STATUS_TIMEOUT,
                 ClientException)
DEFINE_EXCEPTION(ServerDoesntExistException,
                 STATUS_SERVER_DOESNT_EXIST,
                 ClientException)
DEFINE_EXCEPTION(InvalidObjectException,
                 STATUS_INVALID_OBJECT,
                 ClientException)
DEFINE_EXCEPTION(TabletDoesntExistException,
                 STATUS_TABLET_DOESNT_EXIST,
                 ClientException)

} // namespace RAMCloud

#endif // RAMCLOUD_CLIENTEXCEPTION_H
