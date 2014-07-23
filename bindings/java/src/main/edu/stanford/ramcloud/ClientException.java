/* Copyright (c) 2014 Stanford University
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
package edu.stanford.ramcloud;

/**
 * The base class for all exceptions that can be generated within
 * clients by the RAMCloud library. Exceptions correspond to Status
 * values.
 */
public class ClientException extends RuntimeException {
    /**
     * Avoid calling Status.values() repeatedly
     */
    private static Status[] statuses = Status.values();

    /**
     * Throws any exceptions if the returned status from the function was
     * not SUCCESS.
     * @param statusCode The status returned from C++.
     */
    public static void checkStatus(int statusCode) {
        Status status = Status.statuses[statusCode];
        switch (status) {
            case STATUS_OK:
                return;
            case STATUS_UNKNOWN_TABLET:
                throw new UnknownTabletException();
            case STATUS_TABLE_DOESNT_EXIST:
                throw new TableDoesntExistException();
            case STATUS_OBJECT_DOESNT_EXIST:
                throw new ObjectDoesntExistException();
            case STATUS_OBJECT_EXISTS:
                throw new ObjectExistsException();
            case STATUS_WRONG_VERSION:
                throw new WrongVersionException();
            case STATUS_NO_TABLE_SPACE:
                throw new NoTableSpaceException();
            case STATUS_MESSAGE_TOO_SHORT:
                throw new MessageTooShortError();
            case STATUS_UNIMPLEMENTED_REQUEST:
                throw new UnimplementedRequestError();
            case STATUS_REQUEST_FORMAT_ERROR:
                throw new RequestFormatError();
            case STATUS_RESPONSE_FORMAT_ERROR:
                throw new ResponseFormatError();
            case STATUS_COULDNT_CONNECT:
                throw new CouldntConnectException();
            case STATUS_BACKUP_BAD_SEGMENT_ID:
                throw new BackupBadSegmentIdException();
            case STATUS_BACKUP_OPEN_REJECTED:
                throw new BackupOpenRejectedException();
            case STATUS_BACKUP_SEGMENT_OVERFLOW:
                throw new BackupSegmentOverflowException();
            case STATUS_BACKUP_MALFORMED_SEGMENT:
                throw new BackupMalformedSegmentException();
            case STATUS_SEGMENT_RECOVERY_FAILED:
                throw new SegmentRecoveryFailedException();
            case STATUS_RETRY:
                throw new RetryException();
            case STATUS_SERVICE_NOT_AVAILABLE:
                throw new ServiceNotAvailableException();
            case STATUS_TIMEOUT:
                throw new TimeoutException();
            case STATUS_SERVER_NOT_UP:
                throw new ServerNotUpException();
            case STATUS_INTERNAL_ERROR:
                throw new InternalErrorException();
            case STATUS_INVALID_OBJECT:
                throw new InvalidObjectException();
            case STATUS_TABLET_DOESNT_EXIST:
                throw new TabletDoesntExistException();
            case STATUS_PARTITION_BEFORE_READ:
                throw new PartitionBeforeReadException();
            case STATUS_WRONG_SERVER:
                throw new WrongServerException();
            case STATUS_CALLER_NOT_IN_CLUSTER:
                throw new CallerNotInClusterException();
            case STATUS_REQUEST_TOO_LARGE:
                throw new RequestTooLargeException();
            case STATUS_UNKNOWN_INDEXLET:
                throw new UnknownIndexletException();
            case STATUS_UNKNOWN_INDEX:
                throw new UnknownIndexException();
            default:
                throw new UnrecognizedErrorException();
        }
    }

    /**
     * Superclass covering all exceptions that can be generated as a
     * result of checking RejectRules.
     */
    public static class RejectRulesException extends ClientException {}

    /**
     * Superclass covering all exceptions that correspond to internal
     * errors within the RAMCloud system.
     */
    public static class InternalError extends ClientException {}

    public static class UnknownTabletException extends ClientException {}
    public static class TableDoesntExistException extends ClientException {}
    public static class ObjectDoesntExistException extends RejectRulesException {}
    public static class ObjectExistsException extends RejectRulesException {}
    public static class WrongVersionException extends RejectRulesException {}
    public static class NoTableSpaceException extends ClientException {}
    public static class MessageTooShortError extends InternalError {}
    public static class UnimplementedRequestError extends InternalError {}
    public static class RequestFormatError extends InternalError {}
    public static class ResponseFormatError extends InternalError {}
    public static class CouldntConnectException extends ClientException {}
    public static class BackupBadSegmentIdException extends ClientException {}
    public static class BackupOpenRejectedException extends ClientException {}
    public static class BackupSegmentOverflowException extends ClientException {}
    public static class BackupMalformedSegmentException extends ClientException {}
    public static class SegmentRecoveryFailedException extends ClientException {}
    public static class RetryException extends ClientException {}
    public static class ServiceNotAvailableException extends ClientException {}
    public static class TimeoutException extends ClientException {}
    public static class ServerNotUpException extends ClientException {}
    public static class InternalErrorException extends ClientException {}
    public static class InvalidObjectException extends ClientException {}
    public static class TabletDoesntExistException extends ClientException {}
    public static class PartitionBeforeReadException extends ClientException {}
    public static class WrongServerException extends ClientException {}
    public static class CallerNotInClusterException extends ClientException {}
    public static class RequestTooLargeException extends ClientException {}
    public static class UnknownIndexletException extends ClientException {}
    public static class UnknownIndexException extends ClientException {}

    /**
     * Exception thrown when Java doesn't recognize the status code
     * returned from C++. This will probably happen when someone
     * adds an exception in C++ and does not add it in Java.
     */
    public static class UnrecognizedErrorException extends RuntimeException {}
}
