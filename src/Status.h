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
 * This file defines the status values (error codes) returned to
 * clients by RAMCloud operations.
 */

#ifndef RAMCLOUD_STATUS_H
#define RAMCLOUD_STATUS_H

namespace RAMCloud {

/**
 * This enum provides symbolic names for the status values returned
 * to applications by RAMCloud operations.
 *
 * 0 means success; anything else means that an error occurred.
 * Not all status values can be returned by all operations.
 */
enum Status {
    STATUS_OK                           = 0,
    STATUS_TABLE_DOESNT_EXIST           = 1,
    STATUS_OBJECT_DOESNT_EXIST          = 2,
    STATUS_OBJECT_EXISTS                = 3,
    STATUS_WRONG_VERSION                = 4,
    STATUS_NO_TABLE_SPACE               = 5,
    STATUS_MESSAGE_TOO_SHORT            = 6,
    STATUS_UNIMPLEMENTED_REQUEST        = 7,
    STATUS_REQUEST_FORMAT_ERROR         = 8,
    STATUS_RESPONSE_FORMAT_ERROR        = 9,
    STATUS_COULDNT_CONNECT              = 10,
    STATUS_BACKUP_BAD_SEGMENT_ID        = 11,
    STATUS_BACKUP_SEGMENT_ALREADY_OPEN  = 12,
    STATUS_BACKUP_SEGMENT_OVERFLOW      = 13,
    STATUS_BACKUP_MALFORMED_SEGMENT     = 14,
    STATUS_SEGMENT_RECOVERY_FAILED      = 15,
    STATUS_RETRY                        = 16,
    STATUS_SERVICE_NOT_AVAILABLE        = 17,
    STATUS_TIMEOUT                      = 18,
    STATUS_SERVER_DOESNT_EXIST          = 19,
    STATUS_INTERNAL_ERROR               = 20,
    STATUS_MAX_VALUE                    = 20
    // Note: if you add a new status value you must make the following
    // additional updates:
    // * Modify STATUS_MAX_VALUE to have a value equal to the largest
    //   defined status value, and make sure its definition is the last one
    //   in the list.  STATUS_MAX_VALUE is used primarily for testing.
    // * Add new entries in the tables "messages" and "symbols" in Status.cc.
    // * Add a new exception class to ClientException.h
    // * Add a new "case" to ClientException::throwException to map from
    //   the status value to a status-specific ClientException subclass.
};

extern const char* statusToString(Status status);
extern const char* statusToSymbol(Status status);

} // namespace RAMCloud

#endif // RAMCLOUD_STATUS_H
