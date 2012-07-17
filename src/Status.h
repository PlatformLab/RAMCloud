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
    /// Default return value when an operation was successful.
    STATUS_OK                           = 0,

    /// Indicates that the server does not know about (and is not responsible
    /// for) a given table, but that it may exist elsewhere in the system.
    /// When it's possible that the table exists on another server, this status
    /// should be returned (in preference to the definitive TABLE_DOESNT_EXIST).
    STATUS_UNKNOWN_TABLE                = 1,

    /// Indicates that a table does not exist anywhere in the system. At present
    /// only the coordinator can say with certainly that a table does not exist.
    STATUS_TABLE_DOESNT_EXIST           = 2,


    /// Indicates that an object does not exist anywhere in the system. Note
    /// that unlike with tables there is no UNKNOWN_OBJECT status. This is just
    /// because servers will reject operations on objects in unknown tables with
    /// a table-related status. If they own a particular tablet, then they can
    /// say with certainty if an object exists there or not.
    STATUS_OBJECT_DOESNT_EXIST          = 3,

    // TODO(anyone) More documentation below, please.

    STATUS_OBJECT_EXISTS                = 4,
    STATUS_WRONG_VERSION                = 5,
    STATUS_NO_TABLE_SPACE               = 6,
    STATUS_MESSAGE_TOO_SHORT            = 7,
    STATUS_UNIMPLEMENTED_REQUEST        = 8,
    STATUS_REQUEST_FORMAT_ERROR         = 9,
    STATUS_RESPONSE_FORMAT_ERROR        = 10,
    STATUS_COULDNT_CONNECT              = 11,
    STATUS_BACKUP_BAD_SEGMENT_ID        = 12,
    /// Returned by backups when they cannot (or do not wish to) allocate
    /// space for a segment replica.
    STATUS_BACKUP_OPEN_REJECTED         = 13,
    STATUS_BACKUP_SEGMENT_OVERFLOW      = 14,
    STATUS_BACKUP_MALFORMED_SEGMENT     = 15,
    STATUS_SEGMENT_RECOVERY_FAILED      = 16,
    STATUS_RETRY                        = 17,
    STATUS_SERVICE_NOT_AVAILABLE        = 18,
    STATUS_TIMEOUT                      = 19,
    STATUS_SERVER_DOESNT_EXIST          = 20,
    STATUS_INTERNAL_ERROR               = 21,

    /// Indicates that the object chosen for an operation does not match the
    /// associated requirements. Therefore the chosen object is invalid.
    STATUS_INVALID_OBJECT               = 22,
    /// Indicates that a tablet does not exist. This status is of relevance
    /// when doing split or merge operations on tablets are executed.
    STATUS_TABLET_DOESNT_EXIST          = 23,
    STATUS_MAX_VALUE                    = 23,
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
