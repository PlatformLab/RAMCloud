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

#ifndef RAMCLOUD_REJECTRULES_H
#define RAMCLOUD_REJECTRULES_H

#include <inttypes.h>

#ifdef __cplusplus
namespace RAMCloud {
#endif

/**
 * Used in conditional operations to specify conditions under
 * which an operation should be aborted with an error.
 *
 * RejectRules are typically used to ensure consistency of updates;
 * for example, we might want to update a value but only if it hasn't
 * changed since the last time we read it.  If a RejectRules object
 * is passed to an operation, the operation will be aborted if any
 * of the following conditions are satisfied:
 * - doesntExist is nonzero and the object does not exist
 * - exists is nonzero and the object does exist
 * - versionLeGiven is nonzero and the object exists with a version
 *   less than or equal to givenVersion.
 * - versionNeGiven is nonzero and the object exists with a version
 *   different from givenVersion.
 */
struct RejectRules {
    uint64_t  givenVersion;
    uint8_t   doesntExist;
    uint8_t   exists;
    uint8_t   versionLeGiven;
    uint8_t   versionNeGiven;
} __attribute__((packed));

#ifdef __cplusplus
} // namespace RAMCloud
#endif

#endif // RAMCLOUD_REJECTRULES_H

