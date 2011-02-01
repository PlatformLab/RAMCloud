/* Copyright (c) 2009, 2010 Stanford University
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

#ifndef RAMCLOUD_LOGTYPES_H
#define RAMCLOUD_LOGTYPES_H

#include "Common.h"

namespace RAMCloud {

enum LogEntryType {
    LOG_ENTRY_TYPE_UNINIT    = 0x0,        // in case of unfinished segment
    LOG_ENTRY_TYPE_INVALID   = 0x21444142, // "BAD!" in little endian
    LOG_ENTRY_TYPE_SEGHEADER = 0x72646873, // "shdr" in little endian
    LOG_ENTRY_TYPE_SEGFOOTER = 0x72746673, // "sftr" in little endian
    LOG_ENTRY_TYPE_OBJ       = 0x216a626f, // "obj!" in little endian
    LOG_ENTRY_TYPE_OBJTOMB   = 0x626d6f74, // "tomb" in little endian
    LOG_ENTRY_TYPE_LOGDIGEST = 0x74736764  // "dgst" in little endian
};

} // namespace

#endif // !RAMCLOUD_LOGTYPES_H
