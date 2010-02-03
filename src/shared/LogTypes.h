/* Copyright (c) 2009 Stanford University
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

#ifndef _LOG_TYPES_H_
#define _LOG_TYPES_H_

namespace RAMCloud {

enum log_entry_type_t {
        LOG_ENTRY_TYPE_SEGMENT_HEADER   = 0x72646873,	// "shdr" in little endian
        LOG_ENTRY_TYPE_SEGMENT_CHECKSUM = 0x6b686373,	// "schk" in little endian 
        LOG_ENTRY_TYPE_OBJECT           = 0x216a626f,   // "obj!" in little endian
        LOG_ENTRY_TYPE_OBJECT_TOMBSTONE = 0x626d6f74    // "tomb" in little endian
};

} // namespace

#endif // !_LOG_TYPES_H_
