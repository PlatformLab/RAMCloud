/* Copyright (c) 2012 Stanford University
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

#include "Tablet.h"
#include "Logger.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Populate a protocol buffer entry with the details of this tablet.
 * Note, this does not provide the service_locator field (which isn't
 * known by the tablet).
 *
 * \param entry
 *      Entry in the protocol buffer to populate.
 */
void
Tablet::serialize(ProtoBuf::Tablets::Tablet& entry) const
{
    entry.set_table_id(tableId);
    entry.set_start_key_hash(startKeyHash);
    entry.set_end_key_hash(endKeyHash);
    entry.set_server_id(serverId.getId());
    if (status == NORMAL)
        entry.set_state(ProtoBuf::Tablets::Tablet::NORMAL);
    else if (status == RECOVERING)
        entry.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    else
        DIE("Unknown status stored in tablet map");
    entry.set_ctime_log_head_id(ctime.getSegmentId());
    entry.set_ctime_log_head_offset(ctime.getSegmentOffset());
}

} // namespace RAMCloud
