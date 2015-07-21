/* Copyright (c) 2012-2015 Stanford University
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

#ifndef RAMCLOUD_TABLETSBUILDER_H
#define RAMCLOUD_TABLETSBUILDER_H

#include "Log.h"
#include "Tablets.pb.h"

namespace RAMCloud {

/**
 * Convenient way to build ProtoBuf::Tablets objects.
 */
struct TabletsBuilder {
    /**
     * \param[out] tablets
     *      Tablets will be appended to this list.
     */
    explicit TabletsBuilder(ProtoBuf::Tablets& tablets)
        : tablets(tablets)
    {
    }

    enum State {
        NORMAL = 0,
        RECOVERING = 1,
    };

    typedef ProtoBuf::Tablets::Tablet Tablet;

    /**
     * Invocations of this method can be placed directly next to each other,
     * since it returns a reference to 'this' object. This allows the caller to
     * build up tablets very succinctly, which is useful for testing.
     *
     * These parameters are the same as the ones defined in Tablets.proto.
     *
     * Example:
     * TabletsBuilder{tablets}
     *     (123,  0,  9, TabletsBuilder::RECOVERING, 0)
     *     (123, 20, 29, TabletsBuilder::RECOVERING, 0)
     *     (123, 10, 19, TabletsBuilder::RECOVERING, 1);
     */
    TabletsBuilder&
    operator()(uint32_t tableId,
               uint64_t startKeyHash,
               uint64_t endKeyHash,
               State state = NORMAL,
               uint64_t userData = 0lu,
               ServerId serverId = ServerId(0, 0),
               LogPosition ctime = LogPosition(0, 0))
    {
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(tableId);
        tablet.set_start_key_hash(startKeyHash);
        tablet.set_end_key_hash(endKeyHash);
        tablet.set_state(static_cast<Tablet::State>(state));
        tablet.set_ctime_log_head_id(ctime.getSegmentId());
        tablet.set_ctime_log_head_offset(ctime.getSegmentOffset());
        tablet.set_user_data(userData);
        tablet.set_server_id(serverId.getId());
        return *this;
    }

    ProtoBuf::Tablets& tablets;
};

} // namespace RAMCloud

#endif // RAMCLOUD_TABLETSBUILDER_H

