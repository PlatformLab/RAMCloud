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

#ifndef RAMCLOUD_MARK_H
#define RAMCLOUD_MARK_H

namespace RAMCloud {

/**
 * A Mark is a named point in the server's code.
 *
 * Used to select which points on a codepath are of interest for
 * metrics (see rc_select_perf_counter()).
 *
 * More technically, for some particular trace of the codepath
 * followed on a RPC a Mark is a point on that trace.
 *
 * See Metrics::mark() to see how to place marks on a codepath.  (Or
 * better yet, just grep for "mark").
 */
enum Mark {
    /// Doesn't occur on the codepath; used to disable metrics.
    MARK_NONE = 0,
    /// After the complete request is received by the Server from the
    /// transport layer
    MARK_RPC_PROCESSING_BEGIN,
    /// Before the completed response is sent to the Transport layer
    MARK_RPC_PROCESSING_END,
};

} // namespace RAMCloud

#endif
