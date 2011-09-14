/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "Buffer.h"
#include "MetricsHash.h"
#include "MetricList.pb.h"

namespace RAMCloud {

/**
 * Constructor for MetricsHash objects.  This method leaves the object
 * empty.
 */
MetricsHash::MetricsHash() : metrics()
{
}

/**
 * Incorporate a server's metrics data into this object.  Existing
 * entries are not deleted, but may be overridden by new data.
 *
 * \param buffer
 *      Contains metrics data formatted as a binary string using Protocol
 *      Buffers in the form of a MetricList, such as the result of a
 *      GET_METRICS RPC.
 */
void
MetricsHash::load(Buffer& buffer)
{
    ProtoBuf::MetricList list;
    uint32_t bufferLength = buffer.getTotalLength();
    string s(static_cast<const char*>(buffer.getRange(0, bufferLength)),
                bufferLength);
    if (!list.ParseFromString(s)) {
        throw FormatError(HERE);
    }
    for (int i = 0; i < list.metric_size(); i++) {
        const ProtoBuf::MetricList_Entry& metric = list.metric(i);
        metrics[metric.name()] = metric.value();
    }
}

/**
 * Destructor for MetricsHash objects.
 */
MetricsHash::~MetricsHash() {
    // Nothing to do here.
}

/**
 * Given another MetricsHash, for each entry in the other MetricsHash,
 * subtract its value from the corresponding entry in the current object.
 *
 * \param other
 *      Another set of performance data.  This usually holds metrics
 *      gathered at an earlier time, so when this method returns this
 *      object will hold the change in values that occurred over a
 *      time interval.
 */
void
MetricsHash::difference(MetricsHash& other)
{
    for (MetricsHash::iterator it = other.begin(); it != other.end();
            it++) {
        (*this)[it->first] -= it->second;
    }
}

} // namespace RAMCloud
