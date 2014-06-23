/* Copyright (c) 2011-2014 Stanford University
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
#include "MetricList.pb.h"
#include "ServerMetrics.h"

namespace RAMCloud {

/**
 * Constructor for ServerMetrics objects.  This method leaves the object
 * empty.
 */
ServerMetrics::ServerMetrics() : metrics()
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
ServerMetrics::load(Buffer& buffer)
{
    uint32_t bufferLength = buffer.size();
    string s(static_cast<const char*>(buffer.getRange(0, bufferLength)),
                bufferLength);
    load(s);
}

/**
 * Incorporate a server's metrics data into this object.  Existing
 * entries are not deleted, but may be overridden by new data.
 *
 * \param s
 *      Contains metrics data formatted as a binary string using Protocol
 *      Buffers in the form of a MetricList, such as the result of a
 *      GET_METRICS RPC.
 */
void
ServerMetrics::load(const string& s)
{
    ProtoBuf::MetricList list;
    if (!list.ParseFromString(s)) {
        throw FormatError(HERE);
    }
    for (int i = 0; i < list.metric_size(); i++) {
        const ProtoBuf::MetricList_Entry& metric = list.metric(i);
        metrics[metric.name()] = metric.value();
    }
}

/**
 * Destructor for ServerMetrics objects.
 */
ServerMetrics::~ServerMetrics() {
    // Nothing to do here.
}

/**
 * Given another ServerMetrics, compute a new ServerMetrics object by
 * subtracting each value in \c other from the corresponding value in the
 * current object.
 *
 * \param other
 *      Another set of performance data.  This usually holds metrics
 *      gathered at an earlier time.
 *
 * \return
 *      A new ServerMetrics object containing one element for each element
 *      in the current object, whose value consists of the difference between
 *      the value in \c this and the corresponding value in \c other.
 */
ServerMetrics
ServerMetrics::difference(ServerMetrics& other)
{
    ServerMetrics diff;
    for (ServerMetrics::iterator it = begin(); it != end(); it++) {
        uint64_t otherValue = 0;
        ServerMetrics::iterator it2 = other.metrics.find(it->first);
        if (it2 != other.end())
            otherValue = it2->second;
        // Some of the "metrics" just provide identifying information;
        // just copy these values rather than differencing them (which
        // would zero them out).
        if ((it->first.compare("clockFrequency") == 0) ||
                (it->first.compare("pid") == 0) ||
                (it->first.compare("serverId") == 0) ||
                (it->first.compare("segmentSize") == 0)) {
            diff[it->first] = it->second;
        } else {
            diff[it->first] = it->second - otherValue;
        }
    }
    return diff;
}

} // namespace RAMCloud
