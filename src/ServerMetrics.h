/* Copyright (c) 2011-2015 Stanford University
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

#ifndef RAMCLOUD_SERVERMETRICS_H
#define RAMCLOUD_SERVERMETRICS_H

#include <unordered_map>
#include <vector>

#include "Exception.h"

namespace RAMCloud {

/**
 * This class provides a hash-table-like mechanism for collecting
 * and querying performance metrics; this is the primary interface used
 * by applications and benchmarks to access performance information.
 * Each metric has a hierarchical string name such as "master.replicas"
 * and a uint64_t value.  The available names are defined in rawmetrics.py.
 * ServerMetrics is a thin layer on top of unordered_map, so you can use
 * standard map mechanisms like [].
 */
class ServerMetrics {
  public:
    ServerMetrics();
    ~ServerMetrics();
    void load(Buffer& buffer);
    void load(const string& s);
    ServerMetrics difference(ServerMetrics& other);

    // The following methods all delegate directly to the corresponding
    // methods in unordered_map.
    uint64_t& operator[](string name)
    {
        return metrics[name];
    }

    typedef std::unordered_map<std::string, uint64_t>::iterator iterator;
    iterator begin()
    {
        return metrics.begin();
    }
    iterator end()
    {
        return metrics.end();
    }
    iterator find(const string& name)
    {
        return metrics.find(name);
    }

    void clear()
    {
        metrics.clear();
    }

    size_t erase(const string& name)
    {
        return metrics.erase(name);
    }

    bool empty()
    {
        return metrics.empty();
    }

    size_t size()
    {
        return metrics.size();
    }

    /**
     * An exception thrown when the data passed to load cannot be parsed.
     */
    class FormatError : public Exception {
      public:
        explicit FormatError(const CodeLocation& where) : Exception(where) {
            message = "format error in Protocol Buffer data";
        }
        ~FormatError() throw() {}
    };

  PRIVATE:
    /// Holds all of the metrics.
    std::unordered_map<std::string, uint64_t> metrics;
};

} // end RAMCloud

#endif  // RAMCLOUD_SERVERMETRICS_H
