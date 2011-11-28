/* Copyright (c) 2011 Stanford University
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

#ifndef RAMCLOUD_CLUSTERMETRICS_H
#define RAMCLOUD_CLUSTERMETRICS_H

#include <unordered_map>
#include "Common.h"
#include "RamCloud.h"
#include "ServerMetrics.h"

namespace RAMCloud {

/**
 * An object of this class holds metrics from all of the servers in a
 * RAMCloud cluster, including the coordinator, indexed by the service
 * locator for each server.  It is typically used by benchmarking
 * applications in the following way:
 * - Fetch ClusterMetrics from the cluster before running a benchmark.
 * - Run the benchmark.
 * - Fetch another set of ClusterMetrics and take the difference
 *   between the two sets.
 * - Analyze interesting metrics from the difference.
 *
 * ClusterMetrics is a thin layer on top of unordered_map, so you can
 * use most standard map methods, such as operator[].
 */
class ClusterMetrics {
  public:
    explicit ClusterMetrics(RamCloud* cluster = NULL);
    ~ClusterMetrics() { }
    void load(RamCloud* cluster);
    ClusterMetrics difference(ClusterMetrics& other);

    // The following methods all delegate directly to the corresponding
    // methods in unordered_map.
    ServerMetrics& operator[](const string& serviceLocator)
    {
        return servers[serviceLocator];
    }

    typedef std::unordered_map<std::string, ServerMetrics>::iterator iterator;
    iterator begin()
    {
        return servers.begin();
    }
    iterator end()
    {
        return servers.end();
    }
    iterator find(const string& serviceLocator)
    {
        return servers.find(serviceLocator);
    }

    void clear()
    {
        servers.clear();
    }

    size_t erase(const string& serviceLocator)
    {
        return servers.erase(serviceLocator);
    }

    bool empty()
    {
        return servers.empty();
    }

    size_t size()
    {
        return servers.size();
    }

  PRIVATE:
    /// Maps from service locators to the metrics for that server.
    std::unordered_map<std::string, ServerMetrics> servers;
};

} // end RAMCloud

#endif  // RAMCLOUD_CLUSTERMETRICS_H
