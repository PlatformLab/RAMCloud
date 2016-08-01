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

#include "AbstractServerList.h"
#include "Buffer.h"
#include "ClusterMetrics.h"
#include "CoordinatorClient.h"
#include "Logger.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Construct a ClusterMetrics object, and optionally fill it with
 * performance information.
 */
ClusterMetrics::ClusterMetrics(RamCloud* cluster) : servers()
{
    if (cluster != NULL)
        load(cluster);
}

/**
 * Retrieve performance counters from all of the servers in a cluster.
 * Any existing counters in the object are deleted.
 *
 * \param cluster
 *      Identifies the RAMCloud cluster from which to retrieve metrics.
 */
void
ClusterMetrics::load(RamCloud* cluster)
{
    // Get information about all servers in the cluster.
    ProtoBuf::ServerList serverList;
    CoordinatorClient::getServerList(cluster->clientContext, &serverList);

    // Create one ServerMetrics for each unique service locator, starting
    // with the coordinator.
    servers.clear();
    string* clusterLocator = cluster->getServiceLocator();
    servers[*clusterLocator] = cluster->getMetrics(clusterLocator->c_str());
    for (int i = 0; i < serverList.server_size(); i++) {
        const ProtoBuf::ServerList_Entry& server = serverList.server(i);
        const string& serviceLocator = server.service_locator();
        if (find(serviceLocator) == end()) {
            if (server.status() != uint32_t(ServerStatus::UP)) {
                // Don't fetch metrics from servers that aren't up.
                LOG(DEBUG, "Skipping metrics for %s; it is not up",
                    serviceLocator.c_str());
                continue;
            }
            LOG(DEBUG, "Gathering metrics from %s", serviceLocator.c_str());
            servers[serviceLocator] = cluster->getMetrics(
                    serviceLocator.c_str());
        }
    }
}

/**
 * Given another ClusterMetrics object, compute the difference between
 * this object and the other one.
 *
 * \param earlier
 *      Metrics gathered from the same cluster as this object, but at an
 *      earlier point in time.
 *
 * \return
 *       A ClusterMetrics object computed by pairing the ServerMetrics in
 *       \c this and \c earlier by matching their serviceLocators.  For each
 *       pair, the difference between the two objects is added to the result.
 *       If the data for particular server is only present in one of the
 *       ClusterObjects then it is ignored.
 */
ClusterMetrics
ClusterMetrics::difference(ClusterMetrics& earlier)
{
    ClusterMetrics diff;
    for (iterator it = begin(); it != end(); it++) {
        iterator earlierIt = earlier.find(it->first);
        if (earlierIt != earlier.end()) {
            diff.servers[it->first] = it->second.difference(earlierIt->second);
        }
    }
    return diff;
}

} // namespace RAMCloud
