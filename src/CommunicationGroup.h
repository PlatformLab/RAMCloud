#ifndef RAMCLOUD_COMMUNICATIONGROUP_H
#define RAMCLOUD_COMMUNICATIONGROUP_H

#include "ServerId.h"

namespace RAMCloud {

/**
 * A communication group consists of a fixed number of service nodes while each
 * node is assigned a unique integer rank in the range of [0, groupSize).
 *
 * TODO: say more. e.g. all nodes in the group must agree on their ranks; how
 * we derive sub-groups starting from the world group.
 *
 * Finally, a communication group cannot be changed once it's created.
 */
class CommunicationGroup {
  public:
    /// Constructor
    // TODO: doc
    /**
     *
     * \tparam Nodes
     * \param id
     * \param rank
     * \param nodes
     */
    template <typename Nodes>
    explicit CommunicationGroup(int id, int rank, Nodes nodes)
        : id(id)
        , nodes()
        , rank(rank)
    {
        for (typename Nodes::iterator it = std::begin(nodes);
                it != std::end(nodes); it++) {
            this->nodes.push_back(*it);
        }
    }

    /**
     * Return the k-th (k >= 0) node of this communication group.
     *
     * Note: this method assumes a ring structure of the nodes and automatically
     * wraps around if k is too large.
     */
    ServerId
    getNode(int k)
    {
        assert(k >= 0);
        return nodes[k % nodes.size()];
    }

    /**
     * Compute the relative rank of the local node w.r.t. a reference node.
     *
     * That is, imagine the nodes in this communication group forms a ring and
     * we assign ranks to nodes in clockwise order starting from the reference
     * node.
     *
     * \param referenceNode
     *      Rank of the reference node.
     * \return
     *      Relative rank of the local node.
     */
    int
    relativeRank(int referenceNode)
    {
        return downCast<int>(
                (rank - referenceNode + nodes.size()) % nodes.size());
    }

    /**
     * Return the number of nodes in this communication group.
     */
    int
    size()
    {
        return downCast<int>(nodes.size());
    }

    // TODO: the following doc is quite vague and imprecise.

    /// Used by service to uniquely identify a communication group.
    /// Identical on all #nodes.
    int id;

    /// Holds nodes within this communication group. Must be identical on all
    /// #nodes.
    vector<ServerId> nodes;

    /// Rank of the local node in #nodes (i.e. nodes[rank] is the local node).
    int rank;

    DISALLOW_COPY_AND_ASSIGN(CommunicationGroup)
};

}

#endif //RAMCLOUD_COMMUNICATIONGROUP_H
