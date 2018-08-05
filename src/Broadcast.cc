#include "Broadcast.h"
#include "MilliSortService.h"

namespace RAMCloud {

/// This table records the structure of a 100-node k-nomial-tree with node 0
/// being the root. To be more precise, the i-th (i >= 0) element of the first-
/// level vector is also a vector containing the children of node i. Also, to
/// make the table more compact, leaf nodes are not present in the first-level
/// vector.
std::vector<std::vector<int>> K_NOMIAL_TREE_CHILDREN = {
#include "k_nomial_tree.txt"
};

/**
 * Prepare to invoke a broadcast operation by constructing a tree broadcast
 * task and schedules it on the rule engine. The caller must then call zero or
 * more times #appendCollectiveOp followed by a #send to actually initiate the
 * broadcast.
 *
 * \param context
 *      Service context.
 * \param group
 *      Communication group that specifies the recipients of the broadcast.
 * \param merger
 *      Used to combine responses from children nodes.
 */
TreeBcast::TreeBcast(Context* context, CommunicationGroup* group)
    : context(context)
    , group(group)
    , localResult()
    , root(group->rank)
    , outgoingResponse()
    , responseStorage()
    , outstandingRpcs()
    , payloadResponseHeaderLength(0)
    , payloadRequest()
    , payloadRpc()
    , serviceRpc()
{
    outgoingResponse = responseStorage.construct();
}

/**
 * Constructs a tree broadcast task in response to an incoming tree broadcast
 * request.
 *
 * The visibility of this constructor is set to private because it should only
 * be invoked by the #handleRpc method.
 *
 * \param context
 *      Service context.
 * \param group
 *      Communication group that specifies the recipients of the broadcast.
 * \param header
 *      TreeBcast request header as defined in WireFormat.
 * \param rpc
 *      Incoming broadcast RPC.
 */
TreeBcast::TreeBcast(Context* context,
        const WireFormat::TreeBcast::Request* reqHdr, Service::Rpc* rpc)
    : context(context)
    , group(context->getMilliSortService()->getCommunicationGroup(reqHdr->groupId))
    , localResult()
    , root(reqHdr->root)
    , outgoingResponse(rpc->replyPayload)
    , responseStorage()
    , outstandingRpcs()
    , payloadResponseHeaderLength(reqHdr->payloadResponseHeaderLength)
    , payloadRequest()
    , payloadRpc()
    , serviceRpc(rpc)
{
    // Copy out the payload request (from network packet buffers) to make it
    // contiguous. TreeBcast is not meant for large messages anyway.
    payloadRequest.append(rpc->requestPayload, sizeof(*reqHdr), ~0u);

    // Kick start the broadcast.
    start();
}

/**
 * Assuming that #isCompleted has returned true and this method is invoked
 * on the root node of the broadcast operation, return the buffer storing the
 * final result produced from merging the individual responses collected from
 * each node in the communication group. Otherwise, NULL.
 *
 * The interpretation of the content inside the buffer is up to the caller
 * depending on the response format of the RPC being broadcasted and the merge
 * operation being used.
 */
Buffer*
TreeBcast::getResult()
{
    return (serviceRpc == NULL) ? outgoingResponse : NULL;
}

/**
 * Handler for tree broadcast requests. Invoked by the RPC dispatcher when
 * it sees the TREE_BCAST opcode.
 *
 * \pre
 *      The TreeBcast response header has been allocated and initialized.
 * \param context
 *      Service context.
 * \param reqHdr
 *      Header from the incoming RPC request; contains parameters for this
 *      operation.
 * \param rpc
 *      Service RPC object associated with the broadcast RPC.
 */
void
TreeBcast::handleRpc(Context* context,
        const WireFormat::TreeBcast::Request* reqHdr, Service::Rpc* rpc)
{
    // FIXME: temporary hack to set up at least the world comm. group for
    // non-root nodes.
    context->getMilliSortService()->initWorld();

    TreeBcast treeBcast(context, reqHdr, rpc);
    treeBcast.wait();
}

/**
 * Helper method to kick start the broadcast operation. It should be invoked
 * after the request message becomes complete. It will the outgoing RPCs,
 * handles the inner RPC locally, and checks to see if the broadcast can be
 * completed when this method returns.
 */
void
TreeBcast::start()
{
    // K_NOMIAL_TREE_CHILDREN assumes node 0 is the root. Therefore, to find
    // the correct children of this node when node 0 is not the root, we need
    // to use the relative rank of this node to look up the children table.
    int myRank = group->relativeRank(root);
    if (myRank < int(K_NOMIAL_TREE_CHILDREN.size())) {
        for (int child : K_NOMIAL_TREE_CHILDREN[myRank]) {
            if (child >= group->size()) {
                break;
            }

            outstandingRpcs.emplace_back(context, group->getNode(root + child),
                    group->id, root, payloadResponseHeaderLength,
                    &payloadRequest);
        }
    }

    // FIXME: can we avoid using RPC to deliver payload request locally? Could
    // save some time on the leaf nodes.
    // Deliver the payload request locally.
    payloadRpc.construct(payloadResponseHeaderLength);
    payloadRpc->request.appendExternal(&payloadRequest);
    ServerId id = group->getNode(group->rank);
    payloadRpc->session = context->serverList->getSession(id);
    payloadRpc->send();
}

bool
TreeBcast::isReady()
{
    for (OutstandingRpcs::iterator it = outstandingRpcs.begin();
            it != outstandingRpcs.end();) {
        TreeBcastRpc* rpc = &(*it);
        if (rpc->isReady()) {
            rpc->wait();
            outgoingResponse->append(rpc->response);
            it = outstandingRpcs.erase(it);
        } else {
            it++;
        }
    }

    if (payloadRpc) {
        if (payloadRpc->isReady()) {
            payloadRpc->simpleWait(context);
            outgoingResponse->append(payloadRpc->response);
            payloadRpc.destroy();
        }
    }

    return outstandingRpcs.empty() && !payloadRpc;
}

void
TreeBcast::wait()
{
    while (!isReady()) {};
}

}
