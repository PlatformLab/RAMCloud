#ifndef GRANULARCOMPUTING_ALLSHUFFLE_H
#define GRANULARCOMPUTING_ALLSHUFFLE_H

#include <boost/dynamic_bitset.hpp>

#include "Buffer.h"
#include "CommunicationGroup.h"
#include "RpcWrapper.h"
#include "Service.h"
#include "WireFormat.h"
#include "ServerIdRpcWrapper.h"

namespace RAMCloud {
    
class AllShuffle {
  public:

    using RpcType = WireFormat::AllShuffle;

    template <typename Merger>
    explicit AllShuffle(int opId, Context* context, CommunicationGroup* group,
            Merger* merger)
        : group(group)
        , merger(*merger)
        , opId(opId)
        , localData()
        , outstandingRpcs(0)
        , outgoingRpcs(group->size())
        , mutex("receivedFrom")
        , receivedFrom(group->size())
    {
        receivedFrom.set(group->rank);
        for (int i = 0; i < group->size(); i++) {
            if (group->rank != i) {
                outgoingRpcs[i].construct(context, group->getNode(i), opId,
                        group->rank);
            }
        }
    }

    ~AllShuffle() = default;
    void closeSendBuffer(int rank);
    Buffer* getSendBuffer(int rank);
    void handleRpc(const WireFormat::AllShuffle::Request* reqHdr,
            WireFormat::AllShuffle::Response* respHdr, Service::Rpc* rpc);
    bool isReady();
    void wait();

  PRIVATE:
    class AllShuffleRpc : public ServerIdRpcWrapper {
      public:
        explicit AllShuffleRpc(Context* context, ServerId serverId,
                int opId, int senderId)
            : ServerIdRpcWrapper(context, serverId,
                sizeof(WireFormat::AllShuffle::Response))
        {
            WireFormat::AllShuffle::Request* reqHdr(
                    allocHeader<WireFormat::AllShuffle>(downCast<uint32_t>(opId)));
            reqHdr->senderId = downCast<uint32_t>(senderId);
            // TODO: AllShuffleRpc is special; the data to send has not been
            // appended.
//            send();
        }

        /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
        void wait()
        {
            waitAndCheckErrors();
            // Chop off the FlatGather response header.
            response->truncateFront(responseHeaderLength);
        }

        virtual void send() { ServerIdRpcWrapper::send(); }

        DISALLOW_COPY_AND_ASSIGN(AllShuffleRpc);
    };


    /// All nodes that are participating in the shuffle operation.
    CommunicationGroup* group;

    std::function<void(Buffer*)> merger;

    int opId;

    // TODO: should we handle data to local node specially in this class or
    // in the caller of the shuffle op?
    Buffer localData;

    int outstandingRpcs;

    /// RPCs used to transfer data to the other nodes in the group.
    std::vector<Tub<AllShuffleRpc>> outgoingRpcs;

    // TODO: protect both receivedFrom and merger
    SpinLock mutex;

    /// Used to record the nodes from which we have collected the data.
    boost::dynamic_bitset<> receivedFrom;

    DISALLOW_COPY_AND_ASSIGN(AllShuffle)
};

}

#endif