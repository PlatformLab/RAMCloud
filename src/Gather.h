#ifndef GRANULARCOMPUTING_GATHER_H
#define GRANULARCOMPUTING_GATHER_H

#include <boost/dynamic_bitset.hpp>

#include "Buffer.h"
#include "CommunicationGroup.h"
#include "RpcWrapper.h"
#include "ServerIdRpcWrapper.h"
#include "Service.h"
#include "WireFormat.h"

namespace RAMCloud {

class FlatGather {
  public:

    using RpcType = WireFormat::FlatGather;

    // TODO: this is the data-transfer version of the API. Not sure if we need
    // the more generic reduce-like API right now. Actually, unlike bcast, there
    // is no RPC involved because any processing on local data can be simply
    // performed before calling the ctor

    template <typename T, typename Merger>
    explicit FlatGather(int opId, Context* context, CommunicationGroup* group,
            int root, uint32_t numElements, const T* elements, Merger* merger)
        : group(group)
        , merger(*merger)
        , opId(opId)
        , root(root)
        , gatheredFrom()
        , mutex("gatheredFrom lock")
        , sendData()
    {
        uint32_t numBytes = numElements * sizeof32(T);
        if (group->rank == root) {
            gatheredFrom.construct(group->size());
            gatheredFrom->set(downCast<uint32_t>(root));

            Buffer data;
            data.appendExternal(elements, numBytes);
            this->merger(&data);
        } else {
            sendData.construct(context, group->getNode(root), opId, group->rank,
                    numBytes, elements);
        }
    }

    ~FlatGather() = default;

    bool isReady();

    void handleRpc(const WireFormat::FlatGather::Request* reqHdr,
            WireFormat::FlatGather::Response* respHdr, Service::Rpc* rpc);

    void wait();

  PRIVATE:
    class FlatGatherRpc : public ServerIdRpcWrapper {
      public:
        explicit FlatGatherRpc(Context* context, ServerId serverId,
                int opId, int senderId, uint32_t numBytes,
                const void* data)
            : ServerIdRpcWrapper(context, serverId,
                sizeof(WireFormat::FlatGather::Response))
        {
            WireFormat::FlatGather::Request* reqHdr(
                    allocHeader<WireFormat::FlatGather>(downCast<uint32_t>(opId)));
            reqHdr->senderId = downCast<uint32_t>(senderId);
            request.appendExternal(data, numBytes);
            send();
        }

        /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
        void wait()
        {
            waitAndCheckErrors();
            // Chop off the FlatGather response header.
            response->truncateFront(responseHeaderLength);
        }

        DISALLOW_COPY_AND_ASSIGN(FlatGatherRpc);
    };
    
    
    /// All nodes that are participating in the gather operation.
    CommunicationGroup* group;

    std::function<void(Buffer*)> merger;

    int opId;

    int root;

    /// Used to record the nodes from which we have collected the data. Only
    /// present on the root node.
    Tub<boost::dynamic_bitset<>> gatheredFrom;

    /// Used to make sure that only one thread at a time attempts to access
    /// #gatheredFrom.
    SpinLock mutex;

    /// RPC used to transfer data to the root node.
    Tub<FlatGatherRpc> sendData;

    DISALLOW_COPY_AND_ASSIGN(FlatGather)
};

}

#endif