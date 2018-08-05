#include "Gather.h"

namespace RAMCloud {

bool
FlatGather::isReady()
{
    if (group->rank != root) {
        return sendData->isReady();
    } else {
        SpinLock::Guard _(mutex);
        return int(gatheredFrom->count()) == group->size();
    }
}

void
FlatGather::handleRpc(const WireFormat::FlatGather::Request* reqHdr,
        WireFormat::FlatGather::Response* respHdr, Service::Rpc* rpc)
{
    SpinLock::Guard _(mutex);

    assert(group->rank == 0);
    uint32_t senderId = reqHdr->senderId;
    if (gatheredFrom->test(senderId)) {
        respHdr->common.status = STATUS_MILLISORT_REDUNDANT_DATA;
        return;
    }

    // Chop off the FlatGather header and incorporate the data.
    rpc->requestPayload->truncateFront(sizeof(*reqHdr));
    if (merger) {
        merger(rpc->requestPayload);
    }

    gatheredFrom->set(senderId);
}

void
FlatGather::wait()
{
    while (!isReady()) {}
}

}