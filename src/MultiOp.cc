/* Copyright (c) 2013-2014 Stanford University
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

#include "MultiOp.h"
#include "ShortMacros.h"
#include "TimeTrace.h"

namespace RAMCloud {

/**
 * This class implements the client side framework for MasterService multi-ops.
 * It contains the logic to choose which MultiOpObject to place into RPCs,
 * handle RPC completions and handle errors/retries. For information on
 * how to extend and use this framework, see MultiOp.h
 *
 * Externally, this class contains 3 public functions: cancel(), isReady(),
 * and wait(), similar to most RpcWrappers. isReady() is used to advance
 * the multiOp; it checks outgoing RPCs and sends out new ones when possible.
 * wait() internally calls isReady() but blocks until either the multiOp
 * has finished or is canceled. cancel() aborts the MultiOperation.
 * NOTE: This does not implement transactions semantics, so cancel()ing an
 * operation midway may result in half the MultiOps having executed and the
 * other half unexecuted.
 *
 *
 *
 * Internally, the MultiOpObjects pointers are managed on a divided work queue.
 * The division point is marked by startIndex. Everything < startIndex is
 * considered junk. Everything after startIndex are all MultiOpObjects are
 * waiting to be schedueld into an RPC. These MultiOpObjects can either be new
 * or rescheduled objects.
 *
 *  ***************************************************************
 *  *    JUNK     *        UNSCHEDULED MULTIOPOBJECTS             *
 *  *    JUNK     *                                               *
 *  ***************************************************************
 *                /\ startIndex
 *
 * Initially startIndex starts at 0. As MultiOpObjects get scheduled into RPCs,
 * the startIndex moves forward. When RPCs return and certain MultiOpObjects
 * need to be retried, they will be put at startIndex and startIndex will be
 * decremented.
 *
 * Each time isReady() is invoked, startRpcs() will also be invoked. At
 * each invocation to startRpcs(), at least a subset of the unscheduled
 * MultiOpObjects would be examined and scheduled into RPCs if possible.
 * There are certain pecularities and heuristics implemented in startRpcs()
 * that determine when RPCs are sent and when scheduling stops. The details
 * of which are detailed within the function itself.
 *
 * isReady() will also invoke finishRpc() on any RPCs that have completed.
 * finishRpc() will look through all the responses of the RPC and reschedule
 * them into the MultiOpObject work queue.
 *
 * The work queue is initially populated by the constructor.
 */

/**
 * Constructor for MultiOp Framework: pushes the MultiOp requests onto
 * a work queue but does not start any RPCs.
 *
 * This method is intended to be invoked ONLY by the subclass. If the
 * subclass's specifications require rpcs to be started, then startRpcs()
 * must be invoked by the subclass's constructor. It cannot be done here
 * since startRpcs() contain callbacks to the subclass (which would not
 * have been constructed yet).
 *
 * \param ramcloud
 *      The RAMCloud object that governs this operation.
 * \param type
 *      The MultiOp kind of messages this base call shall manage.
 * \param requests
 *      Each element in this array describes one object to read, and where
 *      to place its value.
 * \param numRequests
 *      Number of elements in \c requests.
 */
MultiOp::MultiOp(RamCloud* ramcloud, WireFormat::MultiOp::OpType type,
                MultiOpObject * const requests[], uint32_t numRequests)
    : ramcloud(ramcloud)
    , opType(type)
    , requests(requests)
    , numRequests(numRequests)
    , rpcs()
    , canceled(false)
    , workQueue()
    , startIndex(0)
    , test_ignoreBufferOverflow(false)
{
    workQueue.reserve(numRequests);

    for (uint32_t i = 0; i < numRequests; i++) {
        workQueue.push_back(requests[i]);
    }
}

/**
 * Abort this request if it hasn't already completed.  Once this
 * method returns wait will throw RpcCanceledException if it is invoked.
 */

void
MultiOp::cancel()
{
    for (uint32_t i = 0; i < MAX_RPCS; i++) {
        rpcs[i].destroy();
    }
    canceled = true;
}

/**
 * Check to see whether the multi-Op operation is complete.  If not,
 * start more RPCs if needed.
 *
 * \return
 *      True means that the operation has finished or been canceled; #wait
 *      will not block.  False means that one or more RPCs are still underway.
 */
bool
MultiOp::isReady() {
    // See if any outstanding RPCs have completed (and if so, process their
    // results).
    uint32_t finished = 0;
    uint32_t slotsAvailable = 0;
    for (uint32_t i = 0; i < MAX_RPCS; i++) {
        Tub<PartRpc>& rpc = rpcs[i];
        if (!rpc) {
            slotsAvailable++;
            continue;
        }
        if (rpc->isReady()) {
            finished++;
            finishRpc(rpc.get());
            rpc.destroy();
            slotsAvailable++;

        }
    }

    if ((finished == 0) && (slotsAvailable < MAX_RPCS)) {
        // Nothing more we can do now except wait for RPCs to
        // finish.
        return false;
    }

    // See if there's more work we can start.
    return startRpcs();
}

/**
 * Scan the list of objects and start RPCs if possible. When this method
 * is called, it's possible that some RPCS are already underway (left over
 * from a previous call).
 *
 * \return
 *      True means there is no more work to do: the MultiOp
 *      is done.  False means there are RPCs outstanding.
 */
bool
MultiOp::startRpcs()
{
    uint32_t scheduled = 0, notScheduled = 0, activeRpcCnt = 0;

    for (uint32_t i = 0; i < MAX_RPCS; i++) {
        if (rpcs[i])
            activeRpcCnt++;
    }

//     Each iteration through the following loop examines one of the
//     MultiOp objects (requests) we are supposed to execute and adds
//     it to an outgoing RPC if possible.  We won't necessarily be
//     able to execute all of the operations in one shot, due to
//     limitations on the number of objects we can stuff in one RPC
//     and the number of outstanding RPCs we can have.
//
//     The main loop over all requests below terminates when either:
//     a) There's no more unscheduled requests (i.e. requests that
//        are not finished and are not in an active RPC)
//
//     b) All rpcs are used (activeRpcCnt = MAX_RPCS)
//        -- No point in looking for work we can't do.
//
//     c) The number of scheduled requests (scheduled) equals the number
//        of requests that could not be scheduled.
//        -- Stats show that a 50% miss results in a 10% performance
//           loss and this grows n**2 with higher misses.
//
    for (uint32_t i = startIndex; i < workQueue.size(); i++) {
        MultiOpObject* request = workQueue[i];
        Transport::SessionRef session;

        try {
           session = ramcloud->objectFinder.lookup(request->tableId,
                    request->key, request->keyLength);
        }
        catch (TableDoesntExistException &e) {
            request->status = STATUS_TABLE_DOESNT_EXIST;
            removeRequestAt(i);
            continue;
        }

        // Scan the list of PartRpcs to see if we can send a request for
        // this object.
        for (uint32_t j = 0; j < MAX_RPCS; j++) {
            Tub<PartRpc>& rpc = rpcs[j];
            if (!rpc) {
                // An unused RPC: start a new one.  If we get to an empty
                // slot then there cannot be any more uninitiated RPCs after
                // this slot.
                rpc.construct(ramcloud, session, opType);
            }

            if (rpc->session == session) {
                if ((rpc->reqHdr->count < PartRpc::MAX_OBJECTS_PER_RPC)
                        && (rpc->state == RpcWrapper::RpcState::NOT_STARTED)) {
                    uint32_t lengthBefore = rpc->request.getTotalLength();
                    appendRequest(request, &(rpc->request));
                    uint32_t lengthAfter = rpc->request.getTotalLength();

                    // Check that request isn't too big in general
                    if (lengthAfter - lengthBefore > maxRequestSize) {
                        rpc->request.truncate(lengthBefore);
                        request->status = STATUS_REQUEST_TOO_LARGE;
                        removeRequestAt(i);
                        break;
                    }

                    // Check for full rpc, remove + retry
                    if (lengthAfter > maxRequestSize) {
                        rpc->request.truncate(lengthBefore);
                        activeRpcCnt++;
                        rpc->send();
                        continue;
                    }

                    rpc->requests[rpc->reqHdr->count] = request;
                    rpc->reqHdr->count++;
                    request->status = UNDERWAY;
                    removeRequestAt(i);

                    if (rpc->reqHdr->count == PartRpc::MAX_OBJECTS_PER_RPC) {
                        activeRpcCnt++;
                        rpc->send();
                    }

                    break;
                }
            }
        }

        // Keep count of # of requests scheduled in this round
        if (request->status == UNDERWAY)
            scheduled++;
        else
            notScheduled++;

        // Check Termination conditions (50% scheduled and all RPCs used)
        if (scheduled == notScheduled || activeRpcCnt == MAX_RPCS)
            break;
    }

    // Now launch all of the new RPCs we have generated that are not yet full
    bool done = true;
    for (uint32_t j = 0; j < MAX_RPCS; j++) {
        Tub<PartRpc>& rpc = rpcs[j];
        if (!rpc) {
            continue;
        }
        done = false;
        if (rpc->state == RpcWrapper::RpcState::NOT_STARTED) {
            rpc->send();
        }
    }
    return done;
}

/**
 * This method is invoked when an RPC finishes; it copies result
 * information back to the request objects, and handles certain
 * error conditions.
 *
 * \param rpc
 *      PartRpc that's finished
 */
void
MultiOp::finishRpc(MultiOp::PartRpc* rpc) {
    assert(!rpc->inProgress());
    bool messageLogged = false;

    uint32_t i;
    if (!rpc->isFinished()) {
        // Transport error or canceled; just reset state so that all of
        // the objects will be retried.
        for (i = 0; i < rpc->reqHdr->count; i++) {
            retryRequest(rpc->requests[i]);
        }
        return;
    }

    // The following variable acts as a cursor in the response as
    // we scan through the results for each request.
    uint32_t respOffset = sizeof32(WireFormat::MultiOp::Response);
    uint32_t respSize = rpc->response->getTotalLength();

    // Each iteration extracts one object from the response.  Be careful
    // to handle situations where the response is too short.  This can
    // happen legitimately if the server ran out of room in the response
    // buffer.
    for (i = 0; i < rpc->reqHdr->count; i++) {
        MultiOpObject* request = rpc->requests[i];

        if (!test_ignoreBufferOverflow && respOffset >= respSize)
            break;

        /**
         * At this point four things can happen after the call
         * to the subclass's readResponseFromRPC():
         *
         * a) A Retry is required for the request.
         * b) TabletMap needs to be refreshed.
         * c) Request is considered finished.
         * d) Missing data is detected.
         *
         * Options a) and b) shall be signaled by changing the
         * request's Status to STATUS_RETRY, or
         * STATUS_UNKNOWN_TABLET respectively. Option a) shall
         * be signaled with the use of any other status and
         * Option d) shall be signaled by a false boolean
         * return from the call.
         *
         * Option d) can occur when there are response is cut
         * short by the master and buffer contains partial data.
         */
        bool missingDataDetected =
               readResponse(request, rpc->response, &respOffset);

        if (missingDataDetected)
            break;

        if (request->status == STATUS_UNKNOWN_TABLET) {
            // The object isn't where we thought it should be. Refresh our
            // configuration cache and arrange for this object to be
            // fetched again.
            if (!messageLogged) {
                // (Only log one message per call to this method).
                LOG(NOTICE, "Server %s doesn't store <%lu, %*s>; "
                        "refreshing object map",
                        rpc->session->getServiceLocator().c_str(),
                        request->tableId, (request->keyLength > 100) ? 100 :
                                request->keyLength,
                        reinterpret_cast<const char*>(request->key));
                messageLogged = true;
            }
            ramcloud->objectFinder.flush(request->tableId);
            request->status = STATUS_RETRY;
        }

        // We check status instead of directly retryRequest in other places
        // to ensure that requests don't accidently get retried twice.
        if (request->status == STATUS_RETRY) {
            retryRequest(request);
        }
    }

    // When we get here, it's possible that we aborted part way through
    // because we hit the end of the buffer. For all objects we haven't
    // currently processed, reset their statuses to indicate that these
    // objects need to be fetched again.
    for ( ; i < rpc->reqHdr->count; i++) {
        retryRequest(rpc->requests[i]);
    }
}

/**
 * Wait for the MultiOp operation to complete.
 */
void
MultiOp::wait()
{
    // When invoked in RAMCloud servers there is a separate dispatch thread,
    // so we just busy-wait here. When invoked on RAMCloud clients we're in
    // the dispatch thread so we have to invoke the dispatcher while waiting.
    bool isDispatchThread =
            ramcloud->clientContext->dispatch->isDispatchThread();

    while (true) {
        if (canceled)
            throw RpcCanceledException(HERE);
        if (isReady())
            return;
        if (isDispatchThread)
            ramcloud->clientContext->dispatch->poll();
    }
}

/**
 * Deletes an entry off the request queue.
 *
 * It is the responsibility of the caller to ensure that
 * the index is a valid index in the request queue.
 *
 * \param index
 *      Index of the request in the queue
 */
inline void
MultiOp::removeRequestAt(uint32_t index) {
    assert(index >= startIndex && index < numRequests);

    if (index > startIndex)
        workQueue[index] = workQueue[startIndex];

    startIndex++;
}

/**
 * Adds a request back to the request queue.
 *
 * It is the responsibility of the caller to ensure that a
 * request is not retried when it is already in the request
 * queue.
 *
 * \param request
 *      Pointer to the request to be read in.
 */
inline void
MultiOp::retryRequest(MultiOpObject* request) {
    assert(startIndex > 0);

    startIndex--;
    request->status = STATUS_RETRY;
    workQueue[startIndex] = request;
}

/**
 * Constructor for PartRpc objects.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param type
 *      The Multi OpType kind of messages this PartRpc shall contain.
 * \param session
 *      Session on which this RPC will eventually be sent.
 */
MultiOp::PartRpc::PartRpc(RamCloud* ramcloud,
        Transport::SessionRef session, WireFormat::MultiOp::OpType type)
    : RpcWrapper(sizeof(WireFormat::MultiOp::Response))
    , ramcloud(ramcloud)
    , session(session)
    , requests()
    , reqHdr(allocHeader<WireFormat::MultiOp>())
{
    reqHdr->type = type;
    reqHdr->count = 0;
}

// See RpcWrapper for documentation.
bool
MultiOp::PartRpc::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mappings.  The objects
    // will all be retried when \c finish is called.
    if (session.get() != NULL) {
        ramcloud->clientContext->transportManager->flushSession(
                session->getServiceLocator());
        session = NULL;
    }
    for (uint32_t i = 0; i < reqHdr->count; i++) {
        ramcloud->objectFinder.flush(requests[i]->tableId);
    }
    return true;
}

// See RpcWrapper for documentation
void
MultiOp::PartRpc::send()
{
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

} // namespace RAMCloud
