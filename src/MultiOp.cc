/* Copyright (c) 2013-2015 Stanford University
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

#include <utility>

#include "Context.h"
#include "Dispatch.h"
#include "MultiOp.h"
#include "ObjectFinder.h"
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
 * Internally, startRPCs dispatches the request array into sessionQueues,
 * each of which buffers requests intended for a particular master (identified
 * by its session).  Whenever a session queue reaches MAX_OBJECTS_PER_RPC it is
 * packaged into an RPC and sent.  All sesseion queues are drained (sent) once
 * all requests are dispatched.  startRPC is typically called multiple times and
 * it returns to isReady whenever MAX_RPC RPCs are underway or when the MultiOp
 * is finished.
 *
 * isReady takes care of the respones by calling finishRpc, which can trigger a
 * retry of an RPC if necessary.  The retry re-inserts the RPC into
 * sessionQueues, allowing the session queue to grow slightly beyond
 * MAX_OBJECTS_PER_RPC.
 *
 * isReady also calls startRPC when there is at least one free RPC.  The list
 * of free RPCs and RPCs underway is maintained through startIndexIdleRpc and
 * ptrRpcs.  The rpcs array itself is unordered, so free RPCs and RPCs underway
 * are interleaved in the array.
 */

/**
 * The constructor is intended to be invoked ONLY by the subclass. If the
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
    , numDispatched(0)
    , rpcs()
    , startIndexIdleRpc(0)
    , canceled(false)
    , sessionQueues()
    , test_ignoreBufferOverflow(false)
{
    for (uint32_t i = 0; i < MAX_RPCS; ++i) {
        ptrRpcs[i] = &rpcs[i];
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
 * Looks up the SessionQueue that matches the request's session and adds the
 * the request.  Creates a new session buffer if the requests is for a formerly
 * unseen session.
 *
 * \param request
 *      Either the next request from requests array or a request that needs to
 *      be retried.
 * \param[out] session
 *      Session that identifies the master that must service this request
 *      (returned by ObjectFinder).  Can be NULL if the request is for a
 *      non-existing table.
 * \param[out] queue
 *      Points to either a newly created session queue or a previously created
 *      queue in sessionQueues.  Its last element is request, unless request
 *      is for a non-existing table.
 */
void
MultiOp::dispatchRequest(MultiOpObject* request,
    Transport::SessionRef *session, SessionQueue **queue)
{
    *queue = NULL;
    *session = NULL;

    try {
       *session = ramcloud->clientContext->objectFinder->lookup(
               request->tableId, request->key, request->keyLength);
    }
    catch (TableDoesntExistException &e) {
        request->status = STATUS_TABLE_DOESNT_EXIST;
        return;
    }

    auto iter = sessionQueues.find(*session);
    if (iter == sessionQueues.end()) {
        *queue = new SessionQueue();
        (*queue)->reserve(PartRpc::MAX_OBJECTS_PER_RPC);
        (*queue)->push_back(request);
        sessionQueues.insert(
            SessionQueues::value_type(*session,
                                      std::shared_ptr<SessionQueue>(*queue)));
    } else {
        *queue = iter->second.get();
        (*queue)->push_back(request);
    }
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
    uint32_t respSize = rpc->response->size();

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
            ramcloud->clientContext->objectFinder->flush(request->tableId);
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
 * Package up to MAX_OBJECTS_PER_RPC requests from a session buffer into an RPC
 * and send the RPC.
 *
 * \param session
 *      The session of the requests in queue
 * \param queue
 *      An array of requests for session.  The array can be larger than
 *      MAX_OBJECTS_PER_RPC but at most one RPC is sent.  The size of a non
 *      empty queue will decrease.
 */
void
MultiOp::flushSessionQueue(Transport::SessionRef session, SessionQueue *queue) {
    assert(startIndexIdleRpc < MAX_RPCS);

    Tub<PartRpc> *rpc = ptrRpcs[startIndexIdleRpc];
    rpc->construct(ramcloud, session, opType);
    startIndexIdleRpc++;

    size_t queueLen = queue->size();
    size_t residue = (queueLen <= PartRpc::MAX_OBJECTS_PER_RPC) ?
        0 : queueLen - PartRpc::MAX_OBJECTS_PER_RPC;
    while (queueLen > residue) {
        MultiOpObject *request = (*queue)[queueLen-1];
        uint32_t lengthBefore = (*rpc)->request.size();
        appendRequest(request, &((*rpc)->request));
        uint32_t lengthAfter = (*rpc)->request.size();

        // Check for full rpc, remove + retry
        if (lengthAfter > maxRequestSize) {
            (*rpc)->request.truncate(lengthBefore);

            // Check that request isn't too big in general
            if (lengthAfter - lengthBefore > maxRequestSize) {
                request->status = STATUS_REQUEST_TOO_LARGE;
                queueLen--;
                continue;
            }
            break;
        }

        request->status = UNDERWAY;
        (*rpc)->requests[(*rpc)->reqHdr->count] = request;
        (*rpc)->reqHdr->count++;
        queueLen--;
    }
    // At this point, queueLen must have been decreased by at least 1

    // Send if there is at least one good request in the RPC
    if ((*rpc)->reqHdr->count > 0) {
        (*rpc)->send();
    } else {
        rpc->destroy();
        startIndexIdleRpc--;
    }
    // Not sure if vector::resize does this internally anyway
    (queueLen == 0) ? queue->clear() : queue->resize(queueLen);
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
    // results).  Put the returned RPCs in the upper end of free RPCs in
    // ptrRpcs.
    for (uint32_t i = 0; i < startIndexIdleRpc; ) {
        Tub<PartRpc>& rpc = *(ptrRpcs[i]);
        if (rpc->isReady()) {
            finishRpc(rpc.get());
            rpc.destroy();
            startIndexIdleRpc--;
            std::swap(ptrRpcs[i], ptrRpcs[startIndexIdleRpc]);
        } else {
            ++i;
        }
    }

    // See if there's more work we can schedule.
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
    if (startIndexIdleRpc == MAX_RPCS) {
        // Nothing more we can do now except wait for RPCs to finish.
        return false;
    }

    // Dispatch remaining RPCs to session queues.  Flush session queues as they
    // are large enough to fill a multi-op RPC.  Stop when the maximum number
    // of in-flight RPCs is reached.
    while (numDispatched < numRequests) {
        MultiOpObject * const request = requests[numDispatched];
        SessionQueue* queue;
        Transport::SessionRef session;

        dispatchRequest(request, &session, &queue);
        ++numDispatched;
        // Invalid requests are ignored
        if (queue == NULL) {
            continue;
        }
        if (queue->size() >= PartRpc::MAX_OBJECTS_PER_RPC) {
            flushSessionQueue(session, queue);
        }

        if (startIndexIdleRpc == MAX_RPCS) {
            // Must wait for outstanding RPCs to return before starting any
            // more RPCs.
            return false;
        }
    }
    assert(startIndexIdleRpc < MAX_RPCS);

    // At this point all of the requests have been dispatched to SessionQueues,
    // but there may be SessionQueues for which RPCs have not yet been sent.
    // Start up more RPCs if possible.

    std::unordered_map<Transport::SessionRef, std::shared_ptr<SessionQueue>,
        HashSessionRef>::iterator i;
    for (i = sessionQueues.begin(); i != sessionQueues.end(); ) {
        Transport::SessionRef session = i->first;
        SessionQueue *queue = i->second.get();

        if (queue->size() > 0) {
            flushSessionQueue(session, queue);
        }

        // Unless we have to retry an RPC, we don't need empty queues anymore
        // and we don't want to iterate through them again
        if (queue->size() == 0) {
            auto erase_me = i++;
            sessionQueues.erase(erase_me);
        }

        if (startIndexIdleRpc == MAX_RPCS) {
            return false;
        }
    }

    // No more work when no new RPCs have been sent
    return startIndexIdleRpc == 0;
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
 * Adds a request back to a session buffer.
 *
 * \param request
 *      Pointer to the request to be read in.
 */
inline void
MultiOp::retryRequest(MultiOpObject* request) {
    request->status = STATUS_RETRY;
    Transport::SessionRef session;
    SessionQueue *queue;
    dispatchRequest(request, &session, &queue);
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
        ramcloud->clientContext->objectFinder->flush(requests[i]->tableId);
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
