/* Copyright (c) 2012 Stanford University
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

#include "MultiRead.h"
#include "Segment.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Constructor for MultiRead objects: initiates one or more RPCs for a
 * multiRead operation, but returns once the RPCs have been initiated,
 * without waiting for any of them to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this operation.
 * \param requests
 *      Each element in this array describes one object to read, and where
 *      to place its value.
 * \param numRequests
 *      Number of elements in \c requests.
 */
MultiRead::MultiRead(RamCloud* ramcloud, MultiReadObject* requests[],
        uint32_t numRequests)
    : ramcloud(ramcloud)
    , requests(requests)
    , numRequests(numRequests)
    , rpcs()
    , canceled(false)
{
    startRpcs();
    for (uint32_t i = 0; i < numRequests; i++) {
        requests[i]->value->destroy();
    }
}

/**
 * Abort this request if it hasn't already completed.  Once this
 * method returns wait will throw RpcCanceledException if it is invoked.
 */
void
MultiRead::cancel()
{
    for (uint32_t i = 0; i < MAX_RPCS; i++) {
        rpcs[i].destroy();
    }
    canceled = true;
}

/**
 * Check to see whether the multi-Read operation is complete.  If not,
 * start more RPCs if needed.
 *
 * \return
 *      True means that the operation has finished or been canceled; #wait
 *      will not block.  False means that one or more RPCs are still underway.
 */
bool
MultiRead::isReady() {
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
        if (!rpc->isReady()) {
            continue;
        }
        finished++;
        rpc->finish();
        rpc.destroy();
        slotsAvailable++;
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
 *      True means there is no more work to do: the multiRead
 *      is done.  False means there are RPCs outstanding.
 */
bool
MultiRead::startRpcs()
{
    // Each iteration through the following loop examines one of the
    // objects we are supposed to read.  If we haven't already read it,
    // add it to an outgoing RPC if possible.  We won't necessarily
    // be able to read all of the objects in one shot, due to limitations
    // on the number of objects we can read in one RPC and the number
    // of outstanding RPCs we can have.
    for (uint32_t i = 0; i < numRequests; i++) {
        MultiReadObject& request = *requests[i];
        if (*request.value) {
            // This object has already been read successfully.
            continue;
        }
        if (request.status != STATUS_OK) {
            // We tried to read this object and failed (or the object is
            // being requested in an active RPC (status UNDERWAY)). No
            // need to try again.
            continue;
        }
        Transport::SessionRef session;
        try {
            session = ramcloud->objectFinder.lookup(request.tableId,
                    request.key, request.keyLength);
        }
        catch (TableDoesntExistException &e) {
            request.status = STATUS_TABLE_DOESNT_EXIST;
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
                rpc.construct(ramcloud, session);
            }
            if (rpc->session == session) {
                if ((rpc->reqHdr->count < PartRpc::MAX_OBJECTS_PER_RPC)
                        && (rpc->state == RpcWrapper::RpcState::NOT_STARTED)) {
                    // Add the current object to the list of those being
                    // fetched by this RPC.
                    new(&rpc->request, APPEND)
                            WireFormat::MultiRead::Request::Part(
                            request.tableId, request.keyLength);
                    Buffer::Chunk::appendToBuffer(&rpc->request, request.key,
                            request.keyLength);
                    rpc->requests[rpc->reqHdr->count] = &request;
                    rpc->reqHdr->count++;
                    request.status = UNDERWAY;
                }
                break;
            }
        }
    }

    // Now launch all of the new RPCs we have generated.
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
 * Wait for the multiRead operation to complete.
 */
void
MultiRead::wait()
{
    // When invoked in RAMCloud servers there is a separate dispatch thread,
    // so we just busy-wait here. When invoked on RAMCloud clients we're in
    // the dispatch thread so we have to invoke the dispatcher while waiting.
    bool isDispatchThread =
            ramcloud->clientContext.dispatch->isDispatchThread();

    while (true) {
        if (canceled)
            throw RpcCanceledException(HERE);
        if (isReady())
            return;
        if (isDispatchThread)
            ramcloud->clientContext.dispatch->poll();
    }
}

/**
 * Constructor for PartRpc objects.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param session
 *      Session on which this RPC will eventually be sent.
 */
MultiRead::PartRpc::PartRpc(RamCloud* ramcloud,
        Transport::SessionRef session)
    : RpcWrapper(sizeof(WireFormat::MultiRead::Response))
    , ramcloud(ramcloud)
    , session(session)
    , requests()
    , reqHdr(allocHeader<WireFormat::MultiRead>())
{
    reqHdr->count = 0;
}

/**
 * This method is invoked when an RPC finishes; it copies result
 * information back to where the multiRead caller expects it, and
 * handles certain error conditions.
 */
void
MultiRead::PartRpc::finish()
{
    bool messageLogged = false;
    assert(getState() != IN_PROGRESS);

    uint32_t i;
    if (getState() != FINISHED) {
        // Transport error or canceled; just reset state so that all of
        // the objects will be retried.
        for (i = 0; i < reqHdr->count; i++) {
            requests[i]->status = STATUS_OK;
        }
    }

    // The following variable acts as a cursor in the response as
    // we scan through the results for each request.
    uint32_t respOffset = sizeof32(WireFormat::MultiRead::Response);

    // Each iteration extracts one object from the response.  Be careful
    // to handle situations where the response is too short.  This can
    // happen legitimately if the server ran out of room in the response
    // buffer.
    for (i = 0; i < reqHdr->count; i++) {
        MultiReadObject& request = *requests[i];
        const Status* status = response->getOffset<Status>(respOffset);
        if (status == NULL) {
            TEST_LOG("missing status");
            break;
        }

        request.status = *status;
        respOffset += sizeof32(Status);

        if (*status == STATUS_OK) {
            const WireFormat::MultiRead::Response::Part* part =
                response->getOffset<
                    WireFormat::MultiRead::Response::Part>(respOffset);
            if (part == NULL) {
                TEST_LOG("missing Response::Part");
                break;
            }
            respOffset += sizeof32(*part);

            if (response->getTotalLength() < respOffset + part->length) {
                TEST_LOG("missing object data");
                break;
            }

            request.value->construct();
            void* data = new(request.value->get(), APPEND) char[part->length];
            response->copy(respOffset, part->length, data);
            request.version = part->version;
            respOffset += part->length;
        } else {
            if (*status == STATUS_UNKNOWN_TABLET) {
                // The object isn't where we thought it should be. Refresh our
                // configuration cache and arrange for this object to be
                // fetched again.
                if (!messageLogged) {
                    // (Only log one message per call to this method).
                    LOG(NOTICE, "Server %s doesn't store <%lu, %*s>; "
                            "refreshing object map",
                            session->getServiceLocator().c_str(),
                            request.tableId, (request.keyLength > 100) ? 100 :
                                    request.keyLength,
                            reinterpret_cast<const char*>(request.key));
                    messageLogged = true;
                }
                ramcloud->objectFinder.flush();
                request.status = STATUS_OK;
            }
        }
    }

    // When we get here, it's possible that we aborted part way through
    // because we hit the end of the buffer. For all objects we haven't
    // currently processed, reset their statuses to indicate that these
    // objects need to be fetched again.

    for ( ; i < reqHdr->count; i++) {
        requests[i]->status = STATUS_OK;
    }
}

// See RpcWrapper for documentation.
bool
MultiRead::PartRpc::handleTransportError()
{
    // There was a transport-level failure. Flush cached state related
    // to this session, and related to the object mappings.  The objects
    // will all be retried when \c finish is called.
    if (session.get() != NULL) {
        ramcloud->clientContext.transportManager->flushSession(
                session->getServiceLocator().c_str());
        session = NULL;
    }
    ramcloud->objectFinder.flush();
    return true;
}

// See RpcWrapper for documentation
void
MultiRead::PartRpc::send()
{
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

} // namespace RAMCloud
