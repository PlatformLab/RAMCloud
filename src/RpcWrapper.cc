/* Copyright (c) 2012-2015 Stanford University
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

#include "ClientException.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "Exception.h"
#include "Logger.h"
#include "RpcWrapper.h"
#include "ShortMacros.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * Constructor for RpcWrapper objects.
 * \param responseHeaderLength
 *      The size of header expected in the response for this RPC;
 *      incoming responses will be checked by this class to ensure that
 *      they contain at least this much data, wrapper subclasses can
 *      use the getResponseHeader method to access the response header
 *      once isReady has returned true.
 * \param response
 *      Optional client-supplied buffer to use for the RPC's response;
 *      if NULL then we use a built-in buffer. Any existing contents
 *      of this buffer will be cleared automatically by the transport.
 */
RpcWrapper::RpcWrapper(uint32_t responseHeaderLength, Buffer* response)
    : request()
    , response(response)
    , defaultResponse()
    , state(NOT_STARTED)
    , session(NULL)
    , retryTime(0)
    , responseHeaderLength(responseHeaderLength)
    , responseHeader(NULL)
{
    if (response == NULL) {
        defaultResponse.construct();
        this->response = defaultResponse.get();
    }
}

/**
  * Destructor for RpcWrapper. If the RPC is in progress then it is
  * canceled.
  */
RpcWrapper::~RpcWrapper() {
    cancel();
}

/**
 * Abort the RPC (if it hasn't already completed).  Once this method
 * returns the transport will no longer access this object or the
 * associated buffers. Furthermore, #waitInternal will throw
 * RpcCanceledException if it is invoked.
 */
void
RpcWrapper::cancel()
{
    // This code is potentially tricky, because complete or failed
    // could get invoked concurrently. Fortunately, we know that
    // this can only happen before cancelRequest returns, and
    // neither of these methods does anything besides setting state.
    if ((getState() == IN_PROGRESS) && session) {
        session->cancelRequest(this);
    }
    state = CANCELED;
}

/**
 * This method is invoked by isReady when an RPC returns with an error
 * status that isn't known to isReady. Subclasses can override the
 * default implementation to handle some status values (such as
 * STATUS_UNKNOWN_TABLET) by retrying the request.
 *
 * \return
 *      The return value from this method will be the return value
 *      from isReady.  True means no retry was performed, so the
 *      RPC is now finished. False means that this method initiated
 *      a retry, so the RPC is still in progress.
 */
bool
RpcWrapper::checkStatus()
{
    // Default version that does no additional checks.
    return true;
}

// See Transport::Notifier for documentation.
void
RpcWrapper::completed() {
    // Since this method can be invoked concurrently with other
    // methods, it's important that it does nothing except modify
    // state. Don't add any more functionality to this method
    // unless you carefully review all of the synchronization
    // properties of RpcWrappers!
    Fence::sfence();
    state = FINISHED;
}


// See Transport::Notifier for documentation.
void
RpcWrapper::failed() {
    // See comment in completed: the same warning applies here.
    Fence::sfence();
    state = FAILED;
}


/**
 * This method is implemented in RpcWrapper subclasses; it is invoked
 * by isReady to handle RPC failures that occur because of transport
 * errors. Typically the method will open a new session and retry the
 * RPC.
 *
 * \return
 *      The return value from this method will be the return value
 *      from isReady.  True means no retry was performed, so the
 *      RPC is now finished. False means that this method initiated
 *      a retry, so the RPC is still in progress.
 */
bool
RpcWrapper::handleTransportError()
{
    // Default version that does not handle errors; probably only useful
    // for testing.
    return true;
}

/**
 * Indicates whether a response has been received for an RPC.  Used
 * for asynchronous processing of RPCs.
 *
 * \return
 *      True means that the RPC has finished or been canceled; #wait will
 *      not block.  False means that the RPC is still being processed.
 */
bool
RpcWrapper::isReady() {
    // Note: in addition to indicating whether the RPC is complete,
    // this method is where all the work of retrying is implemented.
    RpcState copyOfState = getState();

    if (copyOfState == FINISHED) {
        // Retrieve the status value from the response and handle the
        // normal case of success as quickly as possible.  Note: check to
        // make sure the server has returned enough bytes for the header length
        // expected by the wrapper, but we only use the status word here.
        responseHeader = static_cast<const WireFormat::ResponseCommon*>(
                response->getRange(0, responseHeaderLength));
        if ((responseHeader != NULL) &&
                (responseHeader->status == STATUS_OK)) {
            return true;
        }

        // We only get to here if something unusual happened. Work through
        // all of the special cases one at a time.
        if (responseHeader == NULL) {
            // Not enough bytes in the response for a full header; check to see
            // if there is at least a status value. Note: we're asking for
            // fewer bytes here than in the getRange above (just enough for
            // the status info instead of the entire response header that
            // higher level wrapper will want).
            responseHeader = static_cast<const WireFormat::ResponseCommon*>(
                    response->getRange(0, sizeof(WireFormat::ResponseCommon)));
            if ((responseHeader == NULL)
                    || (responseHeader->status == STATUS_OK)) {
                LOG(ERROR, "Response from %s for %s RPC is too short "
                        "(needed at least %d bytes, got %d)",
                        session->getServiceLocator().c_str(),
                        WireFormat::opcodeSymbol(&request),
                        responseHeaderLength,
                        downCast<int>(response->size()));
                throw MessageTooShortError(HERE);
            }
        }
        if (responseHeader->status == STATUS_RETRY) {
            // The server wants us to try again; typically this means that
            // it is overloaded, or there is some reason why the operation
            // can't complete right away and it's better for us to wait
            // here, rather than occupy resources on the server by waiting
            // there.
            WireFormat::RetryResponse defaultResponse =
                    {{STATUS_RETRY}, 100, 200, 0};
            const WireFormat::RetryResponse* retryResponse =
                    response->getStart<WireFormat::RetryResponse>();
            if (retryResponse == NULL) {
                retryResponse = &defaultResponse;
            }
            if (retryResponse->messageLength > 0) {
                const char* message = static_cast<const char*>(
                        response->getRange(sizeof32(WireFormat::RetryResponse),
                        retryResponse->messageLength));
                if (message == NULL) {
                    message = "<response format error>";
                }
                RAMCLOUD_CLOG(NOTICE,
                        "Server %s returned STATUS_RETRY from %s request: %s",
                        session->getServiceLocator().c_str(),
                        WireFormat::opcodeSymbol(&request),
                        message);
            } else {
                RAMCLOUD_CLOG(NOTICE,
                        "Server %s returned STATUS_RETRY from %s request",
                        session->getServiceLocator().c_str(),
                        WireFormat::opcodeSymbol(&request));
            }
            retry(retryResponse->minDelayMicros,
                    retryResponse->maxDelayMicros);
            return false;
        }

        // The server returned an error status that we don't know how to
        // process; see if a subclass knows what to do with it.
        return checkStatus();
    }

    if (copyOfState == IN_PROGRESS) {
        return false;
    }

    if (copyOfState == RETRY) {
        if (Cycles::rdtsc() >= retryTime) {
            send();
        }
        return false;
    }

    if (copyOfState == CANCELED) {
        return true;
    }

    if (copyOfState == FAILED) {
        // There was a transport-level failure, which the transport should
        // already have logged. Invoke a subclass to decide whether or not
        // to retry.
        return handleTransportError();
    }

    LOG(ERROR, "RpcWrapper::isReady found unknown state %d for "
            "%s request", copyOfState, WireFormat::opcodeSymbol(&request));
    throw InternalError(HERE, STATUS_INTERNAL_ERROR);
    return false;
}

/**
 * This method is invoked in situations where the RPC should be retried
 * after a time delay. This method sets up state for that delay; it doesn't
 * actually wait for the delay to complete.
 * \param minDelayMicros
 *      Minimum time to wait, in microseconds.
 * \param maxDelayMicros
 *      Maximum time to wait, in microseconds. The actual delay time
 *      will be chosen randomly between minDelayMicros and maxDelayMicros.
 */
void
RpcWrapper::retry(uint32_t minDelayMicros, uint32_t maxDelayMicros)
{
    uint64_t delay = minDelayMicros;
    if (minDelayMicros != maxDelayMicros) {
        delay += randomNumberGenerator(maxDelayMicros + 1 - minDelayMicros);
    }
    retryTime = Cycles::rdtsc() + Cycles::fromNanoseconds(1000*delay);
    state = RETRY;
}


/**
 * This method is implemented by various subclasses to start sending
 * an RPC request to a server.  It is invoked by wrapper subclasses
 * once the request message has been filled in, and it is also invoked
 * to initiate retries. It finds an appropriate session and starts
 * transmitting the request message via that session. It returns once the
 * transport has initiated the send.
 */
void
RpcWrapper::send()
{
    // Most subclasses override this method.  There are two reasons for
    // using this default method:
    // - For testing
    // - For a few RPCs that compute their own sessions using unusual
    //   approaches (such as using service locators): they must set the
    //   session member before invoking this method.

    state = IN_PROGRESS;
    if (session)
        session->sendRequest(&request, response, this);
}

/**
 * This method provides a simple implementation of \c wait that
 * doesn't do any processing of the result; it just waits for completion
 * and checks for errors.  It is invoked by various other wrapper
 * classes as their implementation of \c wait.
 *
 * \param context
 *      The dispatcher for this context will be used for polling while waiting.
 */
void
RpcWrapper::simpleWait(Context* context)
{
    waitInternal(context->dispatch);
    if (responseHeader->status != STATUS_OK)
        ClientException::throwException(HERE, responseHeader->status);
}

/**
 * Return a string corresponding to the current state of the wrapper.
 * This method is intended only for testing.
 */
const char*
RpcWrapper::stateString() {
    switch (state) {
        case RpcWrapper::RpcState::NOT_STARTED:
            return "NOT_STARTED";
        case RpcWrapper::RpcState::IN_PROGRESS:
            return "IN_PROGRESS";
        case RpcWrapper::RpcState::FINISHED:
            return "FINISHED";
        case RpcWrapper::RpcState::FAILED:
            return "FAILED";
        case RpcWrapper::RpcState::CANCELED:
            return "CANCELED";
        case RpcWrapper::RpcState::RETRY:
            return "RETRY";
    }
    static char buffer[100];
    snprintf(buffer, sizeof(buffer), "unknown (%d)", int(state));
    return buffer;
}

/**
 * This method is typically invoked by wrapper subclasses. It waits for the
 * RPC to complete (either with a response, an unrecoverable failure, or a
 * timeout) and may attempt to retry the RPC after certain failures.
 *
 * \param dispatch
 *      Dispatch to use for polling while waiting.
 * \param abortTime
 *      If Cycles::rdtsc() exceeds this time then return even if the
 *      RPC has not completed. All ones means wait forever.
 *
 * \return
 *      The return value is true if the RPC completed or failed, and false if
 *      abortTime passed with no response yet.
 *
 * \throw RpcCanceledException
 *      The RPC has previously been canceled, so it doesn't make sense
 *      to wait for it.
 */
bool
RpcWrapper::waitInternal(Dispatch* dispatch, uint64_t abortTime)
{
    // When invoked in RAMCloud servers there is a separate dispatch thread,
    // so we just busy-wait here. When invoked on RAMCloud clients we're in
    // the dispatch thread so we have to invoke the dispatcher while waiting.
    bool isDispatchThread = dispatch->isDispatchThread();

    while (!isReady()) {
        if (isDispatchThread)
            dispatch->poll();
        if (dispatch->currentTime > abortTime)
            return false;
    }
    if (getState() == CANCELED)
        throw RpcCanceledException(HERE);
    return true;
}

} // namespace RAMCloud
