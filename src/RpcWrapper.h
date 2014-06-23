/* Copyright (c) 2012-2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_RPCWRAPPER_H
#define RAMCLOUD_RPCWRAPPER_H

#include "Dispatch.h"
#include "Fence.h"
#include "ServerId.h"
#include "Transport.h"
#include "WireFormat.h"

namespace RAMCloud {
class RamCloud;

/**
 * RpcWrapper is the base class for a collection of classes that provide
 * a "middle management" layer just above RPC transports.  Wrappers serve
 * two overall purposes:
 * - They implement various strategies for retrying RPCs after failures.
 *   Typically a single strategy is shared among many RPCs (e.g., all RPCs
 *   attempting to communicate with the coordinator use the same strategy).
 * - They allow RPCs to be invoked asynchronously (they hold the state of
 *   the RPC, such as request and response buffers, when the invoker is off
 *   doing something else).
 * There are three layers of wrapper objects:
 * - This class is the bottommost layer, providing facilities that are
 *   common across all wrappers.
 * - This class is subclassed by the next layer, consisting of a small
 *   number of classes that implement different retry policies (see, for
 *   example, CoordinatorWrapper).
 * - The topmost layer consists of one class for each RPC, which is a
 *   subclass of the one of the middle-layer classes. These classes
 *   pack request messages and unpack response messages in an RPC-specific
 *   fashion.
 *
 * RpcWrappers are not thread-safe in general: they do correctly handle
 * synchronization with transports running in a separate dispatch thread,
 * but if a single RpcWrapper is accessed by multiple threads for any
 * other purpose, the threads must synchronize among themselves to ensure
 * that at most one thread is invoking an RpcWrapper method at a time.
 */
class RpcWrapper : public Transport::RpcNotifier {
  public:
    explicit RpcWrapper(uint32_t responseHeaderLength,
            Buffer* response = NULL);
    virtual ~RpcWrapper();

    void cancel();
    virtual void completed();
    virtual void failed();
    bool isReady();

  PROTECTED:
    /// Possible states for an RPC.
    enum RpcState {
        NOT_STARTED,                // Initial state before the request has
                                    // been sent for the first time.
        IN_PROGRESS,                // A request has been sent but no response
                                    // has been received.
        FINISHED,                   // The RPC has completed and the server's
                                    // response is available in response.
        FAILED,                     // The RPC has failed due to a transport
                                    // error; most likely it will be retried.
        CANCELED,                   // The RPC has been canceled, so it
                                    // will never complete.
        RETRY                       // The RPC needs to be retried at the
                                    // time given by retryTime.
    };

    /**
     * Given an RPC type (from WireFormat), allocates a request header
     * in the request buffer, initializes its opcode and service fields,
     * and zeroes everything else.
     *
     * \tparam RpcType
     *      A type from WireFormat, such as WireFormat::Read; determines
     *      the type of the return value and the size of the header.
     *
     * \return
     *      An RPC-specific request header.
     */
    template <typename RpcType>
    typename RpcType::Request*
    allocHeader()
    {
        assert(request.size() == 0);
        typename RpcType::Request* reqHdr =
                request.emplaceAppend<typename RpcType::Request>();
        // Don't allow this method to be used for RPCs that use
        // RequestCommonWithId as the header; use the other form
        // with targetId instead.
        static_assert(sizeof(reqHdr->common) ==
                sizeof(WireFormat::RequestCommon),
                "must pass targetId to allocHeader");
        memset(reqHdr, 0, sizeof(*reqHdr));
        reqHdr->common.opcode = RpcType::opcode;
        reqHdr->common.service = RpcType::service;
        return reqHdr;
    }

    /**
     * Given an RPC type (from WireFormat) that uses RequestCommonWithId
     * (i.e. the request header contains a target server id), allocates a
     * request header in the request buffer, initializes its opcode,
     * service, and targetId fields, and zeroes everything else.
     *
     * \tparam RpcType
     *      A type from WireFormat, such as WireFormat::Read; determines
     *      the type of the return value and the size of the header.
     * 
     * \param targetId
     *      ServerId indicating which server is intended to process this
     *      request.
     *
     * \return
     *      An RPC-specific request header.
     */
    template <typename RpcType>
    typename RpcType::Request*
    allocHeader(ServerId targetId)
    {
        assert(request.size() == 0);
        typename RpcType::Request* reqHdr =
                request.emplaceAppend<typename RpcType::Request>();
        memset(reqHdr, 0, sizeof(*reqHdr));
        reqHdr->common.opcode = RpcType::opcode;
        reqHdr->common.service = RpcType::service;
        reqHdr->common.targetId = targetId.getId();
        return reqHdr;
    }

    /**
     * This method is limited by RpcWrapper subclasses and is invoked
     * by RpcWrapper when an RPC response contains an error status other than
     * the ones the RpcWrapper knows how to deal with. This method gives
     * the subclass a chance to handle the status if it can (for example,
     * ObjectRpcWrapper handles STATUS_UNKNOWN_TABLET by refreshing the
     * configuration cache and retrying).
     * \return
     *      The return value from this method becomes the return value
     *      from RpcWrapper::isReady: true means that the RPC is now
     *      finished (for better or worse), false means it is still
     *      being processed (e.g. a retry was initiated).
     */
    virtual bool checkStatus();

    // The following declaration is a total dummy; it exists merely to
    // provide shared documentation that can be referenced by wrapper-
    // specific declarations of \c wait.

    /**
     * Wait for the RPC to complete, and throw exceptions for any errors.
     */
    void docForWait();

    /**
     * Assuming that isReady has returned true, this method will return
     * the header from the response. This method should not be invoked
     * unless isReady has returned true.
     *
     * \tparam RpcType
     *      A type from WireFormat, such as WireFormat::Read; determines
     *      the type of the result.
     * \return
     *      An RPC-specific response header.
     */
    template <typename RpcType>
    const typename RpcType::Response*
    getResponseHeader()
    {
        assert(responseHeader != NULL);
        assert(responseHeaderLength >= sizeof(typename RpcType::Response));
        return reinterpret_cast<const typename RpcType::Response*>(
                responseHeader);
    }

    /**
     * This method provides safe synchronized access to the #state
     * variable, and is the only mechanism that should be used to read
     * #state.
     *
     * \return
     *      Current state of processing for this RPC.
     */
    RpcState getState() {
        RpcState result = state;
        Fence::lfence();
        return result;
    }

    virtual bool handleTransportError();
    void retry(uint32_t minDelayMicros, uint32_t maxDelayMicros);
    virtual void send();
    void simpleWait(Dispatch* dispatch);
    const char* stateString();
    bool waitInternal(Dispatch* dispatch, uint64_t abortTime = ~0UL);

    /// Request and response messages.  In some cases the response buffer
    /// is provided by the wrapper (e.g., for reads); if not, response refers
    /// to defaultResponse.
    Buffer request;
    Buffer* response;

    /// Storage to use for response if constructor was not given an explicit
    /// response buffer.
    Tub<Buffer> defaultResponse;

    /// Current state of processing this RPC. This variable may be accessed
    /// concurrently by wrapper methods running in one thread and transport
    /// code running in the dispatch thread (transports can only invoke the
    /// completed and failed methods).
    Atomic<RpcState> state;

    /// Session on which RPC has been sent, or NULL if none.
    Transport::SessionRef session;

    /// Retry the RPC when Cycles::rdtsc reaches this value.
    uint64_t retryTime;

    /// Expected size of the response header, in bytes.
    uint32_t responseHeaderLength;

    /// Response header; filled in by isReady, so that wrapper functions
    /// don't have to recompute it.  Guaranteed to actually refer to at
    /// least responseHeaderLength bytes.
    const WireFormat::ResponseCommon* responseHeader;

    DISALLOW_COPY_AND_ASSIGN(RpcWrapper);
};

} // end RAMCloud

#endif  // RAMCLOUD_RPCWRAPPER_H
