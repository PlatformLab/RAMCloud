/* Copyright (c) 2012 Stanford University
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

#ifndef RAMCLOUD_MULTIOP_H
#define RAMCLOUD_MULTIOP_H

#include "RamCloud.h"
#include "RpcWrapper.h"
#include "Transport.h"
#include "WireFormat.h"

namespace RAMCloud {
/**
 * This class implements the client side framework for MasterService multi-ops.
 * It manages multiple concurrent RPCs, each requesting one or more objects
 * from a single server. The behavior of this class is similar to an
 * RpcWrapper, but it isn't an RpcWrapper subclass because it doesn't
 * correspond to a single RPC. Implementation-specific information about this
 * class can be found at the top of MultiOp.cc
 *
 * To add a new multi-operation, xxx, to this framework follow these steps:
 *   1) In Ramcloud.h, create a new MultixxxObject that extends MultiOpObject.
 *      This MultixxxObject will serve as a place to pass in parameters
 *      and store server responses.
 *
 *   2) In WireFormat.h, find struct MultiOp. Inside,
 *      2a) Create a new enum OpType to identify your operation.
 *      2b) Create a new struct xxxPart within MultiOp::Request
 *          and MultiOp::Response.
 *
 *   3) In xxx.cc and xxx.h, create a new Multixxx class that extends MultiOp.
 *      3a) Extend the constructor. Contract/Description located in MultiOp.cc
 *      3b) Implement the two PROTECTED virtual functions. Contract/Description
 *          located below.
 *
 *   4) In MasterService.cc, find the MasterService::multiOp, add your
 *      OpType to the switch statement and implement the server-side multi-op.
 *
 *   5) (optional) Add a high-level call in RamCloud.cc for Multixxx
 *
 * See MultiRead and MultiWrite for examples of multi operations using this
 * framework.
 */
class MultiOp {
  public:
    /// Constructor is PROTECTED; this class is meant to be abstracted.
    virtual ~MultiOp() {}

    void cancel();
    bool isReady();
    void wait();

  PROTECTED:
    MultiOp(RamCloud* ramcloud,  WireFormat::MultiOp::OpType type,
                MultiOpObject * const requests[], uint32_t numRequests);

  /// Encapsulates the state of a single RPC sent to a single server.
    class PartRpc : public RpcWrapper {
        friend class MultiOp;
      public:
        PartRpc(RamCloud* ramcloud, Transport::SessionRef session,
                WireFormat::MultiOp::OpType type);
        virtual ~PartRpc() {}
        bool inProgress() {return getState() == IN_PROGRESS;
                                };
        bool isFinished() {return getState() == FINISHED;
                                };
        bool handleTransportError();
        void send();

        /// Overall client state information.
        RamCloud* ramcloud;

        /// Session that will be used to transmit the RPC.
        Transport::SessionRef session;

        /// Information about all of the objects that are being requested
        /// in this RPC.
#ifdef TESTING
        static const uint32_t MAX_OBJECTS_PER_RPC = 3;
#else
        static const uint32_t MAX_OBJECTS_PER_RPC = 75;
#endif
        MultiOpObject* requests[MAX_OBJECTS_PER_RPC];

        /// Header for the RPC (used to update count as objects are added).
        WireFormat::MultiOp::Request* reqHdr;

        DISALLOW_COPY_AND_ASSIGN(PartRpc);
    };


    bool startRpcs();

  PRIVATE:
    void finishRpc(MultiOp::PartRpc* rpc);
    void removeRequestAt(uint32_t index);
    void retryRequest(MultiOpObject* request);

    /// A special Status value indicating than an RPC is underway but
    /// we haven't yet seen the response.
    static const Status UNDERWAY = Status(STATUS_MAX_VALUE+1);

    /// The maximum RPC length we will ever issue to a master in bytes. Requests
    /// that exceed this value will be issued in multiple RPCs.
    static const uint32_t maxRequestSize = Transport::MAX_RPC_LEN -
                                        sizeof(WireFormat::MultiOp::Request);

    /// Overall client state information.
    RamCloud* ramcloud;

    /// The type of multi* operation that extends this super class; This
    /// is used to multiplex the MultiOp RPC
    WireFormat::MultiOp::OpType opType;

    /// Copy of constructor argument containing information about
    /// desired objects.
    MultiOpObject const * const * requests;

    /// Copy constructor argument giving size of \c requests.
    uint32_t numRequests;

    /// An array holding the constituent RPCs that we are managing.
#ifdef TESTING
    static const uint32_t MAX_RPCS = 2;
#else
    static const uint32_t MAX_RPCS = 10;
#endif
    Tub<PartRpc> rpcs[MAX_RPCS];

    /// Set by \c cancel.
    bool canceled;

    /// Manipulable array used to shuffle requests around for performance.
    /// Size should always be MAX_RPCs, but front (before startIndex)
    /// contains junk and rest contains unfinished rpcs.
    std::vector<MultiOpObject*> workQueue;

    /// Marks the start of unfinished requests in requestIndecies.
    uint32_t startIndex;

    /// Used for tests only. True = ignores buffer size checking in finishRpc.
    /// Needed since test responses don't put anything in the response buffer.
    bool test_ignoreBufferOverflow;

    // TOOD(syang0) These should be abstracted into sub class.
    virtual void appendRequest(MultiOpObject* request, Buffer* buf)=0;
    virtual bool readResponse(MultiOpObject* request, Buffer* response,
                                 uint32_t* respOffset)=0;

    DISALLOW_COPY_AND_ASSIGN(MultiOp);
};

} // end RAMCloud

#endif  // RAMCLOUD_MULTIOP_H
