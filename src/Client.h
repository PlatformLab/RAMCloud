/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_CLIENT_H
#define RAMCLOUD_CLIENT_H

#include <boost/utility/result_of.hpp>

#include "Buffer.h"
#include "ClientException.h"
#include "Common.h"
#include "Dispatch.h"
#include "Rpc.h"
#include "Status.h"
#include "Tub.h"
#include "Transport.h"

/**
 * Defines a synchronous RPC method in terms of an asynchronous RPC class.
 * Defines a method named methodName that constructs an AsyncClass with a
 * reference to its 'this' instance and the arguments its given, then calls
 * the AsyncClass's operator() method.
 */
#define DEF_SYNC_RPC_METHOD(methodName, AsyncClass) \
    template<typename... Args> \
    boost::result_of<AsyncClass()>::type \
    methodName(Args&&... args) { \
        return AsyncClass(*this, static_cast<Args&&>(args)...)(); \
    }

namespace RAMCloud {

/**
 * A Concept used in #parallelRun.
 */
struct AsynchronousTaskConcept {
    /**
     * Return true if calling #wait() would make progress on this task without
     * blocking. This should return false if #isDone() returns true.
     */
    bool isReady();
    /**
     * Return true if there is no more work to be done on this task.
     */
    bool isDone();
    /**
     * Start a new chunk of work, to be later completed in #wait().
     */
    void send();
    /**
     * Wait for a chunk of work previously started in #send() to complete.
     * Note that, after calling #wait(), #isDone() may still return false. If
     * this is the case, #wait() will be called again at some later time.
     */
    void wait();
};

/**
 * Execute asynchronous tasks in parallel until they complete.
 * This is useful for broadcasting RPCs, etc.  This method should be invoked
 * only in worker threads (not in the dispatch thread).
 * \param tasks
 *      An array of \a numTasks entries in length of objects having the
 *      interface documented in #AsynchronousTaskConcept.
 * \param numTasks
 *      The number of entries in the \a tasks array.
 * \param maxOutstanding
 *      The maximum number of task to run in parallel with each other.
 */
template<typename T>
void
parallelRun(Tub<T>* tasks, uint32_t numTasks, uint32_t maxOutstanding)
{
    assert(maxOutstanding > 0 || numTasks == 0);

    uint32_t firstNotIssued = 0;
    uint32_t firstNotDone = 0;

    // Start off first round of tasks
    for (uint32_t i = 0; i < numTasks; ++i) {
        auto& task = tasks[i];
        task->send();
        ++firstNotIssued;
        if (i + 1 == maxOutstanding)
            break;
    }

    // As tasks complete, kick off new ones
    while (firstNotDone < numTasks) {
        for (uint32_t i = firstNotDone; i < firstNotIssued; ++i) {
            auto& task = tasks[i];
            if (task->isDone()) { // completed already
                if (firstNotDone == i)
                    ++firstNotDone;
                continue;
            }
            if (!task->isReady()) // not started or reply hasn't arrived
                continue;
            task->wait();
            if (!task->isDone())
                continue;
            if (firstNotDone == i)
                ++firstNotDone;
            if (firstNotIssued < numTasks) {
                tasks[firstNotIssued]->send();
                ++firstNotIssued;
            }
        }
    }
}

/**
 * A base class for RPC clients.
 */
class Client {
  public:
    Client() : status(STATUS_OK) {}
    virtual ~Client() {}

    /**
     * Completion status from the most recent RPC completed for this client.
     */
    Status status;

    // begin RPC proxy method helpers

    /**
     * Helper for RPC proxy methods that prepares an RPC request header.
     * You should call this before every use of #sendRecv().
     */
    template <typename Rpc>
    static typename Rpc::Request&
    allocHeader(Buffer& requestBuffer)
    {
        assert(requestBuffer.getTotalLength() == 0);
        typename Rpc::Request& requestHeader(
            *new(&requestBuffer, APPEND) typename Rpc::Request);
        memset(&requestHeader, 0, sizeof(requestHeader));
        requestHeader.common.opcode = Rpc::opcode;
        requestHeader.common.service = Rpc::service;
        return requestHeader;
    }

  PROTECTED:

    /// An opaque handle returned by #send() and passed into #recv().
    struct AsyncState {
        AsyncState() : rpc(NULL), responseBuffer(NULL) {}
        bool isReady() { return rpc->isReady(); }
        void cancel() { rpc->cancel(); }
      private:
        Transport::ClientRpc* rpc;
        Buffer* responseBuffer;
        friend class Client;
    };

    /// First half of sendRecv. Call #recv() after this.
    template <typename Rpc>
    AsyncState
    send(Transport::SessionRef& session,
         Buffer& requestBuffer, Buffer& responseBuffer)
    {
        assert(responseBuffer.getTotalLength() == 0);
        AsyncState state;
        state.responseBuffer = &responseBuffer;
        state.rpc = session->clientSend(&requestBuffer, &responseBuffer);
        return state;
    }

    /// Second half of sendRecv. Call #send() before this.
    template <typename Rpc>
    const typename Rpc::Response&
    recv(AsyncState& state)
    {
        state.rpc->wait();
        const typename Rpc::Response* responseHeader =
            state.responseBuffer->getStart<typename Rpc::Response>();
        if (responseHeader == NULL)
            throwShortResponseError(*state.responseBuffer);
        status = responseHeader->common.status;
        return *responseHeader;
    }

    /**
     * Helper for RPC proxy methods that sends a request and receive a
     * response. You should call #allocHeader() before this and #checkStatus()
     * after this.
     */
    template <typename Rpc>
    const typename Rpc::Response&
    sendRecv(Transport::SessionRef& session,
             Buffer& requestBuffer, Buffer& responseBuffer)
    {
        AsyncState state(send<Rpc>(session, requestBuffer, responseBuffer));
        return recv<Rpc>(state);
    }

    /**
     * Helper for RPC proxy methods that throws a ClientException if the status
     * is not OK. You should call this after every use of #sendRecv().
     */
    void checkStatus(const CodeLocation& where) const
    {
        RAMCLOUD_TEST_LOG("status: %d", STATUS_OK);
        if (status != STATUS_OK)
            ClientException::throwException(where, status);
    }

    // end RPC proxy method helpers

    void throwShortResponseError(Buffer& response)
        __attribute__((noreturn));

  private:
    friend class ClientTest;
    DISALLOW_COPY_AND_ASSIGN(Client);
};

} // end RAMCloud

#endif  // RAMCLOUD_CLIENT_H
