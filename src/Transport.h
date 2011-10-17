/* Copyright (c) 2010 Stanford University
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

#ifndef RAMCLOUD_TRANSPORT_H
#define RAMCLOUD_TRANSPORT_H

#include <cstdatomic>
#include <string>
#include <boost/intrusive_ptr.hpp>

#include "Common.h"
#include "BoostIntrusive.h"
#include "Buffer.h"
#include "ServiceLocator.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * An exception that is thrown when the Transport class encounters a problem.
 */
struct TransportException : public Exception {
    explicit TransportException(const CodeLocation& where)
        : Exception(where) {}
    TransportException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    TransportException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    TransportException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

/**
 * An interface for reliable communication across the network.
 *
 * Implementations all send and receive RPC messages reliably over the network.
 * These messages are variable-length and can be larger than a single network
 * frame in size.
 *
 * Implementations differ in the protocol stacks they use and their performance
 * characteristics.
 */
class Transport {

  public:
      /// Transports should cut off longer RPCs to prevent runaways.
      static const uint32_t MAX_RPC_LEN = (1 << 24);

    /**
     * A RPC call that has been sent and is pending a response from a server.
     * #clientSend() will return one of these, and the caller of that method
     * must later call #wait() on it.
     */
    class ClientRpc {
      PROTECTED:
        /**
         * Constructor for ClientRpc.
         *
         * \param request
         *      Holds the outgoing (request) message for the RPC.
         * \param response
         *      Used to hold the response message, when it arrives.
         */
        ClientRpc(Buffer* request, Buffer* response)
            : request(request), response(response), finished(0),
              errorMessage() {}

      public:

        /**
         * Destructor for ClientRpc.
         */
        virtual ~ClientRpc() {}

        /**
         * Wait for the RPC response to arrive (if it hasn't already) and
         * throw an exception if there were any problems. Once this method
         * has returned the caller can access the response message using
         * the #Buffer that was passed to #clientSend().
         *
         * \throw TransportException
         *      Something went wrong at the transport level (e.g. the
         *      server crashed).
         */
        void wait();

        /**
         * Indicate whether a response or error has been received for
         * the RPC.  Used for asynchronous processing of RPCs.
         *
         * \return
         *      True means that #wait will not block when it is invoked.
         *      Note: even if this method returns true, #wait must still
         *      be invoked so it can throw exceptions (this method will
         *      not throw any exceptions).
         */
        bool isReady() {
            return (finished.load() != 0);
        }

        void cancel(const string& message);
        void cancel(const char* message = "");

        // Request and response messages.
        Buffer* request;
        Buffer* response;

      PROTECTED:
        /**
         * This method provides a hook for individual transports to unwind RPCs
         * in progress as part of cancellation.  It is invoked by #cancel with
         * the Dispatch lock held  (and only if the RPC isn't already finished);
         * once it returns #cancel will mark the RPC finished.
         */
        virtual void cancelCleanup() {}
        void markFinished(const string& errorMessage);
        void markFinished(const char* errorMessage = NULL);

        /**
         * Non-zero means that the RPC has completed (either with or without an
         * error), so the next call to #wait should return immediately.
         */
        std::atomic_int finished;

        /**
         * If an error occurred in the RPC then this holds the error message;
         * if the RPC completed normally than this has no value.
         */
        Tub<string> errorMessage;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ClientRpc);
    };

    /**
     * An RPC request that has been received and is either being serviced or
     * waiting for service.
     */
    class ServerRpc {
      PROTECTED:
        /**
         * Constructor for ServerRpc.
         */
        ServerRpc()
            : requestPayload(),
              replyPayload(),
              epoch(INVALID_EPOCH),
              outstandingRpcListHook() {}

        /**
         * Default epoch value on construction. Used to ensure that the proper
         * value is set before the RPC is passed on to the serviceManager.
         */
        static const uint64_t INVALID_EPOCH = -1;

      public:
        /**
         * Destructor for ServerRpc.
         */
        virtual ~ServerRpc() {}

        /**
         * Respond to the RPC with the contents of #replyPayload.
         *
         * You should discard all pointers to this #ServerRpc object after this
         * call.
         *
         * \throw TransportException
         *      If the client has crashed.
         */
        virtual void sendReply() = 0;

        /**
         * Returns false if the epoch was not set, else true. Used to assert
         * that no RPCs are pushed through the ServiceManager without an epoch.
         */
        bool
        epochIsSet()
        {
            return epoch != INVALID_EPOCH;
        }

        /**
         * The incoming RPC payload, which contains a request.
         */
        Buffer requestPayload;

        /**
         * The RPC payload to send as a response with #sendReply().
         */
        Buffer replyPayload;

        /**
         * The epoch of this RPC upon reception. ServerRpcPool will set this
         * value automatically, tagging the incoming RPC before it is passed to
         * the handling service's dispatch method. This number can be used later
         * to determine if any RPCs less than or equal to a certain epoch are
         * still outstanding in the system.
         */
        uint64_t epoch;

        /**
         * Hook for the list of active server RPCs that the ServerRpcPool class
         * maintains. RPCs are added when ServerRpc-derived classes are
         * constructed by ServerRpcPool and removed when they're destroyed.
         */
        IntrusiveListHook outstandingRpcListHook;

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ServerRpc);
    };

    /**
     * A handle to send RPCs to a particular service.
     */
    class Session {
      public:
        Session()
            : refCount(0) , serviceLocator() {}
        virtual ~Session() {
            assert(refCount == 0);
        }

        /**
         * This method is invoked via the boost intrusive_ptr mechanism when
         * all copies of the SessionRef for this session have been deleted; it
         * should reclaim the storage for the session.  This method is invoked
         * (rather than just deleting the object) to enable transport-specific
         * memory allocation for sessions.  In most cases the method should just
         * "delete this".
         */
        virtual void release() = 0;

        /**
         * Send an RPC request.
         * \param[in] request
         *      The RPC request payload to send. The caller must not modify or
         *      even access \a request until the corresponding call to
         *      #Transport::ClientRpc::wait() returns. The Transport may
         *      add new chunks to \a request but will not modify its existing
         *      chunks.
         * \param[out] response
         *      An empty Buffer that will be filled in with the received
         *      RPC response. The caller must not access \a response until the
         *      corresponding call to #Transport::ClientRpc::wait()
         *      returns.
         * \return
         *      The RPC object through which to receive the reply. The caller
         *      must eventually call #Transport::ClientRpc::wait() on this
         *      object to complete the RPC and deliver any exceptions that
         *      might have occurred. This object is allocated in \a response
         *      and will automatically be deallocated when \a response is
         *      deleted or reset.
         * \throw TransportException
         *      If any errors occur while initiating the request.
         */
        virtual ClientRpc* clientSend(Buffer* request, Buffer* response) = 0;

        /**
         * \return
         *      Return a reference to the service locator this Session is to.
         */
        const string& getServiceLocator() {
            return serviceLocator;
        }

        /**
         * \param locator
         *      The service locator this Session is connected to.
         */
        void setServiceLocator(const string& locator) {
            serviceLocator = locator;
        }

        /**
         * Shut down this session: abort any RPCs in progress and reject
         * any future calls to \c clientSend.
         * \param message
         *      Provides information about why the Session is being aborted.
         */
        virtual void abort(const string& message) = 0;

        /// Used by boost::intrusive_ptr. Do not call explicitly.
        friend void intrusive_ptr_add_ref(Session* session) {
            ++session->refCount;
        }

        /// Used by boost::intrusive_ptr. Do not call explicitly.
        friend void intrusive_ptr_release(Session* session) {
            if (--session->refCount == 0)
                session->release();
        }

      PROTECTED:
        uint32_t refCount;
      PRIVATE:
        string serviceLocator;

        DISALLOW_COPY_AND_ASSIGN(Session);
    };

    /**
     * A reference to a #Session object on which to send client RPC requests.
     * Usage is automatically tracked by boost::intrusive_ptr, so this can
     * be copied freely.  When the last copy is deleted the transport is
     * invoked to reclaim the session storage.
     */
    typedef boost::intrusive_ptr<Session> SessionRef;

    /**
     * Constructor for Transport.
     */
    Transport() {}

    /**
     * Destructor for Transport.
     */
    virtual ~Transport() {}

    /**
     * Return a session that will communicate with the given service locator.
     * This function may be called from worker threads and should contain any
     * necessary synchronization.
     * \throw NoSuchKeyException
     *      Service locator option missing.
     * \throw BadValueException
     *      Service locator option malformed.
     * \throw TransportException
     *      The transport can't open a session for \a serviceLocator.
     */
    virtual SessionRef getSession(const ServiceLocator& serviceLocator) = 0;

    /**
     * Return the ServiceLocator for this Transport. If the transport
     * was not provided static parameters (e.g. fixed TCP or UDP port),
     * this function will return a SerivceLocator with those dynamically
     * allocated attributes.
     *
     * Enlisting the dynamic ServiceLocator with the Coordinator permits
     * other hosts to contact dynamically addressed services.
     */
    virtual string getServiceLocator() = 0;

    /**
     * Register a permanently mapped region of memory. This is a hint to
     * the transport that identifies regions of memory that can be used
     * as zero-copy source buffer for transmission.
     * The Dispatch lock must be held by the caller for the duration of this
     * function.
     * \param[in] base
     *      The base address of the region.
     * \param[in] bytes
     *      The length of the region in bytes.
     * \bug A real solution requires some means of locking the region (or
     *      a subset thereof) for updates so long as a Transport is using it.
     */
    virtual void registerMemory(void* base, size_t bytes) {};

    /// Dump out performance and debugging statistics.
    virtual void dumpStats() {}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(Transport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TRANSPORT_H
