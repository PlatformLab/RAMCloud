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

#include <string>
#include <boost/intrusive_ptr.hpp>

#include "Common.h"
#include "ServiceLocator.h"
#include "Buffer.h"

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
      protected:
        /**
         * Constructor for ClientRpc.
         */
        ClientRpc() {}

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
        virtual void wait() = 0;

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
        virtual bool isReady() = 0;

      private:
        DISALLOW_COPY_AND_ASSIGN(ClientRpc);
    };

    /**
     * An RPC request that has been received and is awaiting our response.
     * #serverRecv() will return one of these, and the caller of that method
     * must later call #sendReply() on it.
     */
    class ServerRpc {
      protected:
        /**
         * Constructor for ServerRpc.
         */
        ServerRpc() : recvPayload(), replyPayload() {}

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
         * The received RPC payload.
         */
        Buffer recvPayload;

        /**
         * The RPC payload to send as a response with #sendReply().
         */
        Buffer replyPayload;

      private:
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
        virtual ClientRpc* clientSend(Buffer* request, Buffer* response)
            __attribute__((warn_unused_result)) = 0;

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

        /// Used by boost::intrusive_ptr. Do not call explicitly.
        friend void intrusive_ptr_add_ref(Session* session) {
            ++session->refCount;
        }

        /// Used by boost::intrusive_ptr. Do not call explicitly.
        friend void intrusive_ptr_release(Session* session) {
            if (--session->refCount == 0)
                session->release();
        }

      protected:
        uint32_t refCount;
      private:
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
     * Get the next RPC request that has arrived.
     * \return
     *      The RPC object through which to send a reply, or NULL of no RPC
     *      requests were waiting. The caller must use either
     *      #Transport::ServerRpc::sendReply() to release the resources
     *      associated with this object (if it is not NULL).
     */
    virtual ServerRpc* serverRecv() __attribute__((warn_unused_result)) = 0;

    /**
     * Return a session that will communicate with the given service locator.
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
    virtual ServiceLocator getServiceLocator() = 0;

    /// Dump out performance and debugging statistics.
    virtual void dumpStats() {}

  private:
    DISALLOW_COPY_AND_ASSIGN(Transport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TRANSPORT_H
