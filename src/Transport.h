/* Copyright (c) 2010-2015 Stanford University
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

#include "Atomic.h"
#include "BoostIntrusive.h"
#include "Buffer.h"
#include "CodeLocation.h"
#include "Exception.h"

namespace RAMCloud {
class ServiceLocator;

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
    class RpcNotifier;

      /// Maximum allowable size for an RPC request or response message: must
      /// be large enough to hold an 8MB segment plus header information.
      static const uint32_t MAX_RPC_LEN = ((1 << 23) + 200);

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
            : requestPayload()
            , replyPayload()
            , epoch(0)
            , activities(~0)
            , outstandingRpcListHook()
        {}

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
         * Return a ServiceLocator string describing the client that initiated
         * this RPC. Since it's a client this string can't be used to address
         * anything, but it can be useful for debugging purposes.
         *
         * Note that for performance reasons Transports may defer constructing
         * the string until this method is called (since it is expected to be
         * used only off of the fast path).
         */
        virtual string getClientServiceLocator() = 0;

        /**
         * Returns false if the epoch was not set, else true. Used to assert
         * that no RPCs are pushed through the WorkerManager without an epoch.
         */
        bool
        epochIsSet()
        {
            return epoch != 0;
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
         * A bit mask indicating what sorts of actions are being performed
         * during this RPC (default: ~0, which means all activities).
         * Individual RPCs can replace the default with a more selective
         * value so that the RPCs will be ignored in some cases when
         * scanning epochs. 0 means the RPC isn't doing anything that
         * matters to anyone.
         */
        int activities;

        /**
         * Bit values for activities above.
         * READ_ACTIVITY:             RPC is reading log information
         * APPEND_ACTIVITY:           RPC may add new entries to the log
         */
        static const int READ_ACTIVITY = 1;
        static const int APPEND_ACTIVITY = 2;

        /**
         * Hook for the list of active server RPCs that the ServerRpcPool class
         * maintains. RPCs are added when ServerRpc-derived classes are
         * constructed by ServerRpcPool and removed when they're destroyed.
         */
        IntrusiveListHook outstandingRpcListHook;
      protected:

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
         * memory allocation for sessions.  The default is to delete the
         * Session object.
         */
        virtual void release() {
            delete this;
        }

        /**
         * Initiate the transmission of an RPC request to the server.
         * \param request
         *      The RPC request payload to send. The caller must not modify or
         *      even access \a request until notifier's completed or failed
         *      method has been invoked.
         * \param[out] response
         *      A Buffer that will be filled in with the RPC response. The
         *      transport will clear any existing contents.  The caller must
         *      not access \a response until \a notifier's \c completed or
         *      \c failed method has been invoked.
         * \param notifier
         *      This object will be invoked when the RPC has completed or
         *      failed. It also serves as a unique identifier for the RPC,
         *      which can be passed to cancelRequest.
         */
        virtual void sendRequest(Buffer* request, Buffer* response,
                RpcNotifier* notifier) {}

        /**
         * Cancel an RPC request that was sent previously.
         * \param notifier
         *      Notifier object associated with this request (was passed
         *      to #sendRequest when the RPC was initiated).
         */
        virtual void cancelRequest(RpcNotifier* notifier) {}

        /**
         * Returns a human-readable string containing useful information
         * about any active RPC(s) on this session.
         */
        virtual string getRpcInfo() {
            return format("unknown RPC(s) on %s", serviceLocator.c_str());
        }

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
         * any future calls to \c sendRequest. The caller is responsible
         * for logging the reason for the abort.
         */
        virtual void abort() {}

        /**
         * This method is invoked by boost::intrusive_ptr as part of the
         * implementation of SessionRef; do not call explicitly.
         *
         * \param session
         *      WorkerSession for which a new WorkerSessionRef  is being
         *      created.
         */
        friend void intrusive_ptr_add_ref(Session* session) {
            session->refCount.inc();
        }

        friend void intrusive_ptr_release(Session* session);


      PROTECTED:
        Atomic<int> refCount;          /// Count of SessionRefs that exist
                                       /// for this Session.
      PRIVATE:
        string serviceLocator;

        // The following variable is used to simulate simultaneous calls to
        // intrusive_ptr_release in order to test its conflict handling.
        static bool testingSimulateConflict;

        DISALLOW_COPY_AND_ASSIGN(Session);
    };

    /**
     * A reference to a #Session object on which to send client RPC requests.
     * Usage is automatically tracked by boost::intrusive_ptr, so this can
     * be copied freely.  When the last copy is deleted the transport is
     * invoked to reclaim the session storage.
     *
     * Note: the reference count management in this class is thread-safe
     * (but normal Transport::Session objects are not: they should be
     * manipulated only of the dispatch thread).
     */
    typedef boost::intrusive_ptr<Session> SessionRef;

    /**
     * An RpcNotifier is an object that transports use to notify higher-level
     * software when an RPC has completed.  The RpcNotifier also serves as a
     * unique identifier for the RPC, which can be used to cancel it.
     *
     */
    class RpcNotifier {
      public:
        explicit RpcNotifier() {}
        virtual ~RpcNotifier() {}
        virtual void completed();
        virtual void failed();

        DISALLOW_COPY_AND_ASSIGN(RpcNotifier);
    };

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
     * This function is normally only invoked by TransportManager; clients
     * call TransportManager::getSession.
     *
     * \param serviceLocator
     *      Identifies the server this session will communicate with.
     * \param timeoutMs
     *      If the server becomes nonresponsive for this many milliseconds
     *      the session will be aborted.  0 means the session will pick a
     *      suitable default value.
     * \throw NoSuchKeyException
     *      Service locator option missing.
     * \throw BadValueException
     *      Service locator option malformed.
     * \throw TransportException
     *      The transport can't open a session for \a serviceLocator.
     */
    virtual SessionRef getSession(const ServiceLocator* serviceLocator,
            uint32_t timeoutMs = 0) = 0;

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

    /// Default timeout for transports (individual transports can choose to
    /// use their own default instead of this).  This is the total time
    /// after which a session will be aborted if there has been no sign of
    /// life from the server. Transports may use shorter internal timeouts
    /// to trigger retransmissions.  The current value for this was chosen
    /// somewhat subjectively in 11/2011, based on the presence of time gaps
    /// in the poller of as much as 250ms.
    static const uint32_t DEFAULT_TIMEOUT_MS = 500;

    /**
     * One ServerPort instance is created for a listen port
     * of the transport on the server.
     * It keeps track the port liveness with watchdog timer,
     *
     * When associated client port is closed or the client crashes,
     * the liveness watchdog cleans up the associated server port.
     *
     * Each serverPort instance is freed with 'delete self' at
     * the watchdog timeout.
     * So, severPort has to be dynamically instanciated to avoid
     * 'double free' error.
     *
     **/
    class ServerPort {
      public:
        explicit ServerPort() {}
        virtual ~ServerPort() {}
        /**
         * Close this server port: shutdown and reject any future
         * any future request. The caller is responsible
         * for logging the reason for the close.
         */
        virtual void close() {}
        virtual const string getPortName() const
        {
            return "Error: No transport assigned to this port.";
        }
      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(ServerPort);
    };

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(Transport);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_TRANSPORT_H
