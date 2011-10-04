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

#ifndef RAMCLOUD_TRANSPORTMANAGER_H
#define RAMCLOUD_TRANSPORTMANAGER_H

#include <boost/foreach.hpp>
#include <map>
#include <set>

#include "Common.h"
#include "MockTransport.h"
#include "MockTransportFactory.h"
#include "SpinLock.h"
#include "Transport.h"

namespace RAMCloud {

class TransportFactory;

/**
 * A TransportManager provides the single entry point to the transport
 * subsystem for higher layers.
 *
 * The only method clients should be interested in is #getSession() on the
 * #transportManager instance. They do not need to call #initialize().
 *
 * Servers should first use #transportManager's #initialize(). Then, they may
 * use #getSession().
 *
 * To get a pointer to the current TransportManager instance, use
 * Context::get().transportManager; see the Context class for more info.
 */
class TransportManager {
  public:
    TransportManager();
    ~TransportManager();
    void initialize(const char* serviceLocator);
    Transport::SessionRef getSession(const char* serviceLocator);
    string getListeningLocatorsString();
    void registerMemory(void* base, size_t bytes);
    void dumpStats();
    void dumpTransportFactories();

#if TESTING
    /**
     * Register a mock transport instance for unit testing.
     * The transport can be sent to using the service locator "mock:".
     * Must be paired with a call to #unregisterMock().
     * \param transport
     *      Transport instance, owned by the caller.  If NULL, a new
     *      MockTransport will be created on-demand.
     * \param protocol
     *      #GetSession will return #transport whenever this protocol
     *      is requested.
     */
    void registerMock(Transport* transport, const char* protocol = "mock") {
        transportFactories.push_back(
                new MockTransportFactory(transport, protocol));
        transports.push_back(NULL);
    }

    /**
     * For unit testing.
     * Must be paired with a call to #registerMock().
     */
    void unregisterMock() {
        delete transportFactories.back();
        transportFactories.pop_back();
        transports.pop_back();
        sessionCache.clear();
    }

    struct MockRegistrar {
        explicit MockRegistrar(Transport& transport) {
            Context::get().transportManager->registerMock(&transport);
        }
        ~MockRegistrar() {
            Context::get().transportManager->unregisterMock();
        }
    };
#endif

  PRIVATE:
    /**
     * Sessions of this type are used as wrappers in worker threads on
     * servers.  These are needed because "real" Session objects are owned
     * by transports (which run in the dispatch thread) and hence cannot be
     * accessed in worker threads without synchronization.  WorkerSession
     * objects forward the #clientSend method to the actual Session object
     * after synchronizing appropriately with the dispatch thread.
     */
    class WorkerSession : public Transport::Session {
      public:
        explicit WorkerSession(Transport::SessionRef wrapped);
        ~WorkerSession() {}
        Transport::ClientRpc* clientSend(Buffer* request, Buffer* reply)
            __attribute__((warn_unused_result));
        void release() {
            delete this;
        }
      PRIVATE:
        Transport::SessionRef wrapped; /// clientSend calls must be forwarded
                                       /// to this underlying object.
        DISALLOW_COPY_AND_ASSIGN(WorkerSession);
    };

    /**
     * True means this is a server application, false means this is a client only.
     */
    bool isServer;

    /**
     * Factories to create all possible transports.  The order in this vector
     * matches that in transports.
     */
    std::vector<TransportFactory*> transportFactories;

    /**
     * A transport instance corresponding to each factory. These instances are
     * created on demand (only if needed), so some entries may be NULL.  This
     * vector may be longer than #transportFactories if we end up creating
     * multiple transports from a single factory (e.g., to serve incoming RPCs
     * via multiple locators corresponding to the same transport). In this case
     * the first transport from a factory is in the slot corresponding to that
     * factory, and additional transports for that factory are appended at the
     * end.
     */
    std::vector<Transport*> transports;

    /**
     * Contains the value that will be returned by getListeningLocatorsString:
     * a string containing service locators for all of the ways we are prepared
     * to receive incoming RPCs.
     */
    std::string listeningLocators;

    /**
     * A map from service locator to SessionRef instances for #getSession().
     * This is used as a cache so that the same SessionRef is used if
     * #getSession() is called on an existing service locator string.
     */
    std::map<string, Transport::SessionRef> sessionCache;

    /**
     * The following variables record the parameters for all previous calls
     * to #registerMemory, so that we can pass them to any new transports
     * that are created after the calls occurred.
     */
    std::vector<void*> registeredBases;
    std::vector<size_t> registeredSizes;

    /**
     * Used for mutual exclusion in multi-threaded environments.
     */
    SpinLock mutex;

    DISALLOW_COPY_AND_ASSIGN(TransportManager);
};

} // end RAMCloud

#endif  // RAMCLOUD_TRANSPORTMANAGER_H
