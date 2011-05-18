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
 * use #getSession() and #serverRecv().
 *
 * The only instance of this class is #transportManager.
 */
class TransportManager {
  public:
    TransportManager();
    ~TransportManager();
    void initialize(const char* serviceLocator);
    Transport::SessionRef getSession(const char* serviceLocator);
    Transport::ServerRpc* serverRecv();
    ServiceLocatorList getListeningLocators();
    string getListeningLocatorsString();
    void registerMemory(void* base, size_t bytes);
    void dumpStats();

#if TESTING
    /**
     * Register a mock transport instance for unit testing.
     * The transport can be sent to using the service locator "mock:".
     * Must be paired with a call to #unregisterMock().
     * \param transport
     *      Probably a #MockTransport instance. Owned by the caller.
     */
    void registerMock(Transport* transport) {
        initialized = true;
        listening.push_back(transport);
        transports.push_back(transport);
        protocolTransportMap.insert({"mock", transport});
    }

    /**
     * For unit testing.
     * Must be paired with a call to #registerMock().
     */
    void unregisterMock() {
        protocolTransportMap.erase("mock");
        transports.pop_back();
        listening.pop_back();
        // Invalidate cache because mock transports are ephemeral and
        // come and go.
        sessionCache.clear();
    }

    struct MockRegistrar {
        explicit MockRegistrar(Transport& transport) {
            extern TransportManager transportManager;
            transportManager.registerMock(&transport);
        }
        ~MockRegistrar() {
            extern TransportManager transportManager;
            transportManager.unregisterMock();
        }
    };
#endif

  PRIVATE:
    /**
     * Sessions of the following type are used as wrappers in worker threads
     * on servers.  These are needed because "real" Session objects are owned
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
     * Whether #initialize() has been called.
     */
    bool initialized;

    /**
     * True means this is a server application, false means this is a client only.
     */
    bool isServer;

    /**
     * A set of factories to create all possible transports.
     */
    std::set<TransportFactory*> transportFactories;

    /**
     * The set of all Transport instances.
     */
    std::vector<Transport*> transports;

    /**
     * Transports on which to receive RPC requests. These are polled
     * round-robin in #serverRecv().
     */
    std::vector<Transport*> listening;

    /**
     * The index into #listening of the next Transport that should be polled.
     * This index may be out of bounds and should be checked before use.
     * It is used exclusively in #serverRecv().
     */
    uint32_t nextToListen;

    /**
     * A map from protocol string to Transport instances for #getSession().
     */
    std::multimap<string, Transport*> protocolTransportMap;

    /**
     * A map from service locator to SessionRef instances for #getSession().
     * This is used as a cache so that the same SessionRef is used if
     * #getSession() is called on an existing service locator string.
     */
    std::map<string, Transport::SessionRef> sessionCache;

    friend class TransportManagerTest;
    DISALLOW_COPY_AND_ASSIGN(TransportManager);
};

extern TransportManager transportManager;

} // end RAMCloud

#endif  // RAMCLOUD_TRANSPORTMANAGER_H
