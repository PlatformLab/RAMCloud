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

#include "CycleCounter.h"
#include "Metrics.h"
#include "TransportManager.h"
#include "TransportFactory.h"

#include "TcpTransport.h"
#include "FastTransport.h"
#include "UdpDriver.h"

#ifdef INFINIBAND
#include "InfRcTransport.h"
#include "InfUdDriver.h"
#endif

namespace RAMCloud {

static struct TcpTransportFactory : public TransportFactory {
    TcpTransportFactory()
        : TransportFactory("kernelTcp", "tcp") {}
    Transport* createTransport(const ServiceLocator* localServiceLocator) {
        return new TcpTransport(localServiceLocator);
    }
} tcpTransportFactory;

static struct FastUdpTransportFactory : public TransportFactory {
    FastUdpTransportFactory()
        : TransportFactory("fast+kernelUdp", "fast+udp") {}
    Transport* createTransport(const ServiceLocator* localServiceLocator) {
        return new FastTransport(new UdpDriver(localServiceLocator));
    }
} fastUdpTransportFactory;

#ifdef INFINIBAND
static struct FastInfUdTransportFactory : public TransportFactory {
    FastInfUdTransportFactory()
        : TransportFactory("fast+infinibandud", "fast+infud") {}
    Transport* createTransport(const ServiceLocator* localServiceLocator) {
        return new FastTransport(
            new InfUdDriver<>(localServiceLocator));
    }
} fastInfUdTransportFactory;

static struct InfRcTransportFactory : public TransportFactory {
    InfRcTransportFactory()
        : TransportFactory("infinibandrc", "infrc") {}
    Transport* createTransport(const ServiceLocator* localServiceLocator) {
        return new InfRcTransport<>(localServiceLocator);
    }
} infRcTransportFactory;
#endif

/**
 * The single instance of #TransportManager.
 * Its priority ensures that it is initialized after the TestLog.
 */
TransportManager  __attribute__((init_priority(400))) transportManager;

TransportManager::TransportManager()
    : initialized(false)
    , isServer(false)
    , transportFactories()
    , transports()
    , listeningLocators()
    , protocolTransportMap()
    , sessionCache()
{
    transportFactories.insert(&tcpTransportFactory);
    transportFactories.insert(&fastUdpTransportFactory);
#ifdef INFINIBAND
    transportFactories.insert(&fastInfUdTransportFactory);
    transportFactories.insert(&infRcTransportFactory);
#endif
}

TransportManager::~TransportManager()
{
    // Must clear the cache and destroy sessionRefs before the
    // transports are destroyed.

    // Can't safely execute the following code; see RAM-212 for details.
#if 0
    sessionCache.clear();
    foreach (auto transport, transports)
        delete transport;
#endif
}

/**
 * Initialize information about available transports that is used by both
 * servers and clients.
 *
 * \param localServiceLocator
 *      If the application is a server, this specifies one or more locators
 *      that clients can use to send requests to this server.  An empty
 *      string indicates that this application is a client, so it will not be
 *      receiving any requests.
 */
void
TransportManager::initialize(const char* localServiceLocator)
{
    assert(!initialized);

    auto locators = ServiceLocator::parseServiceLocators(localServiceLocator);
    if (locators.size() != 0) {
        isServer = true;
    }

    foreach (auto factory, transportFactories) {
        Transport* transport = NULL;
        foreach (auto& locator, locators) {
            if (factory->supports(locator.getProtocol().c_str())) {
                // The transport supports a protocol that we can receive
                // requests on. Since it is expected that this transport
                // works, we do not catch exceptions if it is unavailable.
                transport = factory->createTransport(&locator);
                if (listeningLocators.size() != 0)
                    listeningLocators += ";";
                listeningLocators +=
                        transport->getServiceLocator().getOriginalString();
                goto insert_protocol_mappings;
            }
        }

        // The transport cannot be used to receive requests (it didn't
        // support any of the desired locators), but it can still potentially
        // be used for issuing requests to other servers.
        try {
            transport = factory->createTransport(NULL);
        } catch (TransportException &e) {
            // Don't get upset if the transport isn't available; it could
            // be something simple such as the physical device (NIC) does
            // not exist.
        }

 insert_protocol_mappings:
        if (transport != NULL) {
            transports.push_back(transport);
            foreach (auto protocol, factory->getProtocols()) {
                protocolTransportMap.insert({protocol, transport});
            }
        }
    }
    initialized = true;
}

/**
 * Get a session on which to send RPC requests to a service.
 *
 * \param serviceLocator
 *      Desired service.
 *
 * \throw NoSuchKeyException
 *      A transport supporting one of the protocols claims a service locator
 *      option is missing.
 * \throw BadValueException
 *      A transport supporting one of the protocols claims a service locator
 *      option is malformed.
 * \throw TransportException
 *      No transport was found for this service locator.
 */
Transport::SessionRef
TransportManager::getSession(const char* serviceLocator)
{
    if (!initialized)
        initialize("");

    auto it = sessionCache.find(serviceLocator);
    if (it != sessionCache.end())
        return it->second;

    CycleCounter<Metric> counter;

    // Session was not found in the cache, a new one will be created
    auto locators = ServiceLocator::parseServiceLocators(serviceLocator);
    // The first protocol specified in the locator that works is chosen
    foreach (auto& locator, locators) {
        foreach (auto& protocolTransport,
                 protocolTransportMap.equal_range(locator.getProtocol())) {
            auto transport = protocolTransport.second;
            try {
                auto session = transport->getSession(locator);
                if (isServer) {
                    session = new WorkerSession(session);
                }

                // Only first protocol is used, but the cache is based
                // on the complete initial service locator string.
                // No caching should occur if an exception is thrown.
                sessionCache.insert({serviceLocator, session});
                session->setServiceLocator(serviceLocator);

                uint64_t elapsed = counter.stop();
                metrics->transport.sessionOpenTicks += elapsed;
                metrics->transport.sessionOpenSquaredTicks +=
                    elapsed * elapsed;
                ++metrics->transport.sessionOpenCount;
                return session;
            } catch (TransportException& e) {
                // TODO(ongaro): Transport::getName() would be nice here.
                LOG(DEBUG, "Transport %p refused to open session for %s",
                    transport, locator.getOriginalString().c_str());
            }
        }
    }
    throw TransportException(HERE,
        format("No transport found for this service locator: %s",
               serviceLocator));
}

/**
 * Return a ServiceLocator string corresponding to the listening
 * ServiceLocators.
 * \return
 *      A semicolon-delimited, ServiceLocator string containing all
 *      ServiceLocators' strings that are listening to RPCs.
 */
string
TransportManager::getListeningLocatorsString()
{
    return listeningLocators;
}

/**
 * See #Transport::registerMemory.
 */
void
TransportManager::registerMemory(void* base, size_t bytes)
{
    foreach (auto transport, transports)
            transport->registerMemory(base, bytes);
}

/**
 * dumpStats() on all registered transports.
 */
void
TransportManager::dumpStats()
{
    foreach (auto transport, transports)
        transport->dumpStats();
}

/**
 * Construct a WorkerSession.
 *
 * \param wrapped
 *      Another Session object, to which #clientSend requests will be
 *      forwarded.
 */
TransportManager::WorkerSession::WorkerSession(Transport::SessionRef wrapped)
    : wrapped(wrapped)
{
    TEST_LOG("created");
}

// See Transport::Session::clientSend for documentation.
Transport::ClientRpc*
TransportManager::WorkerSession::clientSend(Buffer* request, Buffer* reply)
{
    // Must make sure that the dispatch thread isn't running when we
    // invoked the real clientSend.
    Dispatch::Lock lock;
    return wrapped->clientSend(request, reply);
}

} // namespace RAMCloud
