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
#include "ShortMacros.h"
#include "Metrics.h"
#include "TransportManager.h"
#include "TransportFactory.h"

#include "TcpTransport.h"
#include "FastTransport.h"
#include "UnreliableTransport.h"
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

static struct UnreliableUdpTransportFactory : public TransportFactory {
    UnreliableUdpTransportFactory()
        : TransportFactory("unreliable+kernelUdp", "unreliable+udp") {}
    Transport* createTransport(const ServiceLocator* localServiceLocator) {
        return new UnreliableTransport(new UdpDriver(localServiceLocator));
    }
} unreliableUdpTransportFactory;

#ifdef INFINIBAND
static struct FastInfUdTransportFactory : public TransportFactory {
    FastInfUdTransportFactory()
        : TransportFactory("fast+infinibandud", "fast+infud") {}
    Transport* createTransport(const ServiceLocator* localServiceLocator) {
        return new FastTransport(
            new InfUdDriver<>(localServiceLocator, false));
    }
} fastInfUdTransportFactory;

static struct UnreliableInfUdTransportFactory : public TransportFactory {
    UnreliableInfUdTransportFactory()
        : TransportFactory("unreliable+infinibandud", "unreliable+infud") {}
    Transport* createTransport(const ServiceLocator* localServiceLocator) {
        return new UnreliableTransport(
            new InfUdDriver<>(localServiceLocator, false));
    }
} unreliableInfUdTransportFactory;

static struct FastInfEthTransportFactory : public TransportFactory {
    FastInfEthTransportFactory()
        : TransportFactory("fast+infinibandethernet", "fast+infeth") {}
    Transport* createTransport(const ServiceLocator* localServiceLocator) {
        return new FastTransport(
            new InfUdDriver<>(localServiceLocator, true));
    }
} fastInfEthTransportFactory;

static struct UnreliableInfEthTransportFactory : public TransportFactory {
    UnreliableInfEthTransportFactory()
        : TransportFactory("unreliable+infinibandethernet",
                           "unreliable+infeth") {}
    Transport* createTransport(const ServiceLocator* localServiceLocator) {
        return new UnreliableTransport(
            new InfUdDriver<>(localServiceLocator, true));
    }
} unreliableInfEthTransportFactory;

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
    : isServer(false)
    , transportFactories()
    , transports()
    , listeningLocators()
    , sessionCache()
    , registeredBases()
    , registeredSizes()
    , mutex()
{
    transportFactories.push_back(&tcpTransportFactory);
    transportFactories.push_back(&fastUdpTransportFactory);
    transportFactories.push_back(&unreliableUdpTransportFactory);
#ifdef INFINIBAND
    transportFactories.push_back(&fastInfUdTransportFactory);
    transportFactories.push_back(&unreliableInfUdTransportFactory);
    transportFactories.push_back(&fastInfEthTransportFactory);
    transportFactories.push_back(&unreliableInfEthTransportFactory);
    transportFactories.push_back(&infRcTransportFactory);
#endif
    transports.resize(transportFactories.size(), NULL);
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
 * This method is invoked only on servers; it creates transport(s) that will be
 * used to receive RPC requests.  These transports can also be used for outgoing
 * RPC requests, and additional transports for outgoing requests will be
 * created on-demand by #getSession.
 *
 * \param localServiceLocator
 *      Specifies one or more locators that clients can use to send requests to
 *      this server.
 */
void
TransportManager::initialize(const char* localServiceLocator)
{
    isServer = true;
    Dispatch::init(isServer);
    Dispatch::Lock lock;
    std::vector<ServiceLocator> locators =
            ServiceLocator::parseServiceLocators(localServiceLocator);

    foreach (auto& locator, locators) {
        for (uint32_t i = 0; i < transportFactories.size(); i++) {
            TransportFactory* factory = transportFactories[i];
            if (factory->supports(locator.getProtocol().c_str())) {
                // The transport supports a protocol that we can receive
                // requests on.
                Transport *transport = factory->createTransport(&locator);
                for (uint32_t j = 0; j < registeredBases.size(); j++) {
                    transport->registerMemory(registeredBases[j],
                                              registeredSizes[j]);
                }
                if (transports[i] == NULL) {
                    transports[i] = transport;
                } else {
                    // If we get here, it means we've already created at
                    // least one transport for this factory.
                    transports.push_back(transport);
                }
                if (listeningLocators.size() != 0)
                    listeningLocators += ";";
                // Ask the transport for its service locator. This might be
                // more specific than "locator", as the transport may have
                // added information.
                listeningLocators += transport->getServiceLocator();
                break;
            }
        }
    }
}

/**
 * Get a session on which to send RPC requests to a service.  This method
 * keeps a cache of sessions and will reuse existing sessions whenever
 * possible. If necessary, the method also instantiates new transports
 * based on the service locator.
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
    // If we're running on a server (i.e., multithreaded) must exclude
    // other threads.
    Tub<boost::lock_guard<SpinLock>> lock;
    if (isServer) {
        lock.construct(mutex);
    }

    // First check to see if we have already opened a session for the
    // locator; this should almost always be true.
    auto it = sessionCache.find(serviceLocator);
    if (it != sessionCache.end())
        return it->second;

    CycleCounter<Metric> counter;

    // Session was not found in the cache, so create a new one and add
    // it to the cache.

    // Iterate over all of the sub-locators, looking for a transport that
    // can handle its protocol.
    bool transportSupported = false;
    auto locators = ServiceLocator::parseServiceLocators(serviceLocator);
    foreach (auto& locator, locators) {
        for (uint32_t i = 0; i < transportFactories.size(); i++) {
            TransportFactory* factory = transportFactories[i];
            if (!factory->supports(locator.getProtocol().c_str())) {
                continue;
            }

            if (transports[i] == NULL) {
                // Try to create a new transport via this factory.
                // It's OK if that doesn't work (e.g. the particular
                // transport may depend on physical devices that don't
                // exist on this machine).
                try {
                    Dispatch::Lock lock;
                    transports[i] = factory->createTransport(NULL);
                    for (uint32_t j = 0; j < registeredBases.size(); j++) {
                        transports[i]->registerMemory(registeredBases[j],
                                                      registeredSizes[j]);
                    }
                } catch (TransportException &e) {
                    continue;
                }
            }

            transportSupported = true;
            try {
                auto session = transports[i]->getSession(locator);
                if (isServer) {
                    session = new WorkerSession(session);
                }

                // The cache is based on the complete initial service locator
                // string.
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
                    transports[i], locator.getOriginalString().c_str());
            }
        }
    }

    string errorMsg;
    if (transportSupported) {
        errorMsg = format("Could not obtain transport session for this "
            "service locator: %s", serviceLocator);
    } else {
        errorMsg = format("No supported transport found for this "
            "service locator: %s", serviceLocator);
    }

    throw TransportException(HERE, errorMsg);
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
    Dispatch::Lock lock;
    foreach (auto transport, transports) {
        if (transport != NULL)
            transport->registerMemory(base, bytes);
    }
    registeredBases.push_back(base);
    registeredSizes.push_back(bytes);
}

/**
 * dumpStats() on all existing transports.
 */
void
TransportManager::dumpStats()
{
    Dispatch::Lock lock;
    foreach (auto transport, transports) {
        if (transport != NULL)
            transport->dumpStats();
    }
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
