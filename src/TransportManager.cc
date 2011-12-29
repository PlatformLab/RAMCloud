/* Copyright (c) 2010-2011 Stanford University
 * Copyright (c) 2011 Facebook
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
#include "MembershipClient.h"
#include "ShortMacros.h"
#include "RawMetrics.h"
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

TransportManager::TransportManager()
    : isServer(false)
    , transportFactories()
    , transports()
    , listeningLocators()
    , sessionCache()
    , registeredBases()
    , registeredSizes()
    , mutex()
    , timeoutMs(0)
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
    sessionCache.clear();
    foreach (auto transport, transports)
        delete transport;
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
    Dispatch::Lock lock;
    std::vector<ServiceLocator> locators =
            ServiceLocator::parseServiceLocators(localServiceLocator);

    if (locators.empty()) {
        throw Exception(HERE,
            "Servers must listen on at least one service locator, but "
            "none was provided");
    }

    uint32_t numListeningTransports = 0;
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
                // Ask the transport for its service locator. This might be
                // more specific than "locator", as the transport may have
                // added information.
                string listeningLocator = transport->getServiceLocator();
                if (listeningLocator.empty()) {
                    throw Exception(HERE,
                        format("Listening transport has empty locator. "
                               "Was initialized with '%s'",
                               locator.getOriginalString().c_str()));
                }
                if (listeningLocators.size() != 0)
                    listeningLocators += ";";
                listeningLocators += listeningLocator;
                ++numListeningTransports;
                break;
            }
        }
    }
    if (numListeningTransports == 0) {
        dumpTransportFactories();
        throw Exception(HERE, format(
            "Servers must listen on at least one service locator, but no "
            "possible transports were found for '%s'", localServiceLocator));
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

    CycleCounter<RawMetric> counter;

    // Session was not found in the cache, so create a new one and add
    // it to the cache.

    // Will contain more detailed information about the first transport to
    // throw an exception for this locator.
    string firstException;

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
                auto session = transports[i]->getSession(locator, timeoutMs);
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
                if (firstException.empty())
                    firstException = " (details: " + e.str() + ")";
            }
        }
    }

    string errorMsg;
    if (transportSupported) {
        errorMsg = format("Could not obtain transport session for this "
            "service locator: %s", serviceLocator) + firstException;
    } else {
        errorMsg = format("No supported transport found for this "
            "service locator: %s", serviceLocator);
    }

    throw TransportException(HERE, errorMsg);
}

/**
 * Open a session based on a ServiceLocator string, but ensure that the
 * remote end is the expected ServerId. This will guarantee that the
 * session returned is to the precise server requested. Using locator
 * strings does not guarantee this, as they may be reused across different
 * process instantiations.
 *
 * \param serviceLocator
 *      Desired service.
 *
 * \param needServerId
 *      The ServerId expected for the server being connected to. If the
 *      given id does not match the other end an exception is thrown.
 *
 * \throw NoSuchKeyException
 *      A transport supporting one of the protocols claims a service locator
 *      option is missing.
 *
 * \throw BadValueException
 *      A transport supporting one of the protocols claims a service locator
 *      option is malformed.
 *
 * \throw TransportException
 *      No transport was found for this service locator, or the remote server's
 *      ServerId either could not be obtained or did not match the one provided.
 */
Transport::SessionRef
TransportManager::getSession(const char* serviceLocator, ServerId needServerId)
{
    Transport::SessionRef session = getSession(serviceLocator);
    ServerId actualId;

    try {
        actualId = MembershipClient().getServerId(session);
    } catch (...) {
        throw TransportException(HERE,
            format("Failed to obtain ServerId from \"%s\"", serviceLocator));
    }

    if (actualId != needServerId) {
        // Looks like a locator was reused before this ServerId was
        // removed. This is possible, but should be very rare.
        string errorStr = format("Expected ServerId %lu at \"%s\", but actual "
            "server id was %lu!",
            *needServerId, serviceLocator, *actualId);
        LOG(WARNING, "%s", errorStr.c_str());
        throw TransportException(HERE, errorStr);
    }

    return session;
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
 * Use a particular timeout value for all new transports created from now on.
 *
 * \param timeoutMs
 *      Timeout period (in ms) to pass to transports.
 */
void TransportManager::setTimeout(uint32_t timeoutMs)
{
    this->timeoutMs = timeoutMs;
}

/**
 * Calls dumpStats() on all existing transports.
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
 * Logs the list of transport factories and the protocols they support.
 */
void
TransportManager::dumpTransportFactories()
{
    Dispatch::Lock lock;
    LOG(NOTICE, "The following transport factories are known:");
    uint32_t i = 0;
    foreach (auto factory, transportFactories) {
        LOG(NOTICE,
            "Transport factory %u supports the following protocols:", i);
        foreach (const char* protocol, factory->getProtocols())
          LOG(NOTICE, "  %s", protocol);
        ++i;
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

// See Transport::Session::abort for documentation.
void
TransportManager::WorkerSession::abort(const string& message)
{
    // Must make sure that the dispatch thread isn't running when we
    // invoked the real abort.
    Dispatch::Lock lock;
    return wrapped->abort(message);
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
