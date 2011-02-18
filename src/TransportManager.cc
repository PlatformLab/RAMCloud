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
 * It's priority ensures that it is initialized after the TestLog.
 */
TransportManager  __attribute__((init_priority(400))) transportManager;

TransportManager::TransportManager()
    : initialized(false)
    , transportFactories()
    , transports()
    , listening()
    , nextToListen(0)
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
 * Construct the individual transports that will be used to send and receive.
 *
 * Calling this method is required before any calls to #serverRecv(), since the
 * receiving transports need to be instantiated with their local addresses
 * first. In this case, it must be called explicitly before any calls to
 * #getSession().
 *
 * Calling this method is not required if #serverRecv() will never be called.
 */
void
TransportManager::initialize(const char* localServiceLocator)
{
    assert(!initialized);

    auto locators = ServiceLocator::parseServiceLocators(localServiceLocator);

    foreach (auto factory, transportFactories) {
        Transport* transport = NULL;
        foreach (auto& locator, locators) {
            if (factory->supports(locator.getProtocol().c_str())) {
                // The transport supports a protocol that we can receive
                // packets on. Since it is expected that this transport
                // work, we do not catch exceptions if it is unavailable.
                transport = factory->createTransport(&locator);
                listening.push_back(transport);
                goto insert_protocol_mappings;
            }
        }

        // The transport doesn't support any protocols that we can receive
        // packets on. Such transports need not be available, e.g. if the
        // physical device (NIC) does not exist.
        try {
            transport = factory->createTransport(NULL);
        } catch (TransportException &e) {
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
 * For now, multiple calls with the same argument will yield distinct sessions.
 * This will probably change later.
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

    CycleCounter<Metric> _(&metrics->transport.sessionOpenTicks);

    // Session was not found in the cache, a new one will be created
    auto locators = ServiceLocator::parseServiceLocators(serviceLocator);
    // The first protocol specified in the locator that works is chosen
    foreach (auto& locator, locators) {
        foreach (auto& protocolTransport,
                 protocolTransportMap.equal_range(locator.getProtocol())) {
            auto transport = protocolTransport.second;
            try {
                auto session = transport->getSession(locator);

                // Only first protocol is used, but the cache is based
                // on the complete initial service locator string.
                // No caching should occur if an exception is thrown.
                sessionCache.insert({serviceLocator, session});
                session->setServiceLocator(serviceLocator);
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
 * Receive an RPC request. This will block until receiving a packet from any
 * listening transport.
 * \throw TransportException
 *      There are no listening transports, so this call would block forever.
 */
Transport::ServerRpc*
TransportManager::serverRecv()
{
    if (!initialized || listening.empty())
        throw TransportException(HERE, "no transports to listen on");
    CycleCounter<Metric> _(&metrics->idleTicks);
    while (true) {
        if (nextToListen >= listening.size()) {
            Dispatch::poll();
            nextToListen = 0;
        }
        auto transport = listening[nextToListen++];

        auto rpc = transport->serverRecv();
        if (rpc != NULL)
            return rpc;
    }
}

/**
 * Obtain a list of listening ServiceLocators.
 * \return
 *      A vector of ServiceLocators that are listening for RPCs.
 * \throw TransportException
 *      if this TransportManager has not been initialized.
 */
ServiceLocatorList
TransportManager::getListeningLocators()
{
    if (!initialized)
        throw TransportException(HERE, "TransportManager not initialized");

    ServiceLocatorList list;
    foreach (auto transport, listening)
        list.push_back(transport->getServiceLocator());
    return list;
}

/**
 * Obtain a ServiceLocator string corresponding to the listening
 * ServiceLocators.
 * \return
 *      A semicolon-delimited, ServiceLocator string containing all
 *      ServiceLocators' strings that are listening to RPCs. 
 * \throw TransportException
 *      if this TransportManager has not been initialized.
 */
string
TransportManager::getListeningLocatorsString()
{
    ServiceLocatorList sll = getListeningLocators();
    string ret;
    for (uint32_t i = 0; i < sll.size(); i++) {
        if (i > 0)
            ret += ";";
        ret += sll[i].getOriginalString();
    }
    return ret;
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

} // namespace RAMCloud
