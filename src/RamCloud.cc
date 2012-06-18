/* Copyright (c) 2010-2012 Stanford University
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

#include "RamCloud.h"
#include "MasterClient.h"
#include "PingClient.h"

namespace RAMCloud {

/**
 * Construct a RamCloud for a particular service: opens a connection with the
 * service.
 *
 * \param serviceLocator
 *      The service locator for the coordinator.
 *      See \ref ServiceLocatorStrings.
 * \exception CouldntConnectException
 *      Couldn't connect to the server.
 */
RamCloud::RamCloud(const char* serviceLocator)
    : coordinatorLocator(serviceLocator)
    , realClientContext()
    , clientContext(*realClientContext.construct(false))
    , status(STATUS_OK)
    , coordinator(clientContext, serviceLocator)
    , objectFinder(clientContext, coordinator)
{
}

/**
 * An alternate constructor that inherits an already created context. This is
 * useful for testing and for client programs that mess with the context
 * (which should be discouraged).
 */
RamCloud::RamCloud(Context& context, const char* serviceLocator)
    : coordinatorLocator(serviceLocator)
    , realClientContext()
    , clientContext(context)
    , status(STATUS_OK)
    , coordinator(context, serviceLocator)
    , objectFinder(clientContext, coordinator)
{
}

/// \copydoc CoordinatorClient::createTable
void
RamCloud::createTable(const char* name, uint32_t serverSpan)
{
    coordinator.createTable(name, serverSpan);
}

/// \copydoc CoordinatorClient::dropTable
void
RamCloud::dropTable(const char* name)
{
    coordinator.dropTable(name);
}

/// \copydoc CoordinatorClient::splitTablet
void
RamCloud::splitTablet(const char* name, uint64_t startKeyHash,
                      uint64_t endKeyHash, uint64_t splitKeyHash)
{
    coordinator.splitTablet(name, startKeyHash, endKeyHash, splitKeyHash);
}

/// \copydoc CoordinatorClient::getTableId
uint64_t
RamCloud::getTableId(const char* name)
{
    return coordinator.getTableId(name);
}

/// \copydoc PingClient::getMetrics
ServerMetrics
RamCloud::getMetrics(const char* serviceLocator)
{
    PingClient client(clientContext);
    return client.getMetrics(serviceLocator);
}

/**
 * Retrieve performance counters from the server that stores a given object.
 *
 * \param table
 *      Identifier for a table.
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 */
ServerMetrics
RamCloud::getMetrics(uint64_t table, const char* key, uint16_t keyLength)
{
    PingClient client(clientContext);
    const char *serviceLocator = objectFinder.lookup(table, key, keyLength)->
            getServiceLocator().c_str();
    return client.getMetrics(serviceLocator);
}

/**
 * Return the service locator for the coordinator for this cluster.
 */
string*
RamCloud::getServiceLocator()
{
    return &coordinatorLocator;
}

/**
 * Ping a server at the given ServiceLocator string.
 *
 * \param serviceLocator
 *      ServiceLocator string of the server to ping.
 *
 * \param nonce
 *      64-bit nonce to include in the ping. Can be used to match
 *      replies to requests.
 *
 * \param timeoutNanoseconds
 *      Time to wait for a reply in nanoseconds. If the timeout
 *      expires, a TimeoutException is thrown.
 *
 * \return
 *      The nonce returned by the pinged server is returned.
 *
 * \throw TimeoutException
 *      A TimeoutException is thrown if no reply is received
 *      in #timeoutNanoseconds.
 */
uint64_t
RamCloud::ping(const char* serviceLocator, uint64_t nonce,
               uint64_t timeoutNanoseconds)
{
    PingClient client(clientContext);
    return client.ping(serviceLocator, nonce, timeoutNanoseconds);
}

/**
 * Issue a trivial RPC to the server that manages a particular object,
 * to make sure that it exists and is responsive.
 *
 * \param table
 *      Identifier for a table.
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \param nonce
 *      Arbitrary 64-bit value to pass to the server; the server will return
 *      this value in its response.
 * \param timeoutNanoseconds
 *      The maximum amount of time to wait for a response (in nanoseconds).
 * \result
 *      The value returned by the server, which should be the same as \c nonce
 *      (this method does not verify that the value does in fact match).
 *
 * \throw TimeoutException
 *      The server did not respond within \c timeoutNanoseconds.
 */
uint64_t
RamCloud::ping(uint64_t table, const char* key, uint16_t keyLength,
               uint64_t nonce, uint64_t timeoutNanoseconds)
{
    PingClient client(clientContext);
    const char *serviceLocator = objectFinder.lookup(table, key, keyLength)->
            getServiceLocator().c_str();
    return client.ping(serviceLocator, nonce, timeoutNanoseconds);
}

/// \copydoc PingClient::proxyPing
uint64_t
RamCloud::proxyPing(const char* serviceLocator1,
                    const char* serviceLocator2,
                    uint64_t timeoutNanoseconds1,
                    uint64_t timeoutNanoseconds2)
{
    PingClient client(clientContext);
    return client.proxyPing(serviceLocator1, serviceLocator2,
                            timeoutNanoseconds1, timeoutNanoseconds2);
}

/// \copydoc MasterClient::read
void
RamCloud::read(uint64_t tableId, const char* key, uint16_t keyLength,
               Buffer* value, const RejectRules* rejectRules,
               uint64_t* version)
{
    while (1) {
        // Keep trying the operation if the server responded with a retry
        // status.
        try {
            return Read(*this, tableId, key, keyLength, value, rejectRules,
                        version)();
        } catch (RetryException& e) {
        } catch (UnknownTableException& e) {
            // The Tablet Map pointed to some server, but it's no longer
            // in charge of the appropriate tablet. We need to refresh.
            objectFinder.flush();
        } catch (...) {
            throw;
        }
    }
}

/// \copydoc MasterClient::increment
void
RamCloud::increment(uint64_t tableId, const char* key, uint16_t keyLength,
                    int64_t incrementValue, const RejectRules* rejectRules,
                    uint64_t* version, int64_t* newValue)
{
    MasterClient master(objectFinder.lookup(tableId, key, keyLength));
    master.increment(tableId, key, keyLength, incrementValue, rejectRules,
                     version, newValue);
}

/**
 * Read the current contents of multiple objects.
 *
 * \param requests
 *      Array (of ReadObject's) listing the objects to be read
 *      and where to place their values
 * \param numRequests
 *      Number of valid entries in \c requests.
 */
void
RamCloud::multiRead(MasterClient::ReadObject* requests[], uint32_t numRequests)
{
    std::vector<ObjectFinder::MasterRequests> requestBins =
                            objectFinder.multiLookup(requests, numRequests);

    uint32_t numBins = downCast<uint32_t>(requestBins.size());
    Tub<MasterClient::MultiRead> multiReadInstances[numBins];

    // Send requests to all servers in serial without waiting for
    // responses for parallelism.
    for (uint32_t i = 0; i < numBins; i++) {
        MasterClient master(requestBins[i].sessionRef);
        multiReadInstances[i].construct(master, requestBins[i].requests);
    }
    // Receive responses from the servers in serial.
    for (uint32_t i = 0; i < numBins; i++) {
        multiReadInstances[i]->complete();
    }
}

/// \copydoc MasterClient::remove
void
RamCloud::remove(uint64_t tableId, const char* key, uint16_t keyLength,
                 const RejectRules* rejectRules, uint64_t* version)
{
    MasterClient master(objectFinder.lookup(tableId, key, keyLength));
    while (1) {
        // Keep trying the operation if the server responded with a retry
        // status.
        try {
            master.remove(tableId, key, keyLength, rejectRules, version);
            break;
        } catch (RetryException& e) {
        } catch (UnknownTableException& e) {
            // The Tablet Map pointed to some server, but it's no longer
            // in charge of the appropriate tablet. We need to refresh.
            objectFinder.flush();
        } catch (...) {
            throw;
        }
    }
}

/// \copydoc MasterClient::write
void
RamCloud::write(uint64_t tableId, const char* key, uint16_t keyLength,
                const void* buf, uint32_t length,
                const RejectRules* rejectRules, uint64_t* version,
                bool async)
{
    while (1) {
        // Keep trying the operation if the server responded with a retry
        // status.
        try {
            Write(*this, tableId, key, keyLength, buf, length,
                  rejectRules, version, async)();
            break;
        } catch (RetryException& e) {
        } catch (UnknownTableException &e) {
            // The Tablet Map pointed to some server, but it's no longer
            // in charge of the appropriate tablet. We need to refresh.
            objectFinder.flush();
        } catch (...) {
            throw;
        }
    }
}

/**
 * Write a specific object in a table with a string value; overwrite any
 * existing object, or create a new object if none existed.
 *
 * \param tableId
 *      The table containing the desired object (return value from a
 *      previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \param s
 *      NULL-terminated string; its contents (not including the
 *      terminating NULL character) are stored in the object.
 */
void
RamCloud::write(uint64_t tableId, const char* key, uint16_t keyLength,
                const char* s)
{
    while (1) {
        // Keep trying the operation if the server responded with a retry
        // status.
        try {
            Write(*this, tableId, key, keyLength, s,
                  downCast<int>(strlen(s)), NULL, NULL, false)();
            break;
        } catch (RetryException& e) {
        } catch (UnknownTableException &e) {
            // The Tablet Map pointed to some server, but it's no longer
            // in charge of the appropriate tablet. We need to refresh.
            objectFinder.flush();
        } catch (...) {
            throw;
        }
    }
}

}  // namespace RAMCloud
