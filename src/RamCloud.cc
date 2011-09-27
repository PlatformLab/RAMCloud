
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
    : realClientContext()
    , clientContext(*realClientContext.construct(false))
    , constructorContext(clientContext)
    , status(STATUS_OK)
    , coordinator(serviceLocator)
    , objectFinder(coordinator)
{
    // This should be the last line on all return paths of this constructor.
    constructorContext.leave();
}

/**
 * An alternate constructor that inherits an already created context. This is
 * useful for testing and for client programs that mess with the context
 * (which should be discouraged).
 */
RamCloud::RamCloud(Context& context, const char* serviceLocator)
    : realClientContext()
    , clientContext(context)
    , constructorContext(clientContext)
    , status(STATUS_OK)
    , coordinator(serviceLocator)
    , objectFinder(coordinator)
{
    // This should be the last line on all return paths of this constructor.
    constructorContext.leave();
}

/// \copydoc CoordinatorClient::createTable
void
RamCloud::createTable(const char* name)
{
    Context::Guard _(clientContext);
    coordinator.createTable(name);
}

/// \copydoc CoordinatorClient::dropTable
void
RamCloud::dropTable(const char* name)
{
    Context::Guard _(clientContext);
    coordinator.dropTable(name);
}

/// \copydoc CoordinatorClient::openTable
uint32_t
RamCloud::openTable(const char* name)
{
    Context::Guard _(clientContext);
    return coordinator.openTable(name);
}

/// \copydoc MasterClient::create
uint64_t
RamCloud::create(uint32_t tableId, const void* buf, uint32_t length,
                 uint64_t* version, bool async)
{
    Context::Guard _(clientContext);
    return Create(*this, tableId, buf, length, version, async)();
}

/// \copydoc PingClient::ping
uint64_t
RamCloud::ping(const char* serviceLocator, uint64_t nonce,
               uint64_t timeoutNanoseconds)
{
    Context::Guard _(clientContext);
    PingClient client;
    return client.ping(serviceLocator, nonce, timeoutNanoseconds);
}

/**
 * Issue a trivial RPC to the server that manages a particular object,
 * to make sure that it exists and is responsive.
 *
 * \param table
 *      Identifier for a table.
 * \param objectId
 *      Identifier for an object within \c tableId; the server that manages
 *      this object is the one that will be pinged.
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
RamCloud::ping(uint32_t table, uint64_t objectId, uint64_t nonce,
               uint64_t timeoutNanoseconds)
{
    Context::Guard _(clientContext);
    PingClient client;
    const char *serviceLocator = objectFinder.lookup(table, objectId)->
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
    Context::Guard _(clientContext);
    PingClient client;
    return client.proxyPing(serviceLocator1, serviceLocator2,
                            timeoutNanoseconds1, timeoutNanoseconds2);
}

/// \copydoc MasterClient::read
void
RamCloud::read(uint32_t tableId, uint64_t id, Buffer* value,
               const RejectRules* rejectRules, uint64_t* version)
{
    Context::Guard _(clientContext);
    return Read(*this, tableId, id, value, rejectRules, version)();
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
    Context::Guard _(clientContext);
    std::vector<ObjectFinder::MasterRequests> requestBins =
                            objectFinder.multiLookup(requests, numRequests);

    // By default multiRead is to be done in parallel. Can be changed here
    // if we need to do it sequentially for benchmarking.
    bool parallel = true;

    if (parallel == true) {
        uint32_t numBins = downCast<uint32_t>(requestBins.size());
        Tub<MasterClient::MultiRead> multiReadInstances[numBins];
        for (uint32_t i = 0; i < numBins; i++) {
            MasterClient master(requestBins[i].sessionRef);
            multiReadInstances[i].construct(master, requestBins[i].requests);
        }
        for (uint32_t i = 0; i < numBins; i++) {
            multiReadInstances[i]->complete();
        }
    } else {
        foreach (ObjectFinder::MasterRequests requestBin, requestBins) {
            MasterClient master(requestBin.sessionRef);
            master.multiRead(requestBin.requests);
        }
    }
}

/// \copydoc MasterClient::remove
void
RamCloud::remove(uint32_t tableId, uint64_t id,
                 const RejectRules* rejectRules, uint64_t* version)
{
    Context::Guard _(clientContext);
    MasterClient master(objectFinder.lookup(tableId, id));
    while (1) {
        // Keep trying the operation if the server responded with a retry
        // status.
        try {
            master.remove(tableId, id, rejectRules, version);
            break;
        } catch (RetryException& e) {
        } catch (...) {
            throw;
        }
    }
}

/// \copydoc MasterClient::write
void
RamCloud::write(uint32_t tableId, uint64_t id,
                const void* buf, uint32_t length,
                const RejectRules* rejectRules, uint64_t* version,
                bool async)
{
    Context::Guard _(clientContext);
    while (1) {
        // Keep trying the operation if the server responded with a retry
        // status.
        try {
            Write(*this, tableId, id, buf, length,
                  rejectRules, version, async)();
            break;
        } catch (RetryException& e) {
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
 *      previous call to openTable).
 * \param id
 *      Identifier within tableId of the object to be written; may or
 *      may not refer to an existing object.
 * \param s
 *      NULL-terminated string; its contents (not including the
 *      terminating NULL character) are stored in the object.
 */
void
RamCloud::write(uint32_t tableId, uint64_t id, const char* s)
{
    Context::Guard _(clientContext);
    while (1) {
        // Keep trying the operation if the server responded with a retry
        // status.
        try {
            Write(*this, tableId, id, s, downCast<int>(strlen(s)), NULL,
                  NULL, false)();
            break;
        } catch (RetryException& e) {
        } catch (...) {
            throw;
        }
    }
}

}  // namespace RAMCloud
