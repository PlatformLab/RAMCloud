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
 * Test if any objects remain in the table.
 *
 * \result
 *      True if any objects remain, or false otherwise.
 */
bool
RamCloud::Enumeration::hasNext()
{
    requestMoreObjects();
    return !done;
}

/**
 * Return the next object in the table.
 *
 * \param[out] size
 *      After a successful return, this field will hold the size of
 *      the object in bytes.
 * \param[out] object
 *      After a successful return, this will point to contiguous
 *      memory containing an instance of Object immediately followed
 *      by its key and data payloads.
 */
void
RamCloud::Enumeration::next(uint32_t* size, const void** object)
{
    *size = 0;
    *object = NULL;

    requestMoreObjects();
    if (done) return;

    uint32_t objectSize = *objects.getOffset<uint32_t>(nextOffset);
    nextOffset += downCast<uint32_t>(sizeof(uint32_t));

    const void* blob = objects.getRange(nextOffset, objectSize);
    nextOffset += objectSize;

    // Store result in out params.
    *size = objectSize;
    *object = blob;
}

/**
 * Used internally by #hasNext() and #next() to retrieve objects. Will
 * set the #done field if enumeration is complete. Otherwise the
 * #objects Buffer will contain at least one object.
 */
void
RamCloud::Enumeration::requestMoreObjects()
{
    if (done || nextOffset < objects.getTotalLength()) return;

    uint64_t nextTabletStartHash;
    objects.reset();
    nextOffset = 0;

    while (objects.getTotalLength() == 0) {
        try {
            MasterClient master(
                ramCloud.objectFinder.lookup(tableId, tabletStartHash));
            master.enumeration(tableId, tabletStartHash, &nextTabletStartHash,
                               &iter[currentIter], &iter[!currentIter],
                               &objects);
        } catch (RetryException& e) {
            continue;
        } catch (UnknownTableException& e) {
            // The Tablet Map pointed to some server, but it's no longer
            // in charge of the appropriate tablet. We need to refresh.
            ramCloud.objectFinder.flush();
            continue;
        }

        // End of table?
        if (objects.getTotalLength() == 0 &&
            nextTabletStartHash <= tabletStartHash) {
            done = true;
            return;
        }

        currentIter = !currentIter;
        tabletStartHash = nextTabletStartHash;
    }
}

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
uint64_t
RamCloud::createTable(const char* name, uint32_t serverSpan)
{
    return coordinator.createTable(name, serverSpan);
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
    return client.ping(serviceLocator, ServerId(), nonce, timeoutNanoseconds);
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
    return client.ping(serviceLocator, ServerId(), nonce, timeoutNanoseconds);
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

/**
 * Crash a master which owns a specific object. Crash is confirmed through
 * the coordinator at the time the call returns.
 *
 * \param tableId
 *      Together with \a key identifies a master which is the current
 *      owner of that key which will be crashed.
 * \param key
 *      Together with \a tableId identifies a master which is the current
 *      owner of that key which will be crashed.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 */
void
RamCloud::testingKill(uint64_t tableId, const char* key, uint16_t keyLength)
{
    Transport::SessionRef session =
        objectFinder.lookup(tableId, key, keyLength);
    PingClient pingClient(clientContext);
    PingClient::Kill killOp(pingClient, session->getServiceLocator().c_str());
    objectFinder.waitForTabletDown();
    killOp.cancel();
}

/**
 * Fill a master server with the given number of objects, each of the
 * same given size. Objects are added to all tables in the master in
 * a round-robin fashion. This method exists simply to quickly fill a
 * master for experiments.
 *
 * See MasterClient::fillWithTestData() for more information.
 *
 * \bug Will return an error if the master only owns part of a table
 * (because the hash of the fabricated keys may land in a region it
 * doesn't own).
 *
 * \param tableId
 *      Together with \a key identifies a master which is the current
 *      owner of that key which will be filled with junk data.
 * \param key
 *      Together with \a tableId identifies a master which is the current
 *      owner of that key which will be filled with junk data.
 *      It does not necessarily have to be null terminated like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \param objectCount
 *      Total number of objects to add to the server.
 * \param objectSize
 *      Bytes of garbage data to place in each object not including the
 *      key (the keys are ASCII strings starting with "0" and increasing
 *      numerically in each table).
 */
void
RamCloud::testingFill(uint64_t tableId, const char* key, uint16_t keyLength,
                      uint32_t objectCount, uint32_t objectSize)
{
    MasterClient master(objectFinder.lookup(tableId, key, keyLength));
    master.fillWithTestData(objectCount, objectSize);
}

/**
 * Sets a runtime option field on the coordinator to the indicated value.
 *
 * \param option
 *      String name which corresponds to a member field in the RuntimeOptions
 *      class (e.g.  "failRecoveryMasters") whose value should be replaced with
 *      the given value.
 * \param value
 *      String which can be parsed into the type of the field indicated by
 *      \a option. The format is specific to the type of each field but is
 *      generally either a single value (e.g. "10", "word") or a collection
 *      separated by spaces (e.g. "1 2 3", "first second"). See RuntimeOptions
 *      for more information.
 */
void
RamCloud::testingSetRuntimeOption(const char* option, const char* value)
{
    coordinator.setRuntimeOption(option, value);
}

/**
 * Block and query coordinator until all tablets have normal status
 * (that is, no tablet is under recovery).
 */
void
RamCloud::testingWaitForAllTabletsNormal()
{
    objectFinder.waitForAllTabletsNormal();
}

}  // namespace RAMCloud
