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
#include "CoordinatorSession.h"
#include "MasterClient.h"
#include "MultiRead.h"
#include "PingClient.h"

namespace RAMCloud {

// Default RejectRules to use if none are provided by the caller: rejects
// nothing.
static RejectRules defaultRejectRules;

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
    clientContext.coordinatorSession->setLocation(serviceLocator);
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
    clientContext.coordinatorSession->setLocation(serviceLocator);
}

/**
 * Create a new table.
 *
 * \param name
 *      Name for the new table (NULL-terminated string).
 * \param serverSpan
 *      The number of servers across which this table will be divided
 *      (defaults to 1). Keys within the table will be evenly distributed
 *      to this number of servers according to their hash. This is a temporary
 *      work-around until tablet migration is complete; until then, we must
 *      place tablets on servers statically.
 * 
 * \return
 *      The return value is an identifier for the created table; this is
 *      used instead of the table's name for most RAMCloud operations
 *      involving the table.
 */
uint64_t
RamCloud::createTable(const char* name, uint32_t serverSpan)
{
    CreateTableRpc2 rpc(*this, name, serverSpan);
    return rpc.wait();
}

/**
 * Constructor for CreateTableRpc2: initiates an RPC in the same way as
 * #RamCloud::createTable, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param name
 *      Name for the new table (NULL-terminated string).
 * \param serverSpan
 *      The number of servers across which this table will be divided
 *      (defaults to 1).
 */
CreateTableRpc2::CreateTableRpc2(RamCloud& ramcloud,
        const char* name, uint32_t serverSpan)
    : CoordinatorRpcWrapper(ramcloud.clientContext,
            sizeof(WireFormat::CreateTable::Response))
{
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    WireFormat::CreateTable::Request& reqHdr(
            allocHeader<WireFormat::CreateTable>());
    reqHdr.nameLength = length;
    reqHdr.serverSpan = serverSpan;
    memcpy(new(&request, APPEND) char[length], name, length);
    send();
}

/**
 * Wait for the RPC to complete, and return the same results as
 * #RamCloud::createTable.
 * 
 * \return
 *      The return value is an identifier for the created table.
 */
uint64_t
CreateTableRpc2::wait()
{
    waitInternal(*context.dispatch);
    const WireFormat::CreateTable::Response& respHdr(
            getResponseHeader<WireFormat::CreateTable>());
    if (respHdr.common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr.common.status);
    return respHdr.tableId;
}

/**
 * Delete a table.
 *
 * All objects in the table are implicitly deleted, along with any
 * other information associated with the table.  If the table does
 * not currently exist then the operation returns successfully without
 * actually doing anything.
 *
 * \param name
 *      Name of the table to delete (NULL-terminated string).
 */
void
RamCloud::dropTable(const char* name)
{
    DropTableRpc2 rpc(*this, name);
    rpc.wait();
}

/**
 * Constructor for DropTableRpc2: initiates an RPC in the same way as
 * #RamCloud::dropTable, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param name
 *      Name of the table to delete (NULL-terminated string).
 */
DropTableRpc2::DropTableRpc2(RamCloud& ramcloud,
        const char* name)
    : CoordinatorRpcWrapper(ramcloud.clientContext,
            sizeof(WireFormat::DropTable::Response))
{
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    WireFormat::DropTable::Request& reqHdr(
            allocHeader<WireFormat::DropTable>());
    reqHdr.nameLength = length;
    memcpy(new(&request, APPEND) char[length], name, length);
    send();
}

/**
 * Retrieve performance counters from the server that stores a particular
 * object.
 *
 * \param tableId
 *      The table containing the object that determines which server to query.
 * \param key
 *      Variable length key that uniquely identifies an object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 *
 * \return
 *       The performance metrics retrieved from the server that stores the
 *       object indicated by \c tableId, \c key, and \c keyLength.
 */
ServerMetrics
RamCloud::getMetrics(uint64_t tableId, const char* key, uint16_t keyLength)
{
    GetMetricsRpc2 rpc(*this, tableId, key, keyLength);
    return rpc.wait();
}

/**
 * Constructor for GetMetricsRpc2: initiates an RPC in the same way as
 * #RamCloud::getMetrics, but returns once the RPC has been initiated, without
 * waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      The table containing the object that determines which server to query.
 * \param key
 *      Variable length key that uniquely identifies an object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 */
GetMetricsRpc2::GetMetricsRpc2(RamCloud& ramcloud, uint64_t tableId,
        const char* key, uint16_t keyLength)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::GetMetrics::Response))
{
    allocHeader<WireFormat::GetMetrics>();
    send();
}

/**
 * Wait for a getMetrics RPC to complete, and return the same results as
 * #RamCloud::getMetrics.
 *
 * \return
 *       The performance metrics retrieved from the server that stores the
 *       object indicated by the arguments to the constructor.
 */
ServerMetrics
GetMetricsRpc2::wait()
{
    waitInternal(*ramcloud.clientContext.dispatch);
    const WireFormat::GetMetrics::Response& respHdr(
            getResponseHeader<WireFormat::GetMetrics>());

    if (respHdr.common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr.common.status);

    response->truncateFront(sizeof(respHdr));
    assert(respHdr.messageLength == response->getTotalLength());
    ServerMetrics metrics;
    metrics.load(*response);
    return metrics;
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
 * Given the name of a table, return the table's unique identifier, which
 * is used to access the table.
 *
 * \param name
 *      Name of the desired table (NULL-terminated string).
 *
 * \return
 *      The return value is an identifier for the table; this is used
 *      instead of the table's name for most RAMCloud operations
 *      involving the table.
 *
 * \exception TableDoesntExistException
 */
uint64_t
RamCloud::getTableId(const char* name)
{
    GetTableIdRpc2 rpc(*this, name);
    return rpc.wait();
}

/**
 * Constructor for GetTableIdRpc2: initiates an RPC in the same way as
 * #RamCloud::getTableId, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param name
 *      Name of the desired table (NULL-terminated string).
 */
GetTableIdRpc2::GetTableIdRpc2(RamCloud& ramcloud,
        const char* name)
    : CoordinatorRpcWrapper(ramcloud.clientContext,
            sizeof(WireFormat::CreateTable::Response))
{
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    WireFormat::GetTableId::Request& reqHdr(
            allocHeader<WireFormat::GetTableId>());
    reqHdr.nameLength = length;
    memcpy(new(&request, APPEND) char[length], name, length);
    send();
}

/**
 * Wait for a getTableId RPC to complete, and return the same results as
 * #RamCloud::getTableId.
 * 
 * \return
 *      The return value is an identifier for the table.
 *
 * \exception TableDoesntExistException
 */
uint64_t
GetTableIdRpc2::wait()
{
    waitInternal(*context.dispatch);
    const WireFormat::GetTableId::Response& respHdr(
            getResponseHeader<WireFormat::GetTableId>());
    if (respHdr.common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr.common.status);
    return respHdr.tableId;
}

/**
 * Atomically increment the value of an object whose contents are an
 * 8-byte two's complement, little-endian integer.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param incrementValue
 *      This value is added to the current contents of the object (the value
 *      can be negative).
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the increment
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned here.
 *
 * \return
 *      The new value of the object.
 *
 * \exception InvalidObjectException
 *      The object is not 8 bytes in length.
 */
int64_t
RamCloud::increment(uint64_t tableId, const char* key, uint16_t keyLength,
        int64_t incrementValue, const RejectRules* rejectRules,
        uint64_t* version)
{
    IncrementRpc2 rpc(*this, tableId, key, keyLength, incrementValue,
            rejectRules);
    return rpc.wait(version);
}

/**
 * Constructor for IncrementRpc2: initiates an RPC in the same way as
 * #RamCloud::increment, but returns once the RPC has been initiated, without
 * waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param incrementValue
 *      This value is added to the current contents of the object (the value
 *      can be negative).
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the increment
 *      should be aborted with an error.
 */
IncrementRpc2::IncrementRpc2(RamCloud& ramcloud, uint64_t tableId,
        const char* key, uint16_t keyLength, int64_t incrementValue,
        const RejectRules* rejectRules)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::Increment::Response))
{
    WireFormat::Increment::Request& reqHdr(
            allocHeader<WireFormat::Increment>());
    reqHdr.tableId = tableId;
    reqHdr.keyLength = keyLength;
    reqHdr.incrementValue = incrementValue;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    Buffer::Chunk::appendToBuffer(&request, key, keyLength);
    send();
}

/**
 * Wait for an increment RPC to complete, and return the same results as
 * #RamCloud::increment.
 *
 * \param[out] version
 *      If non-NULL, the current version number of the object is
 *      returned here.
 */
int64_t
IncrementRpc2::wait(uint64_t* version)
{
    waitInternal(*ramcloud.clientContext.dispatch);
    const WireFormat::Increment::Response& respHdr(
            getResponseHeader<WireFormat::Increment>());
    if (version != NULL)
        *version = respHdr.version;

    if (respHdr.common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr.common.status);
    return respHdr.newValue;
}

/**
 * Read the current contents of multiple objects. This method has two
 * performance advantages over calling RamCloud::read separately for
 * each object:
 * - If multiple objects are stored on a single server, this method
 *   issues a single RPC to fetch all of them at once.
 * - If different objects are stored on different servers, this method
 *   issues multiple RPCs concurrently.
 *
 * \param requests
 *      Each element in this array describes one object to read, and where
 *      to place its value.
 * \param numRequests
 *      Number of valid entries in \c requests.
 */
void
RamCloud::multiRead(MultiReadObject* requests[], uint32_t numRequests)
{
    MultiRead request(*this, requests, numRequests);
    request.wait();
}

/**
 * Read the current contents of an object.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the read
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned here.
 */
void
RamCloud::read(uint64_t tableId, const char* key, uint16_t keyLength,
                   Buffer* value, const RejectRules* rejectRules,
                   uint64_t* version)
{
    ReadRpc2 rpc(*this, tableId, key, keyLength, value, rejectRules);
    rpc.wait(version);
}

/**
 * Constructor for ReadRpc: initiates an RPC in the same way as
 * #RamCloud::read, but returns once the RPC has been initiated, without
 * waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the read
 *      should be aborted with an error.
 */
ReadRpc2::ReadRpc2(RamCloud& ramcloud, uint64_t tableId,
        const char* key, uint16_t keyLength, Buffer* value,
        const RejectRules* rejectRules)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::Read::Response), value)
{
    value->reset();
    WireFormat::Read::Request& reqHdr(allocHeader<WireFormat::Read>());
    reqHdr.tableId = tableId;
    reqHdr.keyLength = keyLength;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    Buffer::Chunk::appendToBuffer(&request, key, keyLength);
    send();
}

/**
 * Wait for the RPC to complete, and return the same results as
 * #RamCloud::read.
 *
 * \param[out] version
 *      If non-NULL, the version number of the object is returned here.
 */
void
ReadRpc2::wait(uint64_t* version)
{
    waitInternal(*ramcloud.clientContext.dispatch);
    const WireFormat::Read::Response& respHdr(
            getResponseHeader<WireFormat::Read>());
    if (version != NULL)
        *version = respHdr.version;

    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    response->truncateFront(sizeof(respHdr));
    assert(respHdr.length == response->getTotalLength());

    if (respHdr.common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr.common.status);
}

/**
 * Delete an object from a table. If the object does not currently exist
 * then the operation succeeds without doing anything (unless rejectRules
 * causes the operation to be aborted).
 *
 * \param tableId
 *      The table containing the object to be deleted (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the delete
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object (just before
 *      deletion) is returned here.
 */
void
RamCloud::remove(uint64_t tableId, const char* key, uint16_t keyLength,
        const RejectRules* rejectRules, uint64_t* version)
{
    RemoveRpc2 rpc(*this, tableId, key, keyLength, rejectRules);
    rpc.wait(version);
}

/**
 * Constructor for RemoveRpc2: initiates an RPC in the same way as
 * #RamCloud::remove, but returns once the RPC has been initiated, without
 * waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      The table containing the object to be deleted (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the delete
 *      should be aborted with an error.
 */
RemoveRpc2::RemoveRpc2(RamCloud& ramcloud, uint64_t tableId,
        const char* key, uint16_t keyLength, const RejectRules* rejectRules)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::Remove::Response))
{
    WireFormat::Remove::Request& reqHdr(allocHeader<WireFormat::Remove>());
    reqHdr.tableId = tableId;
    reqHdr.keyLength = keyLength;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    Buffer::Chunk::appendToBuffer(&request, key, keyLength);
    send();
}

/**
 * Wait for a remove RPC to complete, and return the same results as
 * #RamCloud::remove.
 *
 * \param[out] version
 *      If non-NULL, the version number of the object (just before
 *      deletion) is returned here.
 */
void
RemoveRpc2::wait(uint64_t* version)
{
    waitInternal(*ramcloud.clientContext.dispatch);
    const WireFormat::Remove::Response& respHdr(
            getResponseHeader<WireFormat::Remove>());
    if (version != NULL)
        *version = respHdr.version;

    if (respHdr.common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr.common.status);
}

/**
 * Divide a tablet into two separate tablets.
 *
 * All objects in the table are implicitly deleted, along with any
 * other information associated with the table.  If the table does
 * not currently exist then the operation returns successfully without
 * actually doing anything.
 *
 * \param name
 *      Name of the table containing the tablet to be split.
 *     (NULL-terminated string).
 * \param startKeyHash
 *      First key of the key range of the tablet to be split.
 * \param endKeyHash
 *      Last key of the key range of the tablet to be split.
 * \param splitKeyHash
 *      Dividing point for the new tablets. All key hashes less than
 *      this will belong to one tablet, and all key hashes >= this
 *      will belong to the other.
 */
void
RamCloud::splitTablet(const char* name, uint64_t startKeyHash,
        uint64_t endKeyHash, uint64_t splitKeyHash)
{
    SplitTabletRpc2 rpc(*this, name, startKeyHash, endKeyHash, splitKeyHash);
    rpc.wait();
}

/**
 * Constructor for SplitTabletRpc2: initiates an RPC in the same way as
 * #RamCloud::splitTablet, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param name
 *      Name of the table containing the tablet to be split.
 *     (NULL-terminated string).
 * \param startKeyHash
 *      First key of the key range of the tablet to be split.
 * \param endKeyHash
 *      Last key of the key range of the tablet to be split.
 * \param splitKeyHash
 *      Dividing point for the new tablets. All key hashes less than
 *      this will belong to one tablet, and all key hashes >= this
 *      will belong to the other.
 */
SplitTabletRpc2::SplitTabletRpc2(RamCloud& ramcloud,
        const char* name, uint64_t startKeyHash,
        uint64_t endKeyHash, uint64_t splitKeyHash)
    : CoordinatorRpcWrapper(ramcloud.clientContext,
            sizeof(WireFormat::SplitTablet::Response))
{
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    WireFormat::SplitTablet::Request& reqHdr(
            allocHeader<WireFormat::SplitTablet>());
    reqHdr.nameLength = length;
    reqHdr.startKeyHash = startKeyHash;
    reqHdr.endKeyHash = endKeyHash;
    reqHdr.splitKeyHash = splitKeyHash;
    memcpy(new(&request, APPEND) char[length], name, length);
    send();
}

/**
 * Ask a master to create a given number of objects, each of the
 * same given size. Objects are added to all tables in the master in
 * a round-robin fashion. This method exists simply to quickly fill a
 * master for experiments.
 *
 * \bug Will return an error if the master only owns part of a table
 * (because the hash of the fabricated keys may land in a region it
 * doesn't own).
 *
 * \param tableId
 *      Together with \a key identifies the master that should create
 *      the objects.
 * \param key
 *      Together with \a tableId identifies the master that should create
 *      the objects. It does not necessarily have to be null terminated
 *      like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \param numObjects
 *      Total number of objects to add to the server.
 * \param objectSize
 *      Bytes of garbage data to place in each object not including the
 *      key (the keys are ASCII strings starting with "0" and increasing
 *      numerically in each table).
 */
void
RamCloud::testingFill(uint64_t tableId, const char* key, uint16_t keyLength,
                      uint32_t numObjects, uint32_t objectSize)
{
    FillWithTestDataRpc2 rpc(*this, tableId, key, keyLength, numObjects,
            objectSize);
    rpc.wait();
}

/**
 * Constructor for FillWithTestDataRpc2: initiates an RPC in the same way as
 * #RamCloud::testingFill, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      Together with \a key identifies the master that should create
 *      the objects.
 * \param key
 *      Together with \a tableId identifies the master that should create
 *      the objects. It does not necessarily have to be null terminated
 *      like a string.
 * \param keyLength
 *      Size in bytes of the key.
 * \param numObjects
 *      Total number of objects to add to the server.
 * \param objectSize
 *      Bytes of garbage data to place in each object not including the
 *      key (the keys are ASCII strings starting with "0" and increasing
 *      numerically in each table).
 */
FillWithTestDataRpc2::FillWithTestDataRpc2(RamCloud& ramcloud,
        uint64_t tableId, const char* key, uint16_t keyLength,
        uint32_t numObjects, uint32_t objectSize)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::FillWithTestData::Response))
{
    WireFormat::FillWithTestData::Request& reqHdr(
            allocHeader<WireFormat::FillWithTestData>());
    reqHdr.numObjects = numObjects;
    reqHdr.objectSize = objectSize;
    send();
}

/**
 * Crash the master that owns a specific object. Crash is confirmed through
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
    KillRpc2 rpc(*this, tableId, key, keyLength);
    objectFinder.waitForTabletDown();
}

/**
 * Constructor for KillRpc2: initiates an RPC in the same way as
 * #RamCloud::testingKill, but returns once the RPC has been initiated, without
 * waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 */
KillRpc2::KillRpc2(RamCloud& ramcloud, uint64_t tableId,
        const char* key, uint16_t keyLength)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::Kill::Response))
{
    allocHeader<WireFormat::Kill>();
    send();
}

/**
 * Set a runtime option field on the coordinator to the indicated value.
 *
 * \param option
 *      String name corresponding to a member field in the RuntimeOptions
 *      class (e.g. "failRecoveryMasters") whose value should be replaced with
 *      the given value.
 * \param value
 *      String that can be parsed into the type of the field indicated by
 *      \a option. The format is specific to the type of each field but is
 *      generally either a single value (e.g. "10", "word") or a collection
 *      separated by spaces (e.g. "1 2 3", "first second"). See RuntimeOptions
 *      for more information.
 */
void
RamCloud::testingSetRuntimeOption(const char* option, const char* value)
{
    SetRuntimeOptionRpc2 rpc(*this, option, value);
    rpc.wait();
}

/**
 * Constructor for SetRuntimeOptionRpc2: initiates an RPC in the same way as
 * #RamCloud::dropTable, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param option
 *      String name corresponding to a member field in the RuntimeOptions
 *      class (e.g. "failRecoveryMasters") whose value should be replaced with
 *      the given value.
 * \param value
 *      String that can be parsed into the type of the field indicated by
 *      \a option. The format is specific to the type of each field but is
 *      generally either a single value (e.g. "10", "word") or a collection
 *      separated by spaces (e.g. "1 2 3", "first second"). See RuntimeOptions
 *      for more information.
 */
SetRuntimeOptionRpc2::SetRuntimeOptionRpc2(RamCloud& ramcloud,
        const char* option, const char* value)
    : CoordinatorRpcWrapper(ramcloud.clientContext,
            sizeof(WireFormat::SetRuntimeOption::Response))
{
    WireFormat::SetRuntimeOption::Request& reqHdr(
            allocHeader<WireFormat::SetRuntimeOption>());
    reqHdr.optionLength = downCast<uint32_t>(strlen(option) + 1);
    reqHdr.valueLength = downCast<uint32_t>(strlen(value) + 1);
    Buffer::Chunk::appendToBuffer(&request, option, reqHdr.optionLength);
    Buffer::Chunk::appendToBuffer(&request, value, reqHdr.valueLength);
    send();
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

/**
 * Replace the value of a given object, or create a new object if none
 * previously existed.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the write
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned here.
 *      If the operation was successful this will be the new version for
 *      the object. If the operation failed then the version number returned
 *      is the current version of the object, or 0 if the object does not
 *      exist.
 * \param async
 *      If true, the new object will not be immediately replicated to backups.
 *      Data loss may occur!
 *
 * \exception RejectRulesException
 */
void
RamCloud::write(uint64_t tableId, const char* key, uint16_t keyLength,
        const void* buf, uint32_t length, const RejectRules* rejectRules,
        uint64_t* version, bool async)
{
    WriteRpc2 rpc(*this, tableId, key, keyLength, buf, length, rejectRules,
            async);
    rpc.wait(version);
}

/**
 * Replace the value of a given object, or create a new object if none
 * previously existed.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param value
 *      NULL-terminated string providing the new value for the object (the
 *      terminating NULL character will not be part of the object).
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the write
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned here.
 *      If the operation was successful this will be the new version for
 *      the object. If the operation failed then the version number returned
 *      is the current version of the object, or 0 if the object does not
 *      exist.
 * \param async
 *      If true, the new object will not be immediately replicated to backups.
 *      Data loss may occur!
 *
 * \exception RejectRulesException
 */
void
RamCloud::write(uint64_t tableId, const char* key, uint16_t keyLength,
        const char* value, const RejectRules* rejectRules, uint64_t* version,
        bool async)
{
    WriteRpc2 rpc(*this, tableId, key, keyLength, value,
            downCast<uint32_t>(strlen(value)), rejectRules, async);
    rpc.wait(version);
}

/**
 * Constructor for WriteRpc2: initiates an RPC in the same way as
 * #RamCloud::write, but returns once the RPC has been initiated, without
 * waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Variable length key that uniquely identifies the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the write
 *      should be aborted with an error.
 * \param async
 *      If true, the new object will not be immediately replicated to backups.
 *      Data loss may occur!
 */
WriteRpc2::WriteRpc2(RamCloud& ramcloud, uint64_t tableId,
        const char* key, uint16_t keyLength, const void* buf, uint32_t length,
        const RejectRules* rejectRules, bool async)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::Write::Response))
{
    WireFormat::Write::Request& reqHdr(allocHeader<WireFormat::Write>());
    reqHdr.tableId = tableId;
    reqHdr.keyLength = keyLength;
    reqHdr.length = length;
    reqHdr.rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    reqHdr.async = async;
    Buffer::Chunk::appendToBuffer(&request, key, keyLength);
    Buffer::Chunk::appendToBuffer(&request, buf, length);
    send();
}

/**
 * Wait for a write RPC to complete, and return the same results as
 * #RamCloud::write.
 *
 * \param[out] version
 *      If non-NULL, the current version number of the object is
 *      returned here.
 */
void
WriteRpc2::wait(uint64_t* version)
{
    waitInternal(*ramcloud.clientContext.dispatch);
    const WireFormat::Write::Response& respHdr(
            getResponseHeader<WireFormat::Write>());
    if (version != NULL)
        *version = respHdr.version;

    if (respHdr.common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr.common.status);
}

//-------------------------------------------------------
// OLD: everything below here should eventually go away.
//-------------------------------------------------------

/// \copydoc PingClient::getMetrics
ServerMetrics
RamCloud::getMetrics(const char* serviceLocator)
{
    PingClient client(clientContext);
    return client.getMetrics(serviceLocator);
}

}  // namespace RAMCloud
