/* Copyright (c) 2010-2015 Stanford University
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

#include <stdarg.h>

#include "RamCloud.h"
#include "ClientLeaseAgent.h"
#include "ClientTransactionManager.h"
#include "CoordinatorSession.h"
#include "Dispatch.h"
#include "LinearizableObjectRpcWrapper.h"
#include "FailSession.h"
#include "MasterClient.h"
#include "MultiIncrement.h"
#include "MultiRead.h"
#include "MultiRemove.h"
#include "MultiWrite.h"
#include "Object.h"
#include "ObjectFinder.h"
#include "ProtoBuf.h"
#include "RpcTracker.h"
#include "ShortMacros.h"
#include "TimeTrace.h"

namespace RAMCloud {

// Default RejectRules to use if none are provided by the caller: rejects
// nothing.
static RejectRules defaultRejectRules;

/**
 * Construct a RamCloud for a particular cluster.
 *
 * \param locator
 *      Describes how to locate the coordinator. It can have either of
 *      two forms. The preferred form is a locator for external storage
 *      that contains the cluster configuration information (such as a
 *      string starting with "zk:", which will be passed to the ZooStorage
 *      constructor). With this form, sessions can automatically be
 *      redirected to a new coordinator if the current one crashes.
 *      Typically the value for this argument will be the same as the
 *      value of the "-x" command-line option given to the coordinator
 *      when it started. The second form is deprecated, but is retained
 *      for testing. In this form, the location is specified as a RAMCloud
 *      service locator for a specific coordinator. With this form it is
 *      not possible to roll over to a different coordinator if a given
 *      one fails; we will have to wait for the specified coordinator to
 *      restart.
 * \param clusterName
 *      Name of the current cluster. Used to allow independent operation
 *      of several clusters sharing many of the same resources. This is
 *      typically the same as the value of the "--clusterName" command-line
 *      option given to the coordinator when it started.
 *
 * \exception CouldntConnectException
 *      Couldn't connect to the server.
 */
RamCloud::RamCloud(const char* locator, const char* clusterName)
    : coordinatorLocator(locator)
    , realClientContext(new Context(false))
    , clientContext(realClientContext)
    , status(STATUS_OK)
    , clientLeaseAgent(new ClientLeaseAgent(this))
    , rpcTracker(new RpcTracker())
    , transactionManager(new ClientTransactionManager())
{
    clientContext->coordinatorSession->setLocation(locator, clusterName);
}

/**
 * An alternate constructor that inherits an already created context. This is
 * useful for testing and for client programs that mess with the context
 * (which should be discouraged).
 */
RamCloud::RamCloud(Context* context, const char* locator,
        const char* clusterName)
    : coordinatorLocator(locator)
    , realClientContext(NULL)
    , clientContext(context)
    , status(STATUS_OK)
    , clientLeaseAgent(new ClientLeaseAgent(this))
    , rpcTracker(new RpcTracker())
    , transactionManager(new ClientTransactionManager())
{
    clientContext->coordinatorSession->setLocation(locator, clusterName);
}

/**
 * Destructor of Ramcloud
 **/

RamCloud::~RamCloud()
{
    delete clientLeaseAgent;

    delete rpcTracker;
    delete realClientContext;

    delete transactionManager;
}

/**
 * Give polling-based operations (such as those checking for incoming network
 * packets) a chance to execute. This method is typically invoked during loops
 * that wait for asynchronous RPCs to complete by calling isReady repeatedly.
 * In general, an asynchronous RPC will not make progress unless either
 * this method is invoked or the "wait" method is invoked on the RPC.
 * This method will not block; it checks for interesting events that may have
 * occurred, but doesn't wait for them to occur.
 */
void
RamCloud::poll()
{
    // If we're not running in the dispatch thread, there's no need to do
    // anything (the dispatch thread will be polling continuously).
    if (clientContext->dispatch->isDispatchThread())
        clientContext->dispatch->poll();
}

/**
 * Split an indexlet into two disjoint indexlets at a specific key.
 * Check if the split already exists, in which case, just return.
 * Ask the original server to migrate the second indexlet resulting from the
 * split to another master having server id newOwner.
 * Alert newOwner that it should begin servicing requests on that indexlet.
 *
 * \param newOwner
 *      ServerId of the server that will own the second indexlet resulting
 *      from the split of the original indexlet at the end of the operation.
 * \param tableId
 *      Id of the table to which the index belongs.
 * \param indexId
 *      Id of the secondary key for which this index stores information.
 * \param splitKey
 *      Key used to partition the indexlet into two. Keys less than
 *      \a splitKey belong to one indexlet, keys greater than or equal to
 *      \a splitKey belong to the other.
 * \param splitKeyLength
 *      Length of splitKey in bytes.
 */
void
RamCloud::coordSplitAndMigrateIndexlet(
        ServerId newOwner, uint64_t tableId, uint8_t indexId,
        const void* splitKey, KeyLength splitKeyLength)
{
    CoordSplitAndMigrateIndexletRpc rpc(
        this, newOwner, tableId, indexId, splitKey, splitKeyLength);
    rpc.wait();
}

/**
 * Constructor for CoordSplitAndMigrateIndexletRpc:
 * initiates an RPC in the same way as #RamCloud::coordSplitAndMigrateIndexlet,
 * but returns once the RPC has been initiated, without waiting for it to
 * complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param newOwner
 *      ServerId of the server that will own the second indexlet resulting
 *      from the split of the original indexlet at the end of the operation.
 * \param tableId
 *      Id of the table to which the index belongs.
 * \param indexId
 *      Id of the secondary key for which this index stores information.
 * \param splitKey
 *      Key used to partition the indexlet into two. Keys less than
 *      \a splitKey belong to one indexlet, keys greater than or equal to
 *      \a splitKey belong to the other.
 * \param splitKeyLength
 *      Length of splitKey in bytes.
 */
CoordSplitAndMigrateIndexletRpc::CoordSplitAndMigrateIndexletRpc(
        RamCloud* ramcloud,
        ServerId newOwner, uint64_t tableId, uint8_t indexId,
        const void* splitKey, KeyLength splitKeyLength)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::CoordSplitAndMigrateIndexlet::Response))
{
    WireFormat::CoordSplitAndMigrateIndexlet::Request* reqHdr(
            allocHeader<WireFormat::CoordSplitAndMigrateIndexlet>());
    reqHdr->newOwnerId = newOwner.getId();
    reqHdr->tableId = tableId;
    reqHdr->indexId = indexId;
    reqHdr->splitKeyLength = splitKeyLength;
    request.appendCopy(splitKey, splitKeyLength);
    send();
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
    CreateTableRpc rpc(this, name, serverSpan);
    return rpc.wait();
}

/**
 * Constructor for CreateTableRpc: initiates an RPC in the same way as
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
CreateTableRpc::CreateTableRpc(RamCloud* ramcloud,
        const char* name, uint32_t serverSpan)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::CreateTable::Response))
{
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    WireFormat::CreateTable::Request* reqHdr(
            allocHeader<WireFormat::CreateTable>());
    reqHdr->nameLength = length;
    reqHdr->serverSpan = serverSpan;
    request.append(name, length);
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
CreateTableRpc::wait()
{
    waitInternal(context->dispatch);
    const WireFormat::CreateTable::Response* respHdr(
            getResponseHeader<WireFormat::CreateTable>());
    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
    return respHdr->tableId;
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
    DropTableRpc rpc(this, name);
    rpc.wait();
}

/**
 * Constructor for DropTableRpc: initiates an RPC in the same way as
 * #RamCloud::dropTable, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param name
 *      Name of the table to delete (NULL-terminated string).
 */
DropTableRpc::DropTableRpc(RamCloud* ramcloud, const char* name)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::DropTable::Response))
{
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    WireFormat::DropTable::Request* reqHdr(
            allocHeader<WireFormat::DropTable>());
    reqHdr->nameLength = length;
    request.append(name, length);
    send();
}

/**
 * Create a new index.
 *
 * Creating an index has no impact on the existing objects in the table.
 * For example, it's possible that some of the objects in the table already
 * contain secondary keys corresponding to the new index;
 * these objects will not automatically be indexed.
 * To make these objects accessible via the index, the application must
 * rewrite them (for example, by enumerating all of the objects in the table
 * and rewriting each object with a key for the new index).
 *
 * \param tableId
 *      Id of the table to which the index belongs.
 * \param indexId
 *      Id of the secondary keys corresponding to this index.
 *      Must be greater than 0. Id 0 is reserved for "primary key".
 * \param indexType
 *      Type of the keys corresponding to this index.
 *      Currently only string keys are supported, so this parameter is not yet
 *      used (i.e., caller can provide any value).
 * \param numIndexlets
 *      Number of indexlets to partition the index key space.
 *      This is only for performance testing and unit tests.
 *      Its value should always be 1 for real use.
 */
void
RamCloud::createIndex(uint64_t tableId, uint8_t indexId, uint8_t indexType,
        uint8_t numIndexlets)
{
    CreateIndexRpc rpc(this, tableId, indexId, indexType, numIndexlets);
    rpc.wait();
}

/**
 * Constructor for CreateIndexRpc: initiates an RPC in the same way as
 * #RamCloud::createIndex, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      Id of the table to which the index belongs.
 * \param indexId
 *      Id of the secondary keys corresponding to this index.
 *      Must be greater than 0. Id 0 is reserved for "primary key".
 * \param indexType
 *      Type of the keys corresponding to this index.
 *      Currently only string keys are supported, so this parameter is not yet
 *      used (i.e., caller can provide any value).
 * \param numIndexlets
 *      Number of indexlets to partition the index key space.
 *      This is only for performance testing, and value should always be 1 for
 *      real use.
 */
CreateIndexRpc::CreateIndexRpc(RamCloud* ramcloud, uint64_t tableId,
        uint8_t indexId, uint8_t indexType, uint8_t numIndexlets)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::CreateIndex::Response))
{
    WireFormat::CreateIndex::Request* reqHdr(
            allocHeader<WireFormat::CreateIndex>());
    reqHdr->tableId = tableId;
    reqHdr->indexId = indexId;
    reqHdr->indexType = indexType;
    reqHdr->numIndexlets =  numIndexlets;
    send();
}

/**
 * Delete an index.
 *
 * The index is deleted, along with any other information associated
 * with the index.  If the index does not currently exist,
 * then the operation returns successfully without doing anything.
 *
 * Deleting an index does not modify any of the objects in the table.
 * For example, any secondary keys related to the deleted index will
 * remain in objects. To eliminate those secondary keys, an application must
 * enumerate all of the objects in the table and rewrite them without the keys.
 *
 * \param tableId
 *      Id of the table to which the index belongs.
 * \param indexId
 *      Id of the secondary keys corresponding to this index.
 *      Must be greater than 0. Id 0 is reserved for "primary key".
 */
void
RamCloud::dropIndex(uint64_t tableId, uint8_t indexId)
{
    DropIndexRpc rpc(this, tableId, indexId);
    rpc.wait();
}

/**
 * Constructor for DropIndexRpc: initiates an RPC in the same way as
 * #RamCloud::dropIndex, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      Id of the table to which the index belongs
 * \param indexId
 *      Id of the secondary keys corresponding to this index.
 *      Must be greater than 0. Id 0 is reserved for "primary key".
 */
DropIndexRpc::DropIndexRpc(RamCloud* ramcloud, uint64_t tableId,
        uint8_t indexId)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::DropIndex::Response))
{
    WireFormat::DropIndex::Request* reqHdr(
            allocHeader<WireFormat::DropIndex>());
    reqHdr->tableId = tableId;
    reqHdr->indexId = indexId;
    send();
}

/**
 * This method provides the core of table enumeration. It is invoked
 * repeatedly to enumerate a table; each invocation returns the next
 * set of objects (from a particular tablet stored on a particular server)
 * and also provides information about where we are in the overall
 * enumeration, which is used in future invocations of this method.
 *
 * This method is meant to be called from TableEnumerator and should not
 * normally be used directly by applications.
 *
 * \param tableId
 *      The table being enumerated (return value from a previous call
 *      to getTableId) .
 * \param keysOnly
 *      False means that full objects are returned, containing both keys
 *      and data. True means that the returned objects have
 *      been truncated so that the object data (normally the last
 *      field of the object) is omitted. Note: the size field in the
 *      log record headers is unchanged, which means it does not
 *      exist corresponding to the length of the log record.
 * \param tabletFirstHash
 *      Where to continue enumeration. The caller should provide zero
 *       the initial call. On subsequent calls, the caller should pass
 *       the return value from the previous call.
 * \param[in,out] state
 *      Holds the state of enumeration; opaque to the caller.  On the
 *      initial call this Buffer should be empty. At the end of each
 *      call the contents are modified to hold the current state of
 *      the enumeration. The caller must return the new value each
 *      time this method is invoked.
 * \param[out] objects
 *      After a successful return, this buffer will contain zero or
 *      more objects from the requested tablet. If zero objects are
 *      returned, then there are no more objects remaining in the
 *      tablet. When this happens, the return value will be set to
 *      point to the next tablet, or will be set to zero if this is
 *      the end of the entire table.
 *
 * \return
 *       The return value is a key hash indicating where to continue
 *       enumeration (the starting key hash for the tablet where
 *       enumeration should continue); it must be passed to the next call
 *       to this method as the \a tabletFirstHash argument.  A zero
 *       return value, combined with no objects returned in \a objects,
 *       means that enumeration has finished.
 */
uint64_t
RamCloud::enumerateTable(uint64_t tableId, bool keysOnly,
        uint64_t tabletFirstHash, Buffer& state, Buffer& objects)
{
    EnumerateTableRpc rpc(this, tableId, keysOnly,
                            tabletFirstHash, state, objects);
    return rpc.wait(state);
}

/**
 * Constructor for EnumerateTableRpc: initiates an RPC in the same way as
 * #RamCloud::enumerateTable, but returns once the RPC has been initiated, without
 * waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      The table being enumerated (return value from a previous call
 *      to getTableId) .
 * \param keysOnly
 *      False means that full objects are returned, containing both keys
 *      and data. True means that the returned objects have
 *      been truncated so that the object data (normally the last
 *      field of the object) is omitted. Note: the size field in the
 *      log record headers is unchanged, which means it does not
 *      exist corresponding to the length of the log record.
 * \param tabletFirstHash
 *      Where to continue enumeration. The caller should provide zero
*       the initial call. On subsequent calls, the caller should pass
*       the return value from the previous call.
 * \param state
 *      Holds the state of enumeration; opaque to the caller.  On the
 *      initial call this Buffer should be empty. In subsequent calls
 *      this must contain the information returned by \c wait from
 *      the previous call.
 * \param[out] objects
 *      After a successful return, this buffer will contain zero or
 *      more objects from the requested tablet.
 */
EnumerateTableRpc::EnumerateTableRpc(RamCloud* ramcloud, uint64_t tableId,
        bool keysOnly, uint64_t tabletFirstHash, Buffer& state, Buffer& objects)
    : ObjectRpcWrapper(ramcloud->clientContext, tableId, tabletFirstHash,
            sizeof(WireFormat::Enumerate::Response), &objects)
{
    WireFormat::Enumerate::Request* reqHdr(
            allocHeader<WireFormat::Enumerate>());
    reqHdr->tableId = tableId;
    reqHdr->keysOnly = keysOnly;
    reqHdr->tabletFirstHash = tabletFirstHash;
    reqHdr->iteratorBytes = state.size();
    for (Buffer::Iterator it(&state); !it.isDone(); it.next())
        request.append(it.getData(), it.getLength());
    send();
}

/**
 * Wait for an enumerate RPC to complete, and return the same results as
 * #RamCloud::enumerate.
 *
 * \param[out] state
 *      Will be filled in with the current state of the enumeration as of
 *      this method's return.  Must be passed back to this class as the
 *      \a iter parameter to the constructor when retrieving the next
 *      objects.
 * \return
 *       The return value is a key hash indicating where to continue
 *       enumeration (the starting key hash for the tablet where
 *       enumeration should continue); it must be passed to the constructor
 *       as the \a tabletFirstHash argument when retrieving the next
 *       objects.  In addition, zero or more objects from the enumeration
 *       will be returned in the \a objects Buffer specified to the
 *       constructor.  A zero return value, combined with no objects
 *       returned in \a objects, means that all objects in the table have
 *       been enumerated.
 *
 */
uint64_t
EnumerateTableRpc::wait(Buffer& state)
{
    simpleWait(context);
    const WireFormat::Enumerate::Response* respHdr(
            getResponseHeader<WireFormat::Enumerate>());
    uint64_t result = respHdr->tabletFirstHash;

    // Copy iterator from response into nextIter buffer.
    uint32_t iteratorBytes = respHdr->iteratorBytes;
    state.reset();
    if (iteratorBytes != 0) {
        response->copy(
                downCast<uint32_t>(sizeof(*respHdr) + respHdr->payloadBytes),
                iteratorBytes, state.alloc(iteratorBytes));
    }

    // Truncate the front and back of the response buffer, leaving just the
    // objects (the response buffer is the \c objects argument from
    // the constructor).
    assert(response->size() == sizeof(*respHdr) +
            respHdr->iteratorBytes + respHdr->payloadBytes);
    response->truncateFront(sizeof(*respHdr));
    response->truncate(response->size() - respHdr->iteratorBytes);

    return result;
}

/**
 * Retrieve various metrics from a master server's log module.
 *
 * \param serviceLocator
 *      Selects the server, whose log metrics should be retrieved.
 * \param[out] logMetrics
 *      This protocol buffer is filled in with the server's log metrics.
 *
 * \throw TransportException
 *       Thrown if an unrecoverable error occurred while communicating with
 *       the target server.
 */
void
RamCloud::getLogMetrics(const char* serviceLocator,
        ProtoBuf::LogMetrics& logMetrics)
{
    GetLogMetricsRpc rpc(this, serviceLocator);
    rpc.wait(logMetrics);
}

/**
 * Constructor for GetLogMetricsRpc: initiates an RPC in the same way as
 * #RamCloud::getLogMetrics, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param serviceLocator
 *      Selects the server, whose configuration should be retrieved.
 */
GetLogMetricsRpc::GetLogMetricsRpc(RamCloud* ramcloud,
        const char* serviceLocator)
    : RpcWrapper(sizeof(WireFormat::GetLogMetrics::Response))
    , ramcloud(ramcloud)
{
    try {
        session = ramcloud->clientContext->transportManager->getSession(
                serviceLocator);
    } catch (const TransportException& e) {
        session = FailSession::get();
    }
    allocHeader<WireFormat::GetLogMetrics>();
    send();
}

/**
 * Wait for a getLogMetrics RPC to complete, and return the same results as
 * #RamCloud::getLogMetrics.
 *
 * \param[out] logMetrics
 *      This protocol buffer is filled in with the server's log metrics.
 *
 * \throw TransportException
 *       Thrown if an unrecoverable error occurred while communicating with
 *       the target server.
 */
void
GetLogMetricsRpc::wait(ProtoBuf::LogMetrics& logMetrics)
{
    waitInternal(ramcloud->clientContext->dispatch);
    if (getState() != RpcState::FINISHED) {
        throw TransportException(HERE);
    }
    const WireFormat::GetLogMetrics::Response* respHdr(
            getResponseHeader<WireFormat::GetLogMetrics>());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);

    ProtoBuf::parseFromResponse(response, sizeof(*respHdr),
        respHdr->logMetricsLength, &logMetrics);
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
RamCloud::getMetrics(uint64_t tableId, const void* key, uint16_t keyLength)
{
    GetMetricsRpc rpc(this, tableId, key, keyLength);
    return rpc.wait();
}

/**
 * Constructor for GetMetricsRpc: initiates an RPC in the same way as
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
GetMetricsRpc::GetMetricsRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength)
    : ObjectRpcWrapper(ramcloud->clientContext, tableId, key, keyLength,
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
GetMetricsRpc::wait()
{
    waitInternal(context->dispatch);
    const WireFormat::GetMetrics::Response* respHdr(
            getResponseHeader<WireFormat::GetMetrics>());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);

    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->messageLength == response->size());
    ServerMetrics metrics;
    metrics.load(*response);
    return metrics;
}

/**
 * Retrieve performance counters from a server identified by a service locator.
 *
 * \param serviceLocator
 *      Selects the server from which metrics should be retrieved.
 *
 * \return
 *       The performance metrics retrieved from the given server.
 *
 * \throw TransportException
 *       Thrown if an unrecoverable error occurred while communicating with
 *       the target server.
 */
ServerMetrics
RamCloud::getMetrics(const char* serviceLocator)
{
    GetMetricsLocatorRpc rpc(this, serviceLocator);
    return rpc.wait();
}

/**
 * Constructor for GetMetricsLocatorRpc: initiates an RPC in the same way as
 * #RamCloud::getMetrics, but returns once the RPC has been initiated, without
 * waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param serviceLocator
 *      Selects the server from which metrics should be retrieved.
 */
GetMetricsLocatorRpc::GetMetricsLocatorRpc(RamCloud* ramcloud,
        const char* serviceLocator)
    : RpcWrapper(sizeof(WireFormat::GetMetrics::Response))
    , ramcloud(ramcloud)
{
    try {
        session = ramcloud->clientContext->transportManager->getSession(
                serviceLocator);
    } catch (const TransportException& e) {
        session = FailSession::get();
    }
    allocHeader<WireFormat::GetMetrics>();
    send();
}

/**
 * Wait for a getMetrics RPC to complete, and return the same results as
 * #RamCloud::getMetrics.
 *
 * \return
 *       The performance metrics retrieved from the target server.
 *
 * \throw TransportException
 *       Thrown if an unrecoverable error occurred while communicating with
 *       the target server.
 */
ServerMetrics
GetMetricsLocatorRpc::wait()
{
    waitInternal(ramcloud->clientContext->dispatch);
    if (getState() != RpcState::FINISHED) {
        LOG(ERROR, "GetMetricsLocatorRpc call failed with status %d",
            getState());
        throw TransportException(HERE);
    }
    const WireFormat::GetMetrics::Response* respHdr(
            getResponseHeader<WireFormat::GetMetrics>());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);

    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->messageLength == response->size());
    ServerMetrics metrics;
    metrics.load(*response);
    return metrics;
}

/**
 * Retrieve a server's runtime configuration.
 *
 * \param serviceLocator
 *      Selects the server, whose configuration should be retrieved.
 * \param[out] serverConfig
 *      This protocol buffer is filled in with the server's configuration.
 *
 * \throw TransportException
 *       Thrown if an unrecoverable error occurred while communicating with
 *       the target server.
 */
void
RamCloud::getServerConfig(const char* serviceLocator,
        ProtoBuf::ServerConfig& serverConfig)
{
    GetServerConfigRpc rpc(this, serviceLocator);
    rpc.wait(serverConfig);
}

/**
 * Constructor for GetServerConfigRpc: initiates an RPC in the same way as
 * #RamCloud::getServerConfig, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param serviceLocator
 *      Selects the server, whose configuration should be retrieved.
 */
GetServerConfigRpc::GetServerConfigRpc(RamCloud* ramcloud,
        const char* serviceLocator)
    : RpcWrapper(sizeof(WireFormat::GetServerConfig::Response))
    , ramcloud(ramcloud)
{
    try {
        session = ramcloud->clientContext->transportManager->getSession(
                serviceLocator);
    } catch (const TransportException& e) {
        session = FailSession::get();
    }
    allocHeader<WireFormat::GetServerConfig>();
    send();
}

/**
 * Wait for a getServerConfig RPC to complete, and return the same results as
 * #RamCloud::getServerConfig.
 *
 * \param[out] serverConfig
 *      This protocol buffer is filled in with the server's configuration.
 *
 * \throw TransportException
 *       Thrown if an unrecoverable error occurred while communicating with
 *       the target server.
 */
void
GetServerConfigRpc::wait(ProtoBuf::ServerConfig& serverConfig)
{
    waitInternal(ramcloud->clientContext->dispatch);
    if (getState() != RpcState::FINISHED) {
        throw TransportException(HERE);
    }
    const WireFormat::GetServerConfig::Response* respHdr(
            getResponseHeader<WireFormat::GetServerConfig>());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);

    ProtoBuf::parseFromResponse(response, sizeof(*respHdr),
        respHdr->serverConfigLength, &serverConfig);
}

/**
 * Retrieve server statistics from a given server.
 *
 * \param serviceLocator
 *      Selects the server from which statistics should be retrieved.
 * \param[out] serverStats
 *      This protocol buffer is filled in with statistics about the server.
 *
 * \return
 *       The performance metrics retrieved from the given server.
 *
 * \throw TransportException
 *       Thrown if an unrecoverable error occurred while communicating with
 *       the target server.
 */
void
RamCloud::getServerStatistics(const char* serviceLocator,
        ProtoBuf::ServerStatistics& serverStats)
{
    GetServerStatisticsRpc rpc(this, serviceLocator);
    rpc.wait(serverStats);
}

/**
 * Constructor for GetServerStatisticsRpc: initiates an RPC in the same way as
 * #RamCloud::getServerStatistics, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param serviceLocator
 *      Selects the server from which server statistics should be retrieved.
 */
GetServerStatisticsRpc::GetServerStatisticsRpc(RamCloud* ramcloud,
        const char* serviceLocator)
    : RpcWrapper(sizeof(WireFormat::GetServerStatistics::Response))
    , ramcloud(ramcloud)
{
    try {
        session = ramcloud->clientContext->transportManager->getSession(
                serviceLocator);
    } catch (const TransportException& e) {
        session = FailSession::get();
    }
    allocHeader<WireFormat::GetServerStatistics>();
    send();
}

/**
 * Wait for a getServerStatistics RPC to complete, and return the same
 * results as #RamCloud::getServerStatistics.
 *
 * \param[out] serverStats
 *      This protocol buffer is filled in with statistics about the server.
 *
 * \throw TransportException
 *       Thrown if an unrecoverable error occurred while communicating with
 *       the target server.
 */
void
GetServerStatisticsRpc::wait(ProtoBuf::ServerStatistics& serverStats)
{
    waitInternal(ramcloud->clientContext->dispatch);
    if (getState() != RpcState::FINISHED) {
        throw TransportException(HERE);
    }
    const WireFormat::GetServerStatistics::Response* respHdr(
            getResponseHeader<WireFormat::GetServerStatistics>());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);

    ProtoBuf::parseFromResponse(response, sizeof(*respHdr),
        respHdr->serverStatsLength, &serverStats);
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
    GetTableIdRpc rpc(this, name);
    return rpc.wait();
}

/**
 * Constructor for GetTableIdRpc: initiates an RPC in the same way as
 * #RamCloud::getTableId, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param name
 *      Name of the desired table (NULL-terminated string).
 */
GetTableIdRpc::GetTableIdRpc(RamCloud* ramcloud,
        const char* name)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::GetTableId::Response))
{
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    WireFormat::GetTableId::Request* reqHdr(
            allocHeader<WireFormat::GetTableId>());
    reqHdr->nameLength = length;
    request.append(name, length);
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
GetTableIdRpc::wait()
{
    waitInternal(context->dispatch);
    const WireFormat::GetTableId::Response* respHdr(
            getResponseHeader<WireFormat::GetTableId>());
    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
    return respHdr->tableId;
}

/**
 * Atomically increment the value of an object whose contents are an
 * IEEE754 double precision 8-byte floating point value.  If the object does
 * not exist, it is created with an initial value of 0.0 before incrementing.
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
double
RamCloud::incrementDouble(uint64_t tableId, const void* key, uint16_t keyLength,
        double incrementValue, const RejectRules* rejectRules,
        uint64_t* version)
{
    IncrementDoubleRpc rpc(this, tableId, key, keyLength, incrementValue,
            rejectRules);
    return rpc.wait(version);
}

/**
 * Constructor for IncrementDoubleRpc: initiates an RPC in the same way as
 * #RamCloud::incrementDouble, but returns once the RPC has been initiated,
 * without waiting for it to complete.
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
IncrementDoubleRpc::IncrementDoubleRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength, double incrementValue,
        const RejectRules* rejectRules)
    : LinearizableObjectRpcWrapper(ramcloud, true, tableId, key, keyLength,
            sizeof(WireFormat::Increment::Response))
{
    WireFormat::Increment::Request* reqHdr(
            allocHeader<WireFormat::Increment>());
    reqHdr->tableId = tableId;
    reqHdr->keyLength = keyLength;
    reqHdr->incrementInt64 = 0;
    reqHdr->incrementDouble = incrementValue;
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    request.append(key, keyLength);
    fillLinearizabilityHeader<WireFormat::Increment::Request>(reqHdr);
    send();
}

/**
 * Wait for an incrementDouble RPC to complete, and return the same results as
 * #RamCloud::incrementDouble.
 *
 * \param[out] version
 *      If non-NULL, the current version number of the object is
 *      returned here.
 */
double
IncrementDoubleRpc::wait(uint64_t* version)
{
    waitInternal(context->dispatch);
    const WireFormat::Increment::Response* respHdr(
            getResponseHeader<WireFormat::Increment>());
    if (version != NULL)
        *version = respHdr->version;

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
    return respHdr->newValue.asDouble;
}

/**
 * Atomically increment the value of an object whose contents are an
 * 8-byte two's complement, little-endian integer.  If the object does
 * not exist, it is created with an initial value of 0 before incrementing.
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
RamCloud::incrementInt64(uint64_t tableId, const void* key, uint16_t keyLength,
        int64_t incrementValue, const RejectRules* rejectRules,
        uint64_t* version)
{
    IncrementInt64Rpc rpc(this, tableId, key, keyLength, incrementValue,
            rejectRules);
    return rpc.wait(version);
}

/**
 * Constructor for IncrementInt64Rpc: initiates an RPC in the same way as
 * #RamCloud::incrementInt64, but returns once the RPC has been initiated,
 * without waiting for it to complete.
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
IncrementInt64Rpc::IncrementInt64Rpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength, int64_t incrementValue,
        const RejectRules* rejectRules)
    : LinearizableObjectRpcWrapper(ramcloud, true, tableId, key, keyLength,
            sizeof(WireFormat::Increment::Response))
{
    WireFormat::Increment::Request* reqHdr(
            allocHeader<WireFormat::Increment>());
    reqHdr->tableId = tableId;
    reqHdr->keyLength = keyLength;
    reqHdr->incrementInt64 = incrementValue;
    reqHdr->incrementDouble = 0.0;
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    request.append(key, keyLength);
    fillLinearizabilityHeader<WireFormat::Increment::Request>(reqHdr);
    send();
}

/**
 * Wait for an incrementInt64 RPC to complete, and return the same results as
 * #RamCloud::incrementInt64.
 *
 * \param[out] version
 *      If non-NULL, the current version number of the object is
 *      returned here.
 */
int64_t
IncrementInt64Rpc::wait(uint64_t* version)
{
    waitInternal(context->dispatch);
    const WireFormat::Increment::Response* respHdr(
            getResponseHeader<WireFormat::Increment>());
    if (version != NULL)
        *version = respHdr->version;

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
    return respHdr->newValue.asInt64;
}

/**
 * Read objects in a table with given primary key hashes.
 *
 * An RPC will be sent to a single server S1. This request RPC will contain
 * all the key hashes, even though S1 may not be able to service them all.
 * S1 returns the objects corresponding to the key hashes it can, along with an
 * indication that can be used to calculate the next key hash to be fetched.
 * The indication is the number of hashes for which objects are being returned
 * or were not found.
 *
 * The next RPC would be initiated by the client by trimming the key hash
 * list according to the previous response.
 * This rpc will get sent to the server owning the new first key hash.
 * This could be S1 in case it couldn't fit all the objects in a single RPC
 * the first time it sent the response, or a different server S2 in case S1
 * didn't own these other objects.
 *
 * \param tableId
 *      Id of the table to read from.
 * \param numHashes
 *      Number of primary key hashes in the following buffer.
 * \param pKHashes
 *      Buffer of primary key hashes of objects desired in this request.
 *
 * \param[out] response
 *      Return all the objects matching the given primary key hashes
 *      along with their versions, in the format specified by
 *      WireFormat::ReadHashes::Response.
 * \param[out] numObjects
 *      Number of objects that are being returned.
 *      For each primary key hash in pKHashes, the operation could return:
 *      (a) One corresponding object, or
 *      (b) More than one corresponding objects if there are multiple objects
 *              with the same primary key hash and secondary key, or
 *      (c) No object if the object isn't on the server (where the query is
 *              currently being sent), or
 *      (d) No object if the server has appended enough data (objects) to the
 *              response rpc that it cannot fit any more objects.
 * \return
 *      Number of key hashes for which corresponding objects are being
 *      returned, or for which no matching objects were found.
 *      If this number is less than the total number of hashes in the
 *      original request, that means not all of those hashes were used.
 */
uint32_t
RamCloud::readHashes(uint64_t tableId, uint32_t numHashes, Buffer* pKHashes,
        Buffer* response, uint32_t* numObjects)
{
    ReadHashesRpc rpc(this, tableId, numHashes, pKHashes, response);
    return rpc.wait(numObjects);
}

/**
 * Constructor for ReadHashesRpc: initiates RPC(s) in the same way as
 * #RamCloud::ReadHashesRpc, but returns once first RPC has been initiated,
 * without waiting for any to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      Id of the table to read from.
 * \param numHashes
 *      Number of primary key hashes in the following buffer.
 * \param pKHashes
 *      Buffer of primary key hashes of objects desired in this request.
 *
 * \param[out] response
 *      Return all the objects matching the given primary key hashes
 *      along with their versions, in the format specified by
 *      WireFormat::ReadHashes::Response.
 */
ReadHashesRpc::ReadHashesRpc(RamCloud* ramcloud, uint64_t tableId,
        uint32_t numHashes, Buffer* pKHashes, Buffer* response)
    : ObjectRpcWrapper(ramcloud->clientContext, tableId,
            *(pKHashes->getStart<uint64_t>()),
            sizeof(WireFormat::ReadHashes::Response), response)
{
    WireFormat::ReadHashes::Request* reqHdr(
            allocHeader<WireFormat::ReadHashes>());
    reqHdr->tableId = tableId;
    reqHdr->numHashes = numHashes;
    request.append(pKHashes, 0, pKHashes->size());
    send();
}

/**
 * Wait for the RPCs to complete, and return the same results as
 * #RamCloud::ReadHashesRpc.
 *
 * \param[out] numObjects
 *      Number of objects that are being returned.
 *      For each primary key hash in pKHashes, the operation could return:
 *      (a) One corresponding object, or
 *      (b) More than one corresponding objects if there are multiple objects
 *              with the same primary key hash and secondary key, or
 *      (c) No object if the object isn't on the server (where the query is
 *              currently being sent), or
 *      (d) No object if the server has appended enough data (objects) to the
 *              response rpc that it cannot fit any more objects.
 * \return
 *      Number of key hashes for which corresponding objects are being
 *      returned, or for which no matching objects were found.
 */
uint32_t
ReadHashesRpc::wait(uint32_t* numObjects)
{
    simpleWait(context);
    const WireFormat::ReadHashes::Response* respHdr(
            getResponseHeader<WireFormat::ReadHashes>());
    *numObjects = respHdr->numObjects;
    return respHdr->numHashes;
}

/**
 * This RPC is similar to serverControl, except it directs the request
 * at the server storing a particular indexlet.  This RPC is used to
 * invoke a variety of miscellaneous operations on a server, such as
 * starting and stopping special timing mechanisms, dumping metrics,
 * and so on. Most of these operations are used only for testing. Each
 * operation is defined by a specific opcode (controlOp) and an arbitrary
 * chunk of input data. Not all operations require input data, and
 * different operations use the input data in different ways.
 * Each operation can also return an optional result of arbitrary size.
 *
 * \param tableId
 *      Unique identifier for a particular table. Used to select the
 *      server that will handle this operation.
 * \param indexId
 *      Id of an index within tableId.
 *      Must be greater than 0. Id 0 is reserved for "primary key".
 * \param key
 *      Secondary key within the index given by tableId and indexId. The
 *      request will be sent to the server responsible for this key in the
 *      selected index.
 * \param keyLength
 *      Length in bytes of key.
 * \param controlOp
 *      This defines the specific operation to be performed on the
 *      remote server.
 * \param inputData
 *      Input data, such as additional parameters, specific for the
 *      particular operation to be performed. Not all operations use
 *      this information.
 * \param inputLength
 *      Size in bytes of the contents for the inputData.
 * \param[out] outputData
 *      A buffer that contains the return results, if any, from execution of the
 *      control operation on the remote server.
 */
void
RamCloud::indexServerControl(uint64_t tableId, uint8_t indexId,
        const void* key, uint16_t keyLength, WireFormat::ControlOp controlOp,
        const void* inputData, uint32_t inputLength, Buffer* outputData)
{
    IndexServerControlRpc rpc(this, tableId, indexId, key, keyLength,
            controlOp, inputData, inputLength, outputData);
    rpc.wait();
}

/**
 * Constructor for IndexServerControlRpc: initiates an RPC in the same way as
 * #RamCloud::indexServerControl, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      Unique identifier for a particular table. Used to select the
 *      server that will handle this operation.
 * \param indexId
 *      Id of an index within tableId.
 *      Must be greater than 0. Id 0 is reserved for "primary key".
 * \param key
 *      Secondary key within the index given by tableId and indexId. The
 *      request will be sent to the server responsible for this key in the
 *      selected index.
 * \param keyLength
 *      Length in bytes of key.
 * \param controlOp
 *      This defines the specific operation to be performed on the
 *      remote server.
 * \param inputData
 *      Input data, such as additional parameters, specific for the
 *      particular operation to be performed. Not all operations use
 *      this information.
 * \param inputLength
 *      Size in bytes of the contents for the inputData.
 * \param[out] outputData
 *      A buffer that contains the return results, if any, from execution of the
 *      control operation on the remote server.
 */
IndexServerControlRpc::IndexServerControlRpc(RamCloud* ramcloud,
        uint64_t tableId, uint8_t indexId, const void* key, uint16_t keyLength,
        WireFormat::ControlOp controlOp, const void* inputData,
        uint32_t inputLength, Buffer* outputData)
    : IndexRpcWrapper(ramcloud, tableId, indexId, key, keyLength,
            sizeof(WireFormat::ServerControl::Response), outputData)
{
    WireFormat::ServerControl::Request* reqHdr(
            allocHeader<WireFormat::ServerControl>());

    reqHdr->type = WireFormat::ServerControl::INDEX;
    reqHdr->controlOp = controlOp;

    reqHdr->tableId = tableId;
    reqHdr->indexId = indexId;
    reqHdr->keyLength = keyLength;
    request.append(key, keyLength);
    reqHdr->inputLength = inputLength;
    request.append(inputData, inputLength);
    send();
}

/**
 * Waits for the RPC to complete, and returns the same results as
 * #RamCloud::indexServerControl.
 */
void
IndexServerControlRpc::wait()
{
    waitInternal(context->dispatch);
    const WireFormat::ServerControl::Response* respHdr(
            getResponseHeader<WireFormat::ServerControl>());
    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->outputLength == response->size());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
}

/**
 * Lookup objects with index keys corresponding to indexId in the
 * specified range and return primary key hashes for all of the objects
 * in the given range.
 *
 * An RPC will be sent to a single server S1. S1 returns
 * the primary key hashes for the secondary key range that it can,
 * along with the next secondary key + key hash that has to be fetched.
 *
 * The next RPC, if needed, would be initiated by the client by setting
 * firstKey in new request = nextKey from previous response, AND
 * firstAllowedKeyHash in new request = (nextKeyHash from previous resp) + 1.
 *
 * This rpc would get sent to the server owning that firstKey.
 * This could be S1 in case it couldn't fit all the key hashes in a single RPC
 * the first time it sent the response, or a different server S2 in case S1
 * didn't own the new key range.
 *
 * The caller can tell that the range is completely enumerated if the response
 * to an rpc has nextKeyLength = 0 (i.e., returns no nextKey) and
 * nextKeyHash = 0 (i.e., no nextKeyHash).
 *
 *
 * \param tableId
 *      Id of the table in which lookup is to be done.
 * \param indexId
 *      Id of the index to use for lookup.
 *      Must be greater than 0. Id 0 is reserved for "primary key".
 * \param firstKey
 *      Starting key for the key range in which keys are to be matched.
 *      The key range includes the firstKey.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param firstKeyLength
 *      Length in bytes of the firstKey.
 * \param firstAllowedKeyHash
 *      Smallest primary key hash value allowed for firstKey.
 *      This is normally 0. It has a non-zero value in second and later requests
 *      when multiple requests are needed to fetch data (primary key hashes)
 *      for the same secondary key.
 * \param lastKey
 *      Ending key for the key range in which keys are to be matched.
 *      The key range includes the lastKey.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param lastKeyLength
 *      Length in byes of the lastKey.
 * \param maxNumHashes
 *      Maximum number of hashes that the server is allowed to return
 *      in a single rpc.
 *
 * \param[out] responseBuffer
 *      Return buffer containing:
 *      1. The key hashes of the primary keys of all the objects
 *      that match the lookup query and can be returned in this response.
 *      2. Actual bytes of the next key to fetch, if any.
 *      This is the first nextKeyLength bytes of responseBuffer.
 *      (Results starting at nextKey + nextKeyHash couldn't be returned
 *      right now.)
 * \param[out] numHashes
 *      Return the number of objects that matched the lookup, for which
 *      the primary key hashes are being returned here.
 * \param[out] nextKeyLength
 *      Length of nextKey in bytes.
 * \param[out] nextKeyHash
 *      Results starting at nextKey + nextKeyHash couldn't be returned.
 *      Client can send another request according to this.
 */
void
RamCloud::lookupIndexKeys(uint64_t tableId, uint8_t indexId,
        const void* firstKey, uint16_t firstKeyLength,
        uint64_t firstAllowedKeyHash,
        const void* lastKey, uint16_t lastKeyLength,
        uint32_t maxNumHashes,
        Buffer* responseBuffer, uint32_t* numHashes,
        uint16_t* nextKeyLength, uint64_t* nextKeyHash)
{
    LookupIndexKeysRpc rpc(this, tableId, indexId,
            firstKey, firstKeyLength, firstAllowedKeyHash,
            lastKey, lastKeyLength, maxNumHashes, responseBuffer);
    rpc.wait(numHashes, nextKeyLength, nextKeyHash);
}

/**
 * Constructor for LookupIndexKeysRpc: initiates an RPC in the same way as
 * #RamCloud::LookupIndexKeysRpc, but returns once the RPC has been
 * initiated, without waiting for all to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      Id of the table in which lookup is to be done.
 * \param indexId
 *      Id of the index for which keys have to be compared.
 *      Must be greater than 0. Id 0 is reserved for "primary key".
 * \param firstKey
 *      Starting key for the key range in which keys are to be matched.
 *      The key range includes the firstKey.
 *      NULL value indicates lowest possible key.
 *      It does not necessarily have to be null terminated. The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param firstKeyLength
 *      Length in bytes of the firstKey.
 * \param firstAllowedKeyHash
 *      Smallest primary key hash value allowed for firstKey.
 * \param lastKey
 *      Ending key for the key range in which keys are to be matched.
 *      The key range includes the lastKey.
 *      NULL value indicates highest possible key.
 *      It does not necessarily have to be null terminated. The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param lastKeyLength
 *      Length in byes of the lastKey.
 * \param maxNumHashes
 *      Maximum number of hashes that the server is allowed to return
 *      in a single rpc.
 *
 * \param[out] responseBuffer
 *      Response buffer returned on wait().
 */
LookupIndexKeysRpc::LookupIndexKeysRpc(
        RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
        const void* firstKey, uint16_t firstKeyLength,
        uint64_t firstAllowedKeyHash,
        const void* lastKey, uint16_t lastKeyLength,
        uint32_t maxNumHashes, Buffer* responseBuffer)
    : IndexRpcWrapper(ramcloud, tableId, indexId, firstKey, firstKeyLength,
            sizeof(WireFormat::LookupIndexKeys::Response), responseBuffer)
{
    WireFormat::LookupIndexKeys::Request* reqHdr(
            allocHeader<WireFormat::LookupIndexKeys>());
    reqHdr->tableId = tableId;
    reqHdr->indexId = indexId;
    reqHdr->firstKeyLength = firstKeyLength;
    reqHdr->firstAllowedKeyHash = firstAllowedKeyHash;
    reqHdr->lastKeyLength = lastKeyLength;
    reqHdr->maxNumHashes = maxNumHashes;
    request.append(firstKey, firstKeyLength);
    request.append(lastKey, lastKeyLength);
    send();
}

/**
 * Handle the case where the RPC cannot be completed as the containing the index
 * key was not found.
 */
void
LookupIndexKeysRpc::indexNotFound()
{
    response->reset();
    WireFormat::LookupIndexKeys::Response* respHdr =
            response->emplaceAppend<WireFormat::LookupIndexKeys::Response>();
    respHdr->common.status = STATUS_OK;
    respHdr->numHashes = 0;
    respHdr->nextKeyLength = 0;
    respHdr->nextKeyHash = 0;
}

/**
 * Wait for a lookupIndexKeys RPC to complete, and return the same results as
 * #RamCloud::lookupIndexKeys.
 *
 * \param[out] numHashes
 *      Return the number of objects that matched the lookup, for which
 *      the primary key hashes are being returned here.
 * \param[out] nextKeyLength
 *      Length of nextKey in bytes.
 * \param[out] nextKeyHash
 *      Results starting at nextKey + nextKeyHash couldn't be returned.
 *      Client can send another request according to this.
 */
void
LookupIndexKeysRpc::wait(uint32_t* numHashes, uint16_t* nextKeyLength,
        uint64_t* nextKeyHash)
{
    simpleWait(context);

    const WireFormat::LookupIndexKeys::Response* respHdr(
            getResponseHeader<WireFormat::LookupIndexKeys>());
    *numHashes = respHdr->numHashes;
    *nextKeyLength = respHdr->nextKeyLength;
    *nextKeyHash = respHdr->nextKeyHash;
}

/**
 * Request that the master owning a particular tablet migrate it
 * to another designated master.
 *
 * \param tableId
 *      Identifier for the table to be migrated.
 * \param firstKeyHash
 *      First key hash of the tablet range to be migrated.
 * \param lastKeyHash
 *      Last key hash of the tablet range to be migrated.
 * \param newOwnerMasterId
 *      ServerId of the node to which the tablet should be migrated.
 */
void
RamCloud::migrateTablet(uint64_t tableId, uint64_t firstKeyHash,
        uint64_t lastKeyHash, ServerId newOwnerMasterId)
{
    MigrateTabletRpc rpc(this, tableId, firstKeyHash, lastKeyHash,
            newOwnerMasterId);
    rpc.wait();
}

/**
 * Constructor for MigrateTabletRpc: initiates an RPC in the same way as
 * #RamCloud::migrateTablet, but returns once the RPC has been initiated, without
 * waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      Identifier for the table to be migrated.
 * \param firstKeyHash
 *      First key hash of the tablet range to be migrated.
 * \param lastKeyHash
 *      Last key hash of the tablet range to be migrated.
 * \param newOwnerMasterId
 *      ServerId of the node to which the tablet should be migrated.
 */
MigrateTabletRpc::MigrateTabletRpc(RamCloud* ramcloud, uint64_t tableId,
        uint64_t firstKeyHash, uint64_t lastKeyHash,
        ServerId newOwnerMasterId)
    : ObjectRpcWrapper(ramcloud->clientContext, tableId, firstKeyHash,
            sizeof(WireFormat::MigrateTablet::Response))
{
    WireFormat::MigrateTablet::Request* reqHdr(
            allocHeader<WireFormat::MigrateTablet>());
    reqHdr->tableId = tableId;
    reqHdr->firstKeyHash = firstKeyHash;
    reqHdr->lastKeyHash = lastKeyHash;
    reqHdr->newOwnerMasterId = newOwnerMasterId.getId();
    send();
}

/**
 * Increment multiple objects. This method has two performance advantages over
 * calling RamCloud::increment separately for each object:
 * - If multiple objects are stored on a single server, this method
 *   issues a single RPC to increment all of them at once.
 * - If different objects are stored on different servers, this method
 *   issues multiple RPCs concurrently.
 *
 * \param requests
 *      Each element in this array describes one object to increment.
 * \param numRequests
 *      Number of valid entries in \c requests.
 */
void
RamCloud::multiIncrement(MultiIncrementObject* requests[], uint32_t numRequests)
{
    MultiIncrement request(this, requests, numRequests);
    request.wait();
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
    MultiRead request(this, requests, numRequests);
    request.wait();
}

/**
 * Remove multiple objects.
 * This method has two performance advantages over calling RamCloud::remove
 * separately for each object:
 * - If multiple objects are stored on a single server, this method
 *   issues a single RPC to fetch all of them at once.
 * - If different objects are stored on different servers, this method
 *   issues multiple RPCs concurrently.
 *
 * \param requests
 *      Each element in this array describes one object to remove.
 * \param numRequests
 *      Number of valid entries in \c requests.
 */
void
RamCloud::multiRemove(MultiRemoveObject* requests[], uint32_t numRequests)
{
    MultiRemove request(this, requests, numRequests);
    request.wait();
}

/**
 * Write multiple objects. This method has two performance advantages over
 * calling RamCloud::write separately for each object:
 * - If multiple objects belong on a single server, this method
 *   issues a single RPC to write all of them at once.
 * - If different objects belong to different servers, this method
 *   issues multiple RPCs concurrently.
 *
 * \param requests
 *      Each element in this array describes one object to write. The write
 *      operation's status and the object version are also returned here.
 * \param numRequests
 *      Number of valid entries in \c requests.
 */
void
RamCloud::multiWrite(MultiWriteObject* requests[], uint32_t numRequests)
{
    MultiWrite request(this, requests, numRequests);
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
 *      contents of the desired object - only the value portion of the object.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the read
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned here.
 */
void
RamCloud::read(uint64_t tableId, const void* key, uint16_t keyLength,
        Buffer* value, const RejectRules* rejectRules, uint64_t* version)
{
    ReadRpc rpc(this, tableId, key, keyLength, value, rejectRules);
    rpc.wait(version);
}

/**
 * Read the current contents of an object including the keys and the value.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Primary key for the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] value
 *      After a successful return, this ObjectBuffer will hold the
 *      contents of the desired object including the keys and the value.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the read
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned here.
 */
void
RamCloud::readKeysAndValue(uint64_t tableId, const void* key,
        uint16_t keyLength, ObjectBuffer* value,
        const RejectRules* rejectRules, uint64_t* version)
{
    ReadKeysAndValueRpc rpc(this, tableId, key, keyLength, value, rejectRules);
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
ReadRpc::ReadRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength, Buffer* value,
        const RejectRules* rejectRules)
    : ObjectRpcWrapper(ramcloud->clientContext, tableId, key, keyLength,
            sizeof(WireFormat::Read::Response), value)
{
    value->reset();
    WireFormat::Read::Request* reqHdr(allocHeader<WireFormat::Read>());
    reqHdr->tableId = tableId;
    reqHdr->keyLength = keyLength;
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    request.append(key, keyLength);
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
ReadRpc::wait(uint64_t* version)
{
    waitInternal(context->dispatch);
    const WireFormat::Read::Response* respHdr(
            getResponseHeader<WireFormat::Read>());
    if (version != NULL)
        *version = respHdr->version;

    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->length == response->size());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
}

/**
 * Constructor for ReadKeysAndValueRpc: initiates an RPC in the same way as
 * #RamCloud::read, but returns once the RPC has been initiated, without
 * waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param key
 *      Primary key for the object within tableId.
 *      It does not necessarily have to be null terminated.  The caller must
 *      ensure that the storage for this key is unchanged through the life of
 *      the RPC.
 * \param keyLength
 *      Size in bytes of the key.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object consistring of all the keys and the
 *      value.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the read
 *      should be aborted with an error.
 */
ReadKeysAndValueRpc::ReadKeysAndValueRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength, ObjectBuffer* value,
        const RejectRules* rejectRules)
    : ObjectRpcWrapper(ramcloud->clientContext, tableId, key, keyLength,
            sizeof(WireFormat::ReadKeysAndValue::Response), value)
{
    value->reset();
    WireFormat::ReadKeysAndValue::Request* reqHdr(allocHeader<
                            WireFormat::ReadKeysAndValue>());
    reqHdr->tableId = tableId;
    reqHdr->keyLength = keyLength;
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    request.append(key, keyLength);
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
ReadKeysAndValueRpc::wait(uint64_t* version)
{
    waitInternal(context->dispatch);
    const WireFormat::ReadKeysAndValue::Response* respHdr(
            getResponseHeader<WireFormat::ReadKeysAndValue>());
    if (version != NULL)
        *version = respHdr->version;

    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->length == response->size());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
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
RamCloud::remove(uint64_t tableId, const void* key, uint16_t keyLength,
        const RejectRules* rejectRules, uint64_t* version)
{
    RemoveRpc rpc(this, tableId, key, keyLength, rejectRules);
    rpc.wait(version);
}

/**
 * Constructor for RemoveRpc: initiates an RPC in the same way as
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
RemoveRpc::RemoveRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength, const RejectRules* rejectRules)
    : LinearizableObjectRpcWrapper(ramcloud, true, tableId, key, keyLength,
            sizeof(WireFormat::Remove::Response))
{
    WireFormat::Remove::Request* reqHdr(allocHeader<WireFormat::Remove>());
    reqHdr->tableId = tableId;
    reqHdr->keyLength = keyLength;
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    request.append(key, keyLength);
    fillLinearizabilityHeader<WireFormat::Remove::Request>(reqHdr);
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
RemoveRpc::wait(uint64_t* version)
{
    waitInternal(context->dispatch);
    const WireFormat::Remove::Response* respHdr(
            getResponseHeader<WireFormat::Remove>());
    if (version != NULL)
        *version = respHdr->version;

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
}

/**
 * This RPC is used to invoke a variety of miscellaneous operations
 * on a server, such as starting and stopping special timing
 * mechanisms, dumping metrics, and so on. Most of these operations
 * are used only for testing. Each operation is defined by a specific
 * opcode (controlOp) and an arbitrary chunk of input data.
 * Not all operations require input data, and different operations
 * use the input data in different ways.
 * Each operation can also return an optional result of arbitrary size.
 *
 * \param tableId
 *      Unique identifier for a particular table. Used to select the
 *      server that will handle this operation.
 * \param key
 *      Variable-length key that uniquely identifies a particular
 *      object in tableId. The RPC will be sent to the server
 *      that stores this object.
 * \param keyLength
 *      Size in bytes of the key. This is also used to locate that
 *      particular server.
 * \param controlOp
 *      This defines the specific operation to be performed on the
 *      remote server.
 * \param inputData
 *      Input data, such as additional parameters, specific for the
 *      particular operation to be performed. Not all operations use
 *      this information.
 * \param inputLength
 *      Size in bytes of the contents for the inputData.
 * \param[out] outputData
 *      A buffer that contains the return results, if any, from execution of the
 *      control operation on the remote server.
 */
void
RamCloud::objectServerControl(uint64_t tableId, const void* key,
        uint16_t keyLength, WireFormat::ControlOp controlOp,
        const void* inputData, uint32_t inputLength, Buffer* outputData)
{
    ObjectServerControlRpc rpc(this, tableId, key, keyLength, controlOp,
            inputData, inputLength, outputData);
    rpc.wait();
}

/**
 * Constructor for ObjectServerControlRpc: initiates an RPC in the same way as
 * #RamCloud::objectServerControl, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      Unique identifier for a particular table. Used to select the
 *      server that will handle this operation.
 * \param key
 *      Variable-length key that uniquely identifies a particular
 *      object in tableId. The RPC will be sent to the server
 *      that stores this object.
 * \param keyLength
 *      Size in bytes of the key. This is also used to locate that
 *      particular server.
 * \param controlOp
 *      This defines the specific operation to be performed on the
 *      remote server.
 * \param inputData
 *      Input data, such as additional parameters, specific for the
 *      particular operation to be performed. Not all operations use
 *      this information.
 * \param inputLength
 *      Size in bytes of the contents for the inputData.
 * \param[out] outputData
 *      A buffer that contains the return results, if any, from execution of the
 *      control operation on the remote server.
 */
ObjectServerControlRpc::ObjectServerControlRpc(RamCloud* ramcloud,
        uint64_t tableId, const void* key, uint16_t keyLength,
        WireFormat::ControlOp controlOp,
        const void* inputData, uint32_t inputLength, Buffer* outputData)
    : ObjectRpcWrapper(ramcloud->clientContext, tableId, key, keyLength,
            sizeof(WireFormat::ServerControl::Response), outputData)
{
    WireFormat::ServerControl::Request*
            reqHdr(allocHeader<WireFormat::ServerControl>());

    reqHdr->type = WireFormat::ServerControl::OBJECT;
    reqHdr->controlOp = controlOp;

    reqHdr->tableId = tableId;
    reqHdr->keyLength = keyLength;
    request.append(key, keyLength);
    reqHdr->inputLength = inputLength;
    request.append(inputData, inputLength);
    send();
}

/**
 * Waits for the RPC to complete, and returns the same results as
 * #RamCloud::objectServerControl.
 */
void
ObjectServerControlRpc::wait()
{
    waitInternal(context->dispatch);
    const WireFormat::ServerControl::Response* respHdr(
            getResponseHeader<WireFormat::ServerControl>());
    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->outputLength == response->size());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
}
/**
 * This RPCs used to invoke ServerControl on every server in the cluster; it
 * returns all of the responses. For more information on ServerControls, see
 * the objectServerControl method.
 *
 * \param controlOp
 *      This defines the specific to be performed on each server.
 * \param inputData
 *      Input data, such as additional parameters, specific for the
 *      particular operation to be performed. Not all operations use
 *      this information.
 * \param inputLength
 *      Size in bytes of the contents for the inputData.
 * \param[out] outputData
 *      A buffer that contains the raw response from the coordinator. See
 *      WireFormat::ServerControlAll::Response for more details.
 */
void
RamCloud::serverControlAll(WireFormat::ControlOp controlOp,
        const void* inputData, uint32_t inputLength,
        Buffer* outputData)
{
    ServerControlAllRpc rpc(this, controlOp,
            inputData, inputLength, outputData);
    rpc.wait();
}

/**
 * Constructor for ServerControlAllRpc: initiates an RPC in the same way as
 * #RamCloud::serverControlAll, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param controlOp
 *      This defines the specific operation to be performed on the
 *      remote servers.
 * \param inputData
 *      Input data, such as additional parameters, specific for the
 *      particular operation to be performed. Not all operations use
 *      this information.
 * \param inputLength
 *      Size in bytes of the contents for the inputData.
 * \param[out] outputData
 *      A buffer that contains the return results, if any, from execution of the
 *      control operation on the remote server.
 */
ServerControlAllRpc::ServerControlAllRpc(RamCloud* ramcloud,
        WireFormat::ControlOp controlOp,
        const void* inputData, uint32_t inputLength, Buffer* outputData)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::ServerControlAll::Response), outputData)
{
    WireFormat::ServerControlAll::Request*
            reqHdr(allocHeader<WireFormat::ServerControlAll>());

    reqHdr->controlOp = controlOp;
    reqHdr->inputLength = inputLength;
    request.append(inputData, inputLength);
    send();
}

/**
 * Wrapper used to invoke the LOG_MESSAGE ServerControl on every server in
 * the cluster
 *
 * \param level
 *      Which LogLevel the message should be added to.
 * \param[in] fmt
 *      A printf-style format string for the message.
 * \param[in] ...
 *      The arguments to the format string.
 */
void
RamCloud::logMessageAll(LogLevel level, const char* fmt, ...)
{
    Buffer toSend;
    Buffer outputData;

    va_list args;
    va_start(args, fmt);
    string toAdd = vformat(fmt, args);
    va_end(args);

    toSend.emplaceAppend<LogLevel>(level);
    toSend.append(toAdd.data(), (uint32_t) toAdd.size());

    ServerControlAllRpc rpc(this, WireFormat::LOG_MESSAGE,
        toSend.getStart<char>(), toSend.size(), &outputData);
    rpc.wait();
}

/**
 * Divide a tablet into two separate tablets.
 *
 * \param name
 *      Name of the table containing the tablet to be split.
 *     (NULL-terminated string).
 * \param splitKeyHash
 *      Dividing point for the new tablets. All key hashes less than
 *      this will belong to one tablet, and all key hashes >= this
 *      will belong to the other. If splitKeyHash already represents
 *      a split point between two tablets, then this operation will
 *      be a NOP and thus be idempotent.
 */
void
RamCloud::splitTablet(const char* name, uint64_t splitKeyHash)
{
    SplitTabletRpc rpc(this, name, splitKeyHash);
    rpc.wait();
}

/**
 * Constructor for SplitTabletRpc: initiates an RPC in the same way as
 * #RamCloud::splitTablet, but returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param name
 *      Name of the table containing the tablet to be split.
 *     (NULL-terminated string).
 * \param splitKeyHash
 *      Dividing point for the new tablets. All key hashes less than
 *      this will belong to one tablet, and all key hashes >= this
 *      will belong to the other.
 */
SplitTabletRpc::SplitTabletRpc(RamCloud* ramcloud,
        const char* name, uint64_t splitKeyHash)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::SplitTablet::Response))
{
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    WireFormat::SplitTablet::Request* reqHdr(
            allocHeader<WireFormat::SplitTablet>());
    reqHdr->nameLength = length;
    reqHdr->splitKeyHash = splitKeyHash;
    request.append(name, length);
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
RamCloud::testingFill(uint64_t tableId, const void* key, uint16_t keyLength,
        uint32_t numObjects, uint32_t objectSize)
{
    FillWithTestDataRpc rpc(this, tableId, key, keyLength, numObjects,
            objectSize);
    rpc.wait();
}

/**
 * Constructor for FillWithTestDataRpc: initiates an RPC in the same way as
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
FillWithTestDataRpc::FillWithTestDataRpc(RamCloud* ramcloud,
        uint64_t tableId, const void* key, uint16_t keyLength,
        uint32_t numObjects, uint32_t objectSize)
    : ObjectRpcWrapper(ramcloud->clientContext, tableId, key, keyLength,
            sizeof(WireFormat::FillWithTestData::Response))
{
    WireFormat::FillWithTestData::Request* reqHdr(
            allocHeader<WireFormat::FillWithTestData>());
    reqHdr->numObjects = numObjects;
    reqHdr->objectSize = objectSize;
    send();
}

/**
 * Get a value of runtime option field previously set on the coordinator.
 *
 * \param option
 *      String name corresponding to a member field in the RuntimeOptions
 *      class (e.g. "failRecoveryMasters") whose value should be returned
 *      in value buffer.
 * \param[out] value
 *      After a successful return, this Buffer will hold the value of the
 *      desired option.
 */
void
RamCloud::getRuntimeOption(const char* option, Buffer* value)
{
    GetRuntimeOptionRpc rpc(this, option, value);
    rpc.wait();
}

/**
 * Constructor for SetRuntimeOptionRpc: initiates an RPC in the same way as
 * #RamCloud::dropTable and returns once the RPC has been initiated,
 * without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param option
 *      String name corresponding to a member field in the RuntimeOptions
 *      class (e.g. "failRecoveryMasters") whose value should be returned
 *      in value buffer.
 * \param[out] value
 *      After a successful return, this Buffer will hold the value of the desired
 *      option.
 */
GetRuntimeOptionRpc::GetRuntimeOptionRpc(RamCloud* ramcloud, const char* option,
        Buffer* value)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::GetRuntimeOption::Response), value)
{
    value->reset();
    WireFormat::GetRuntimeOption::Request*
            reqHdr(allocHeader<WireFormat::GetRuntimeOption>());
    reqHdr->optionLength = downCast<uint32_t> (strlen(option) + 1);
    request.append(option, reqHdr->optionLength);
    send();
}

/**
 * Wait for the RPC to complete, and return the same results as
 * #RamCloud::read.
 */
void
GetRuntimeOptionRpc::wait()
{
    waitInternal(context->dispatch);
    const WireFormat::GetRuntimeOption::Response* respHdr(
            getResponseHeader<WireFormat::GetRuntimeOption>());
    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->valueLength == response->size());
    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
}

/**
 * Return the server id of the server that owns the specified key.
 * Used in testing scripts to associate particular processes with
 * their internal RAMCloud server id.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
uint64_t
RamCloud::testingGetServerId(uint64_t tableId,
        const void* key, uint16_t keyLength)
{
    KeyHash keyHash = Key::getHash(tableId, key, keyLength);
    return clientContext->objectFinder->lookupTablet(tableId,
            keyHash)->tablet.serverId.getId();
}

/**
 * Return the service locator of the server that owns the specified key.
 * Used in testing scripts to associate particular processes with
 * their internal RAMCloud server id.
 *
 * \throw TableDoesntExistException
 *      The coordinator has no record of the table.
 */
string
RamCloud::testingGetServiceLocator(uint64_t tableId,
        const void* key, uint16_t keyLength)
{
    KeyHash keyHash = Key::getHash(tableId, key, keyLength);
    return clientContext->objectFinder->lookupTablet(tableId, keyHash)->
                serviceLocator;
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
RamCloud::testingKill(uint64_t tableId, const void* key, uint16_t keyLength)
{
    KillRpc rpc(this, tableId, key, keyLength);
    clientContext->objectFinder->waitForTabletDown(tableId);
}

/**
 * Constructor for KillRpc: initiates an RPC in the same way as
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
KillRpc::KillRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength)
    : ObjectRpcWrapper(ramcloud->clientContext, tableId, key, keyLength,
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
RamCloud::setRuntimeOption(const char* option, const char* value)
{
    SetRuntimeOptionRpc rpc(this, option, value);
    rpc.wait();
}

/**
 * Constructor for SetRuntimeOptionRpc: initiates an RPC in the same way as
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
SetRuntimeOptionRpc::SetRuntimeOptionRpc(RamCloud* ramcloud,
        const char* option, const char* value)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::SetRuntimeOption::Response))
{
    WireFormat::SetRuntimeOption::Request* reqHdr(
            allocHeader<WireFormat::SetRuntimeOption>());
    reqHdr->optionLength = downCast<uint32_t>(strlen(option) + 1);
    reqHdr->valueLength = downCast<uint32_t>(strlen(value) + 1);
    request.append(option, reqHdr->optionLength);
    request.append(value, reqHdr->valueLength);
    send();
}

/**
 * Block and query coordinator until all tablets have normal status
 * (that is, no tablet is under recovery).
 */
void
RamCloud::testingWaitForAllTabletsNormal(uint64_t tableId, uint64_t timeoutNs)
{
    clientContext->objectFinder->waitForAllTabletsNormal(tableId, timeoutNs);
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
RamCloud::write(uint64_t tableId, const void* key, uint16_t keyLength,
        const void* buf, uint32_t length, const RejectRules* rejectRules,
        uint64_t* version, bool async)
{
    WriteRpc rpc(this, tableId, key, keyLength, buf, length, rejectRules,
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
RamCloud::write(uint64_t tableId, const void* key, uint16_t keyLength,
        const char* value, const RejectRules* rejectRules, uint64_t* version,
        bool async)
{
    uint32_t valueLength =
            (value == NULL) ? 0 : downCast<uint32_t>(strlen(value));

    WriteRpc rpc(this, tableId, key, keyLength, value, valueLength,
                    rejectRules, async);
    rpc.wait(version);
}

/**
 * Replace the value of a given object, or create a new object if none
 * previously existed. This form of the method allows multiple keys to be
 * specified for the object
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param numKeys
 *      Number of keys in the object.  If is not >= 1, then behavior
 *      is undefined. A value of 1 indicates the presence of only the
 *      primary key
 * \param keyList
 *      List of keys and corresponding key lengths. The first entry should
 *      correspond to the primary key and its length. If this argument is
 *      NULL, then behavior is undefined. RamCloud currently uses a dense
 *      representation of key lengths. If a client does not want to write
 *      key_i in an object, keyList[i]->key should be NULL. The library also
 *      expects that if keyList[j]->key is NOT NULL and keyList[j]->keyLength
 *      is 0, then key string is NULL terminated and the length is computed.
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
RamCloud::write(uint64_t tableId, uint8_t numKeys, KeyInfo *keyList,
        const void* buf, uint32_t length, const RejectRules* rejectRules,
        uint64_t* version, bool async)
{
    WriteRpc rpc(this, tableId, numKeys, keyList, buf, length, rejectRules,
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
 * \param numKeys
 *      Number of keys in the object.  If is not >= 1, then behavior
 *      is undefined. A value of 1 indicates the presence of only the
 *      primary key
 * \param keyList
 *      List of keys and corresponding key lengths. The first entry should
 *      correspond to the primary key and its length. If this argument is
 *      NULL, then behavior is undefined. RamCloud currently uses a dense
 *      representation of key lengths. If a client does not want to write
 *      key_i in an object, keyList[i]->key should be NULL. The library also
 *      expects that if keyList[j]->key exists and keyList[j]->keyLength
 *      is 0, then key string is NULL terminated and the length is computed.
 * \param value
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
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
RamCloud::write(uint64_t tableId, uint8_t numKeys, KeyInfo *keyList,
        const char* value, const RejectRules* rejectRules, uint64_t* version,
        bool async)
{
    uint32_t valueLength =
            (value == NULL) ? 0 : downCast<uint32_t>(strlen(value));
    WriteRpc rpc(this, tableId, numKeys, keyList, value,
            valueLength, rejectRules, async);
    rpc.wait(version);
}

/**
 * Constructor for WriteRpc: initiates an RPC in the same way as
 * #RamCloud::write, but returns once the RPC has been initiated, without
 * waiting for it to complete. This for the constructor is used when only
 * the primary key is being specified.
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
 *      Length of primary key in bytes
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
WriteRpc::WriteRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength, const void* buf, uint32_t length,
        const RejectRules* rejectRules, bool async)
    : LinearizableObjectRpcWrapper(ramcloud, true, tableId, key,
            keyLength, sizeof(WireFormat::Write::Response))
{
    WireFormat::Write::Request* reqHdr(allocHeader<WireFormat::Write>());
    reqHdr->tableId = tableId;

    uint32_t totalLength = 0;
    uint16_t currentKeyLength = 0;
    if (keyLength)
        currentKeyLength = keyLength;
    else
        currentKeyLength = static_cast<uint16_t>(strlen(
                               static_cast<const char *>(key)));

    Key primaryKey(tableId, key, currentKeyLength);
    Object::appendKeysAndValueToBuffer(primaryKey, buf, length,
                                       &request, false, &totalLength);

    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    reqHdr->async = async;
    reqHdr->length = totalLength;

    fillLinearizabilityHeader<WireFormat::Write::Request>(reqHdr);

    send();
}

/**
 * Constructor for WriteRpc: initiates an RPC in the same way as
 * #RamCloud::write, but returns once the RPC has been initiated, without
 * waiting for it to complete. This form of the constructor supports
 * multiple keys.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to getTableId).
 * \param numKeys
 *      Number of keys in the object. Defaults to 1 corresponding to the
 *      primary key. If is not >= 1, then behavior is undefined.
 * \param keyList
 *      List of keys and corresponding key lengths. The first entry should
 *      correspond to the primary key and its length. If the number of keys
 *      is > 1, this SHOULD NOT be NULL. Defaults to NULL in which case this
 *      object has only the primary key.
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
WriteRpc::WriteRpc(RamCloud* ramcloud, uint64_t tableId,
        uint8_t numKeys, KeyInfo *keyList, const void* buf, uint32_t length,
        const RejectRules* rejectRules, bool async)
    : LinearizableObjectRpcWrapper(ramcloud, true, tableId,
            keyList[0].key, keyList[0].keyLength,
            sizeof(WireFormat::Write::Response))
{
    WireFormat::Write::Request* reqHdr(allocHeader<WireFormat::Write>());
    reqHdr->tableId = tableId;

    uint32_t totalLength = 0;
    // invoke the Object constructor and append keysAndValue to the
    // request buffer
    Object::appendKeysAndValueToBuffer(tableId, numKeys, keyList,
                    buf, length, &request, &totalLength);
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    reqHdr->async = async;
    reqHdr->length = totalLength;

    fillLinearizabilityHeader<WireFormat::Write::Request>(reqHdr);

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
WriteRpc::wait(uint64_t* version)
{
    waitInternal(context->dispatch);
    const WireFormat::Write::Response* respHdr(
            getResponseHeader<WireFormat::Write>());

    if (version != NULL)
        *version = respHdr->version;

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
}

}  // namespace RAMCloud
