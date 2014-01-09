/* Copyright (c) 2010-2013 Stanford University
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
#include "FailSession.h"
#include "MasterClient.h"
#include "MultiRead.h"
#include "MultiRemove.h"
#include "MultiWrite.h"
#include "ProtoBuf.h"
#include "ShortMacros.h"

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
    , realClientContext()
    , clientContext(realClientContext.construct(false))
    , status(STATUS_OK)
    , objectFinder(clientContext)
{
    clientContext->coordinatorSession->setLocation(locator,
            clusterName);
}

/**
 * An alternate constructor that inherits an already created context. This is
 * useful for testing and for client programs that mess with the context
 * (which should be discouraged).
 */
RamCloud::RamCloud(Context* context, const char* locator,
        const char* clusterName)
    : coordinatorLocator(locator)
    , realClientContext()
    , clientContext(context)
    , status(STATUS_OK)
    , objectFinder(clientContext)
{
    clientContext->coordinatorSession->setLocation(locator,
            clusterName);
}

/**
 * Destructor of Ramcloud
 **/

RamCloud::~RamCloud()
{
    realClientContext.destroy();
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
DropTableRpc::DropTableRpc(RamCloud* ramcloud,
        const char* name)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::DropTable::Response))
{
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    WireFormat::DropTable::Request* reqHdr(
            allocHeader<WireFormat::DropTable>());
    reqHdr->nameLength = length;
    memcpy(new(&request, APPEND) char[length], name, length);
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
    : ObjectRpcWrapper(ramcloud, tableId, tabletFirstHash,
            sizeof(WireFormat::Enumerate::Response), &objects)
{
    WireFormat::Enumerate::Request* reqHdr(
            allocHeader<WireFormat::Enumerate>());
    reqHdr->tableId = tableId;
    reqHdr->keysOnly = keysOnly;
    reqHdr->tabletFirstHash = tabletFirstHash;
    reqHdr->iteratorBytes = state.getTotalLength();
    for (Buffer::Iterator it(state); !it.isDone(); it.next())
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
    simpleWait(ramcloud->clientContext->dispatch);
    const WireFormat::Enumerate::Response* respHdr(
            getResponseHeader<WireFormat::Enumerate>());
    uint64_t result = respHdr->tabletFirstHash;

    // Copy iterator from response into nextIter buffer.
    uint32_t iteratorBytes = respHdr->iteratorBytes;
    state.reset();
    response->copy(downCast<uint32_t>(sizeof(*respHdr) + respHdr->payloadBytes),
            iteratorBytes, new(&state, APPEND) char[iteratorBytes]);

    // Truncate the front and back of the response buffer, leaving just the
    // objects (the response buffer is the \c objects argument from
    // the constructor).
    assert(response->getTotalLength() == sizeof(*respHdr) +
            respHdr->iteratorBytes + respHdr->payloadBytes);
    response->truncateFront(sizeof(*respHdr));
    response->truncateEnd(respHdr->iteratorBytes);

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
GetMetricsRpc::wait()
{
    waitInternal(ramcloud->clientContext->dispatch);
    const WireFormat::GetMetrics::Response* respHdr(
            getResponseHeader<WireFormat::GetMetrics>());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);

    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->messageLength == response->getTotalLength());
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
    assert(respHdr->messageLength == response->getTotalLength());
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
            sizeof(WireFormat::CreateTable::Response))
{
    uint32_t length = downCast<uint32_t>(strlen(name) + 1);
    WireFormat::GetTableId::Request* reqHdr(
            allocHeader<WireFormat::GetTableId>());
    reqHdr->nameLength = length;
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
RamCloud::increment(uint64_t tableId, const void* key, uint16_t keyLength,
        int64_t incrementValue, const RejectRules* rejectRules,
        uint64_t* version)
{
    IncrementRpc rpc(this, tableId, key, keyLength, incrementValue,
            rejectRules);
    return rpc.wait(version);
}

/**
 * Constructor for IncrementRpc: initiates an RPC in the same way as
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
IncrementRpc::IncrementRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength, int64_t incrementValue,
        const RejectRules* rejectRules)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::Increment::Response))
{
    WireFormat::Increment::Request* reqHdr(
            allocHeader<WireFormat::Increment>());
    reqHdr->tableId = tableId;
    reqHdr->keyLength = keyLength;
    reqHdr->incrementValue = incrementValue;
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    request.append(key, keyLength);
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
IncrementRpc::wait(uint64_t* version)
{
    waitInternal(ramcloud->clientContext->dispatch);
    const WireFormat::Increment::Response* respHdr(
            getResponseHeader<WireFormat::Increment>());
    if (version != NULL)
        *version = respHdr->version;

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
    return respHdr->newValue;
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
    : ObjectRpcWrapper(ramcloud, tableId, firstKeyHash,
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
 * Ask the coordinator to broadcast a quiesce request to all backups.  When
 * this method returns, all backups will have flushed active segment replicas
 * to disk.  This is used primarily during recovery testing: it allows more
 * accurate performance measurements.
 */
void
RamCloud::quiesce()
{
    QuiesceRpc rpc(this);
    rpc.wait();
}

/**
 * Constructor for HintServerCrashedRpc: initiates an RPC in the same way as
 * #RamCloud::hintServerCrashed, but returns once the RPC has been
 * initiated, without waiting for it to complete.
 *
 * \param ramcloud
 *      The RAMCloud object that governs this RPC.
 */
QuiesceRpc::QuiesceRpc(RamCloud* ramcloud)
    : CoordinatorRpcWrapper(ramcloud->clientContext,
            sizeof(WireFormat::BackupQuiesce::Response))
{
    WireFormat::BackupQuiesce::Request* reqHdr(
            allocHeader<WireFormat::BackupQuiesce>(ServerId(0)));
    // By default this RPC is sent to the backup service; retarget it
    // for the coordinator service (which will forward it on to all
    // backups).
    reqHdr->common.service = WireFormat::COORDINATOR_SERVICE;
    send();
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
RamCloud::read(uint64_t tableId, const void* key, uint16_t keyLength,
                   Buffer* value, const RejectRules* rejectRules,
                   uint64_t* version)
{
    ReadRpc rpc(this, tableId, key, keyLength, value, rejectRules);
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
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
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
    waitInternal(ramcloud->clientContext->dispatch);
    const WireFormat::Read::Response* respHdr(
            getResponseHeader<WireFormat::Read>());
    if (version != NULL)
        *version = respHdr->version;

    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->length == response->getTotalLength());

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
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::Remove::Response))
{
    WireFormat::Remove::Request* reqHdr(allocHeader<WireFormat::Remove>());
    reqHdr->tableId = tableId;
    reqHdr->keyLength = keyLength;
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    request.append(key, keyLength);
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
    waitInternal(ramcloud->clientContext->dispatch);
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
RamCloud::serverControl(uint64_t tableId, const void* key, uint16_t keyLength,
            WireFormat::ControlOp controlOp,
            const void* inputData, uint32_t inputLength, Buffer* outputData){
    ServerControlRpc rpc(this, tableId, key, keyLength, controlOp,
            inputData, inputLength, outputData);
    rpc.wait();
}

/**
 * Constructor for ServerControlRpc: initiates an RPC in the same way as
 * #RamCloud::serverControl, but returns once the RPC has been initiated,
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
ServerControlRpc::ServerControlRpc(RamCloud* ramcloud, uint64_t tableId,
            const void* key, uint16_t keyLength,
            WireFormat::ControlOp controlOp,
            const void* inputData, uint32_t inputLength, Buffer* outputData)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::ServerControl::Response), outputData)
{
    outputData->reset();
    WireFormat::ServerControl::Request*
                               reqHdr(allocHeader<WireFormat::ServerControl>());
    reqHdr->inputLength = inputLength;
    reqHdr->controlOp = controlOp;
    request.append(inputData, inputLength);
    send();
}

/**
 * Waits for the RPC to complete, and returns the same results as
 * #RamCloud::serverControl.
 */
void
ServerControlRpc::wait()
{
    waitInternal(ramcloud->clientContext->dispatch);
    const WireFormat::ServerControl::Response* respHdr(
            getResponseHeader<WireFormat::ServerControl>());
    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    response->truncateFront(sizeof(*respHdr));
    assert(respHdr->outputLength == response->getTotalLength());

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
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
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
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
 *      After a successful return, this Buffer will hold the value of the desired
 *      option.
 */
void
RamCloud::getRuntimeOption(const char* option, Buffer* value){
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
                            sizeof(WireFormat::GetRuntimeOption::Response),
                            value)
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
    assert(respHdr->valueLength == response->getTotalLength());
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
    return objectFinder.lookupTablet(tableId, keyHash).server_id();
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
    return objectFinder.lookupTablet(tableId, keyHash).service_locator();
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
    objectFinder.waitForTabletDown();
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
RamCloud::testingWaitForAllTabletsNormal(uint64_t timeoutNs)
{
    objectFinder.waitForAllTabletsNormal(timeoutNs);
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
    WriteRpc rpc(this, tableId, key, keyLength, value,
            downCast<uint32_t>(strlen(value)), rejectRules, async);
    rpc.wait(version);
}

/**
 * Constructor for WriteRpc: initiates an RPC in the same way as
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
WriteRpc::WriteRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength, const void* buf, uint32_t length,
        const RejectRules* rejectRules, bool async)
    : ObjectRpcWrapper(ramcloud, tableId, key, keyLength,
            sizeof(WireFormat::Write::Response))
{
    WireFormat::Write::Request* reqHdr(allocHeader<WireFormat::Write>());
    reqHdr->tableId = tableId;
    reqHdr->keyLength = keyLength;
    reqHdr->length = length;
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    reqHdr->async = async;
    request.append(key, keyLength);
    request.append(buf, length);
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
    waitInternal(ramcloud->clientContext->dispatch);
    const WireFormat::Write::Response* respHdr(
            getResponseHeader<WireFormat::Write>());
    if (version != NULL)
        *version = respHdr->version;

    if (respHdr->common.status != STATUS_OK)
        ClientException::throwException(HERE, respHdr->common.status);
}

}  // namespace RAMCloud
