/* Copyright (c) 2010-2014 Stanford University
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

#ifndef RAMCLOUD_RAMCLOUD_H
#define RAMCLOUD_RAMCLOUD_H

#include "Common.h"
#include "CoordinatorClient.h"
#include "IndexRpcWrapper.h"
#include "MasterClient.h"
#include "ObjectBuffer.h"
#include "ObjectFinder.h"
#include "ObjectRpcWrapper.h"
#include "ServerMetrics.h"

#include "LogMetrics.pb.h"
#include "ServerConfig.pb.h"
#include "ServerStatistics.pb.h"

namespace RAMCloud {
class MultiIncrementObject;
class MultiReadObject;
class MultiRemoveObject;
class MultiWriteObject;

/**
 * This structure describes a key (primary or secondary) and its length.
 * This will be used by clients issuing write RPCs to specify possibly
 * many secondary keys to be included in the objects to be written.
 */
struct KeyInfo
{
    const void *key;        // pimary or secondary key. A NULL value here
                            // indicates absence of a key. Note that this
                            // cannot be true for primary key.
    uint16_t keyLength;     // length of the corresponding key. A 0 value
                            // here means that key is a NULL terminated
                            // string and the actual length is computed
                            // on demand.
};

/**
 * The RamCloud class provides the primary interface used by applications to
 * access a RAMCloud cluster.
 *
 * Each RamCloud object provides access to a particular RAMCloud cluster;
 * all of the RAMCloud RPC requests appear as methods on this object.
 *
 * In multi-threaded clients there must be a separate RamCloud object for
 * each thread; as of 5/2012 these objects are not thread-safe.
 */
class RamCloud {
  public:
    uint64_t createTable(const char* name, uint32_t serverSpan = 1);
    void dropTable(const char* name);
    void createIndex(uint64_t tableId, uint8_t indexId, uint8_t indexType,
                     uint8_t numIndexlets = 1);
    void dropIndex(uint64_t tableId, uint8_t indexId);
    uint64_t enumerateTable(uint64_t tableId, bool keysOnly,
         uint64_t tabletFirstHash, Buffer& state, Buffer& objects);
    void getLogMetrics(const char* serviceLocator,
                       ProtoBuf::LogMetrics& logMetrics);
    ServerMetrics getMetrics(uint64_t tableId, const void* key,
            uint16_t keyLength);
    ServerMetrics getMetrics(const char* serviceLocator);
    void getRuntimeOption(const char* option, Buffer* value);
    void getServerConfig(const char* serviceLocator,
            ProtoBuf::ServerConfig& serverConfig);
    void getServerStatistics(const char* serviceLocator,
            ProtoBuf::ServerStatistics& serverStats);
    string* getServiceLocator();
    uint64_t getTableId(const char* name);
    double incrementDouble(uint64_t tableId,
            const void* key, uint16_t keyLength,
            double incrementValue, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL);
    int64_t incrementInt64(uint64_t tableId,
            const void* key, uint16_t keyLength,
            int64_t incrementValue, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL);
    uint32_t indexedRead(uint64_t tableId, uint32_t numHashes,
            Buffer* pKHashes, uint8_t indexId,
            const void* firstKey, uint16_t firstKeyLength,
            const void* lastKey, uint16_t lastKeyLength,
            Buffer* response, uint32_t* numObjects);
    void indexServerControl(uint64_t tableId, uint8_t indexId,
            const void* key, uint16_t keyLength,
            WireFormat::ControlOp controlOp,
            const void* inputData, uint32_t inputLength, Buffer* outputData);
    void lookupIndexKeys(uint64_t tableId, uint8_t indexId,
            const void* firstKey, uint16_t firstKeyLength,
            uint64_t firstAllowedKeyHash,
            const void* lastKey, uint16_t lastKeyLength,
            uint32_t maxNumHashes,
            Buffer* responseBuffer,
            uint32_t* numHashes, uint16_t* nextKeyLength,
            uint64_t* nextKeyHash);
    void migrateTablet(uint64_t tableId, uint64_t firstKeyHash,
            uint64_t lastKeyHash, ServerId newOwnerMasterId);
    void multiIncrement(MultiIncrementObject* requests[], uint32_t numRequests);
    void multiRead(MultiReadObject* requests[], uint32_t numRequests);
    void multiRemove(MultiRemoveObject* requests[], uint32_t numRequests);
    void multiWrite(MultiWriteObject* requests[], uint32_t numRequests);
    void objectServerControl(uint64_t tableId, const void* key,
            uint16_t keyLength, WireFormat::ControlOp controlOp,
            const void* inputData = NULL, uint32_t inputLength = 0,
            Buffer* outputData = NULL);
    void quiesce();
    void read(uint64_t tableId, const void* key, uint16_t keyLength,
            Buffer* value, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL);
    void readKeysAndValue(uint64_t tableId, const void* key, uint16_t keyLength,
            ObjectBuffer* value, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL);
    void remove(uint64_t tableId, const void* key, uint16_t keyLength,
            const RejectRules* rejectRules = NULL, uint64_t* version = NULL);
    void serverControlAll(WireFormat::ControlOp controlOp,
            const void* inputData = NULL, uint32_t inputLength = 0,
            Buffer* outputData = NULL);
    void splitTablet(const char* name, uint64_t splitKeyHash);
    void testingFill(uint64_t tableId, const void* key, uint16_t keyLength,
            uint32_t numObjects, uint32_t objectSize);
    uint64_t testingGetServerId(uint64_t tableId, const void* key,
            uint16_t keyLength);
    string testingGetServiceLocator(uint64_t tableId, const void* key,
            uint16_t keyLength);
    void testingKill(uint64_t tableId, const void* key, uint16_t keyLength);
    void setRuntimeOption(const char* option, const char* value);
    void testingWaitForAllTabletsNormal(uint64_t tableId,
                                        uint64_t timeoutNs = ~0lu);
    void write(uint64_t tableId, const void* key, uint16_t keyLength,
                const void* buf, uint32_t length,
                const RejectRules* rejectRules = NULL, uint64_t* version = NULL,
                bool async = false);
    void write(uint64_t tableId, const void* key, uint16_t keyLength,
            const char* value, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL, bool async = false);
    void write(uint64_t tableId, uint8_t numKeys, KeyInfo *keyInfo,
                const void* buf, uint32_t length,
                const RejectRules* rejectRules = NULL, uint64_t* version = NULL,
                bool async = false);
    void write(uint64_t tableId, uint8_t numKeys, KeyInfo *keyInfo,
            const char* value, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL, bool async = false);

    void poll();
    explicit RamCloud(const char* serviceLocator,
            const char* clusterName = "main");
    RamCloud(Context* context, const char* serviceLocator,
            const char* clusterName = "main");
    virtual ~RamCloud();

  PRIVATE:
    /**
     * Service locator for the cluster coordinator.
     */
    string coordinatorLocator;

    /**
     * Usually, RamCloud objects create a new context in which to run. This is
     * the location where that context is stored.
     */
    Tub<Context> realClientContext;

  public:
    /**
     * This usually refers to realClientContext. For testing purposes and
     * clients that want to provide their own context that they've mucked with,
     * this refers to an externally defined context.
     */
    Context* clientContext;

    /**
     * Status returned from the most recent RPC.  Warning: as of 7/2012 this
     * field is not properly set.
     */
    Status status;

  public: // public for now to make administrative calls from clients
    ObjectFinder objectFinder;

  private:
    DISALLOW_COPY_AND_ASSIGN(RamCloud);
};

/**
 * Encapsulates the state of a RamCloud::createTable operation,
 * allowing it to execute asynchronously.
 */
class CreateTableRpc : public CoordinatorRpcWrapper {
  public:
    CreateTableRpc(RamCloud* ramcloud, const char* name,
            uint32_t serverSpan = 1);
    ~CreateTableRpc() {}
    uint64_t wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(CreateTableRpc);
};

/**
 * Encapsulates the state of a RamCloud::dropTable operation,
 * allowing it to execute asynchronously.
 */
class DropTableRpc : public CoordinatorRpcWrapper {
  public:
    DropTableRpc(RamCloud* ramcloud, const char* name);
    ~DropTableRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(context->dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(DropTableRpc);
};

/**
 * Encapsulates the state of a RamCloud::createIndex operation,
 * allowing it to execute asynchronously.
 */
class CreateIndexRpc : public CoordinatorRpcWrapper {
  public:
    CreateIndexRpc(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
              uint8_t indexType, uint8_t numIndexlets = 1);
    ~CreateIndexRpc() {}
    void wait() {simpleWait(context->dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(CreateIndexRpc);
};

/**
 * Encapsulates the state of a RamCloud::dropIndex operation,
 * allowing it to execute asynchronously.
 */
class DropIndexRpc : public CoordinatorRpcWrapper {
  public:
    DropIndexRpc(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId);
    ~DropIndexRpc() {}
    void wait() {simpleWait(context->dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(DropIndexRpc);
};

/**
 * Encapsulates the state of a RamCloud::enumerateTable
 * request, allowing it to execute asynchronously.
 */
class EnumerateTableRpc : public ObjectRpcWrapper {
  public:
    EnumerateTableRpc(RamCloud* ramcloud, uint64_t tableId, bool keysOnly,
            uint64_t tabletFirstHash, Buffer& iter, Buffer& objects);
    ~EnumerateTableRpc() {}
    uint64_t wait(Buffer& nextIter);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(EnumerateTableRpc);
};

/**
 * Encapsulates the state of a RamCloud::testingFill operation,
 * allowing it to execute asynchronously.
 */
class FillWithTestDataRpc : public ObjectRpcWrapper {
  public:
    FillWithTestDataRpc(RamCloud* ramcloud, uint64_t tableId, const void* key,
            uint16_t keyLength, uint32_t numObjects, uint32_t objectSize);
    ~FillWithTestDataRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(ramcloud->clientContext->dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(FillWithTestDataRpc);
};

/**
 * Encapsulates the state of a RamCloud::getLogMetrics operation,
 * allowing it to execute asynchronously.
 */
class GetLogMetricsRpc: public RpcWrapper {
  public:
    GetLogMetricsRpc(RamCloud* ramcloud, const char* serviceLocator);
    ~GetLogMetricsRpc() {}
    void wait(ProtoBuf::LogMetrics& logMetrics);

  PRIVATE:
    RamCloud* ramcloud;
    DISALLOW_COPY_AND_ASSIGN(GetLogMetricsRpc);
};

/**
 * Encapsulates the state of a RamCloud::getMetrics operation,
 * allowing it to execute asynchronously.
 */
class GetMetricsRpc : public ObjectRpcWrapper {
  public:
    GetMetricsRpc(RamCloud* ramcloud, uint64_t tableId,
            const void* key, uint16_t keyLength);
    ~GetMetricsRpc() {}
    ServerMetrics wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetMetricsRpc);
};

/**
 * Encapsulates the state of a RamCloud::getMetrics operation that
 * uses a service locator to identify the server rather than an object.
 */
class GetMetricsLocatorRpc : public RpcWrapper {
  public:
    GetMetricsLocatorRpc(RamCloud* ramcloud, const char* serviceLocator);
    ~GetMetricsLocatorRpc() {}
    ServerMetrics wait();

  PRIVATE:
    RamCloud* ramcloud;
    DISALLOW_COPY_AND_ASSIGN(GetMetricsLocatorRpc);
};

/**
 * Encapsulate the state of RamCloud:: getRuntimeOption operation
 * allowing to execute asynchronously.
 */
class GetRuntimeOptionRpc : public CoordinatorRpcWrapper{
    public:
        GetRuntimeOptionRpc(RamCloud* ramcloud, const char* option,
                         Buffer* value);
        ~GetRuntimeOptionRpc() {}
        void wait();
    PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(GetRuntimeOptionRpc);
};

/**
 * Encapsulates the state of a RamCloud::getServerConfig operation,
 * allowing it to execute asynchronously.
 */
class GetServerConfigRpc : public RpcWrapper {
  public:
    GetServerConfigRpc(RamCloud* ramcloud, const char* serviceLocator);
    ~GetServerConfigRpc() {}
    void wait(ProtoBuf::ServerConfig& serverConfig);

  PRIVATE:
    RamCloud* ramcloud;
    DISALLOW_COPY_AND_ASSIGN(GetServerConfigRpc);
};

/**
 * Encapsulates the state of a RamCloud::getServerStatistics operation,
 * allowing it to execute asynchronously.
 */
class GetServerStatisticsRpc : public RpcWrapper {
  public:
    GetServerStatisticsRpc(RamCloud* ramcloud, const char* serviceLocator);
    ~GetServerStatisticsRpc() {}
    void wait(ProtoBuf::ServerStatistics& serverStats);

  PRIVATE:
    RamCloud* ramcloud;
    DISALLOW_COPY_AND_ASSIGN(GetServerStatisticsRpc);
};

/**
 * Encapsulates the state of a RamCloud::getTableId operation,
 * allowing it to execute asynchronously.
 */
class GetTableIdRpc : public CoordinatorRpcWrapper {
  public:
    GetTableIdRpc(RamCloud* ramcloud, const char* name);
    ~GetTableIdRpc() {}
    uint64_t wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetTableIdRpc);
};

/**
 * Encapsulates the state of a RamCloud::incrementDouble operation,
 * allowing it to execute asynchronously.
 */
class IncrementDoubleRpc : public ObjectRpcWrapper {
  public:
    IncrementDoubleRpc(RamCloud* ramcloud, uint64_t tableId, const void* key,
            uint16_t keyLength, double incrementValue,
            const RejectRules* rejectRules = NULL);
    ~IncrementDoubleRpc() {}
    double wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(IncrementDoubleRpc);
};

/**
 * Encapsulates the state of a RamCloud::incrementInt64 operation,
 * allowing it to execute asynchronously.
 */
class IncrementInt64Rpc : public ObjectRpcWrapper {
  public:
    IncrementInt64Rpc(RamCloud* ramcloud, uint64_t tableId, const void* key,
            uint16_t keyLength, int64_t incrementValue,
            const RejectRules* rejectRules = NULL);
    ~IncrementInt64Rpc() {}
    int64_t wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(IncrementInt64Rpc);
};

/**
 * Encapsulates the state of a RamCloud::indexedRead operation,
 * allowing it to execute asynchronously.
 */
class IndexedReadRpc : public ObjectRpcWrapper {
  public:
    IndexedReadRpc(RamCloud* ramcloud, uint64_t tableId,
                   uint32_t numHashes, Buffer* pKHashes, uint8_t indexId,
                   const void* firstKey, uint16_t firstKeyLength,
                   const void* lastKey, uint16_t lastKeyLength,
                   Buffer* response);
    ~IndexedReadRpc() {}
    /// \copydoc RpcWrapper::docForWait
    uint32_t wait(uint32_t* numObjects);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(IndexedReadRpc);
};

/**
 * Encapsulates the state of a RamCloud::indexServerControl operation,
 * allowing it to execute asynchronously.
 */
class IndexServerControlRpc : public IndexRpcWrapper {
  public:
    IndexServerControlRpc(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
        const void* key, uint16_t keyLength, WireFormat::ControlOp controlOp,
        const void* inputData, uint32_t inputLength, Buffer* outputData);
    ~IndexServerControlRpc() {}
    void wait();
  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(IndexServerControlRpc);
};

/**
 * Encapsulates the state of a RamCloud::testingKill operation.  This
 * RPC should never be waited for!!  If you do, it will track the object
 * around the cluster, killing the server currently holding the object,
 * waiting for the object to reappear on a different server, then killing
 * that server, and so on forever.
 */
class KillRpc : public ObjectRpcWrapper {
  public:
    KillRpc(RamCloud* ramcloud, uint64_t tableId, const void* key,
            uint16_t keyLength);
    ~KillRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(ramcloud->clientContext->dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(KillRpc);
};



/**
 * Encapsulates the state of a RamCloud::lookupIndexKeys operation,
 * allowing it to execute asynchronously.
 */
class LookupIndexKeysRpc : public IndexRpcWrapper {
  public:
    LookupIndexKeysRpc(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
                       const void* firstKey, uint16_t firstKeyLength,
                       uint64_t firstAllowedKeyHash,
                       const void* lastKey, uint16_t lastKeyLength,
                       uint32_t maxNumHashes, Buffer* responseBuffer);
    ~LookupIndexKeysRpc() {}

    void indexNotFound();
    void wait(uint32_t* numHashes, uint16_t* nextKeyLength,
              uint64_t* nextKeyHash);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(LookupIndexKeysRpc);
};

/**
 * Encapsulates the state of a RamCloud::migrateTablet operation,
 * allowing it to execute asynchronously.
 */
class MigrateTabletRpc : public ObjectRpcWrapper {
  public:
    MigrateTabletRpc(RamCloud* ramcloud, uint64_t tableId,
            uint64_t firstKeyHash, uint64_t lastKeyHash,
            ServerId newMasterOwnerId);
    ~MigrateTabletRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(ramcloud->clientContext->dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(MigrateTabletRpc);
};

/**
 * The base class used to pass parameters into the MultiOp Framework. Any
 * multi operation xxxxx that uses the Framework should have its own
 * MultixxxxxObject that extends this object to describe its own parameters.
 */
struct MultiOpObject {
    /**
     * The table containing the desired object (return value from
     * a previous call to getTableId).
     */
    uint64_t tableId;

    /**
     * Variable length key that uniquely identifies the object within table.
     * It does not necessarily have to be null terminated like a string.
     * The caller is responsible for ensuring that this key remains valid
     * until the call is reaped/canceled.
     */
    const void* key;

    /**
     * Length of key, in bytes.
     */
    uint16_t keyLength;

    /**
     * The status of read (either that the read succeeded, or the
     * error in case it didn't) is returned here.
     */
    Status status;

    PROTECTED:
    MultiOpObject(uint64_t tableId, const void* key, uint16_t keyLength)
        : tableId(tableId)
        , key(key)
        , keyLength(keyLength)
        , status()
    {}

    MultiOpObject()
        : tableId()
        , key()
        , keyLength()
        , status()
    {}

    MultiOpObject(const MultiOpObject& other)
        : tableId(other.tableId)
        , key(other.key)
        , keyLength(other.keyLength)
        , status(other.status)
    {};

    MultiOpObject& operator=(const MultiOpObject& other) {
        tableId = other.tableId;
        key = other.key;
        keyLength = other.keyLength;
        status = other.status;
        return *this;
    }

    virtual ~MultiOpObject() {};
};

/**
 * Objects of this class are used to pass parameters into \c multiIncrement
 * and for multiIncrement to return the new value and the status values 
 * for conditional operations (if used).
 */
struct MultiIncrementObject : public MultiOpObject {
    /**
     * Summand to add to the existing object, which is either interpreted as
     * an integer or as a floating point value depending on which summand is
     * non-zero.
     */
    int64_t incrementInt64;
    double incrementDouble;

    /**
     * The RejectRules specify when conditional increments should be aborted.
     */
    const RejectRules* rejectRules;

    /**
     * The version number of the newly written object is returned here.
     */
    uint64_t version;

    /**
     * Value of the object after increasing
     */
    union {
        int64_t asInt64;
        double asDouble;
    } newValue;

    MultiIncrementObject(uint64_t tableId, const void* key, uint16_t keyLength,
                 int64_t incrementInt64, double incrementDouble,
                 const RejectRules* rejectRules = NULL)
        : MultiOpObject(tableId, key, keyLength)
        , incrementInt64(incrementInt64)
        , incrementDouble(incrementDouble)
        , rejectRules(rejectRules)
        , version()
        , newValue()
    {}

    MultiIncrementObject()
        : MultiOpObject()
        , incrementInt64()
        , incrementDouble()
        , rejectRules()
        , version()
        , newValue()
    {}

    MultiIncrementObject(const MultiIncrementObject& other)
        : MultiOpObject(other)
        , incrementInt64(other.incrementInt64)
        , incrementDouble(other.incrementDouble)
        , rejectRules(other.rejectRules)
        , version(other.version)
        , newValue(other.newValue)
    {}

    MultiIncrementObject& operator=(const MultiIncrementObject& other) {
        MultiOpObject::operator =(other);
        incrementInt64 = other.incrementInt64;
        incrementDouble = other.incrementDouble;
        rejectRules = other.rejectRules;
        version = other.version;
        newValue = other.newValue;
        return *this;
    }
};

/**
 * Objects of this class are used to pass parameters into \c multiRead
 * and for multiRead to return result values.
 */
struct MultiReadObject : public MultiOpObject {
    /**
     * If the read for this object was successful, the Tub<ObjectBuffer>
     * will hold the contents of the desired object. If not, it will
     * not be initialized, giving "false" when the buffer is tested.
     */
    Tub<ObjectBuffer>* value;

    /**
     * The version number of the object is returned here.
     */
    uint64_t version;

    MultiReadObject(uint64_t tableId, const void* key, uint16_t keyLength,
            Tub<ObjectBuffer>* value)
        : MultiOpObject(tableId, key, keyLength)
        , value(value)
        , version()
    {}

    MultiReadObject()
        : value()
        , version()
    {}

    MultiReadObject(const MultiReadObject& other)
        : MultiOpObject(other)
        , value(other.value)
        , version(other.version)
    {}

    MultiReadObject& operator=(const MultiReadObject& other) {
        MultiOpObject::operator =(other);
        value = other.value;
        version = other.version;
        return *this;
    }
};

/**
 * Objects of this class are used to pass parameters into \c multiRemove.
 */
struct MultiRemoveObject : public MultiOpObject {

    /**
     * The RejectRules specify when conditional removes should be aborted.
     */
    const RejectRules* rejectRules;

    /**
     * The version number of the object just before removal is returned here.
     */
    uint64_t version;

    MultiRemoveObject(uint64_t tableId, const void* key, uint16_t keyLength,
                      const RejectRules* rejectRules = NULL)
        : MultiOpObject(tableId, key, keyLength)
        , rejectRules(rejectRules)
        , version()
    {}

    MultiRemoveObject()
        : rejectRules()
        , version()
    {}

    MultiRemoveObject(const MultiRemoveObject& other)
        : MultiOpObject(other)
        , rejectRules(other.rejectRules)
        , version(other.version)
    {}

    MultiRemoveObject& operator=(const MultiRemoveObject& other) {
        MultiOpObject::operator =(other);
        rejectRules = other.rejectRules;
        version = other.version;
        return *this;
    }
};

/**
 * Objects of this class are used to pass parameters into \c multiWrite
 * and for multiWrite to return status values for conditional operations
 * (if used).
 */
struct MultiWriteObject : public MultiOpObject {
    /**
     * Pointer to the contents of the new object.
     */
    const void* value;

    /**
     * Length of value in bytes.
     */
    uint32_t valueLength;

    /**
    * Number of keys in this multiWrite object
    */
    uint8_t numKeys;

    /**
     * List of keys and their lengths part of this multiWrite object.
     * This will be NULL for single key multiwrite objects
     */
    KeyInfo *keyInfo;
    /**
     * The RejectRules specify when conditional writes should be aborted.
     */
    const RejectRules* rejectRules;

    /**
     * The version number of the newly written object is returned here.
     */
    uint64_t version;

    /**
     * Typically used when each object has a single key.
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
     *      Address of the first byte of the new contents for the object;
     *      must contain at least length bytes.
     * \param valueLength
     *      Size in bytes of the value of the object.
     * \param rejectRules
     *      If non-NULL, specifies conditions under which the write
     *      should be aborted with an error.
     */
    MultiWriteObject(uint64_t tableId, const void* key, uint16_t keyLength,
                 const void* value, uint32_t valueLength,
                 const RejectRules* rejectRules = NULL)
        : MultiOpObject(tableId, key, keyLength)
        , value(value)
        , valueLength(valueLength)
        , numKeys(1)
        , keyInfo(NULL)
        , rejectRules(rejectRules)
        , version()
    {}

    /**
     * Typically used when each object has multiple keys.
     * \param tableId
     *      The table containing the desired object (return value from
     *      a previous call to getTableId).
     * \param value
     *      Address of the first byte of the new contents for the object;
     *      must contain at least length bytes.
     * \param valueLength
     *      Size in bytes of the value of the object.
     * \param numKeys
     *      Number of keys in the object.  If is not >= 1, then behavior
     *      is undefined. A value of 1 indicates the presence of only the
     *      primary key
     * \param keyInfo
     *      List of keys and corresponding key lengths. The first entry should
     *      correspond to the primary key and its length. If this argument is
     *      NULL, then behavior is undefined. RamCloud currently uses a dense
     *      representation of key lengths. If a client does not want to write
     *      key_i in an object, keyInfo[i]->key should be NULL. The library also
     *      expects that if keyInfo[j]->key exists and keyInfo[j]->keyLength
     *      is 0, then key string is NULL terminated and the length is computed.
     * \param rejectRules
     *      If non-NULL, specifies conditions under which the write
     *      should be aborted with an error.
     */
    MultiWriteObject(uint64_t tableId,
                 const void* value, uint32_t valueLength,
                 uint8_t numKeys, KeyInfo *keyInfo,
                 const RejectRules* rejectRules = NULL)
        : MultiOpObject(tableId, NULL, 0)
        , value(value)
        , valueLength(valueLength)
        , numKeys(numKeys)
        , keyInfo(keyInfo)
        , rejectRules(rejectRules)
        , version()
    {}

    MultiWriteObject()
        : MultiOpObject()
        , value()
        , valueLength()
        , numKeys()
        , keyInfo()
        , rejectRules()
        , version()
    {}

    MultiWriteObject(const MultiWriteObject& other)
        : MultiOpObject(other)
        , value(other.value)
        , valueLength(other.valueLength)
        , numKeys(other.numKeys)
        , keyInfo(other.keyInfo)
        , rejectRules(other.rejectRules)
        , version(other.version)
    {}

    MultiWriteObject& operator=(const MultiWriteObject& other) {
        MultiOpObject::operator =(other);
        value = other.value;
        valueLength = other.valueLength;
        numKeys = other.numKeys;
        // shallow copy should be good enough
        keyInfo = other.keyInfo;
        rejectRules = other.rejectRules;
        version = other.version;
        return *this;
    }
};

/**
 * Encapsulates the state of a RamCloud::quiesce operation,
 * allowing it to execute asynchronously.
 */
class QuiesceRpc : public CoordinatorRpcWrapper {
  public:
    explicit QuiesceRpc(RamCloud* ramcloud);
    ~QuiesceRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(context->dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(QuiesceRpc);
};

/**
 * Encapsulates the state of a RamCloud::read operation,
 * allowing it to execute asynchronously.
 */
class ReadRpc : public ObjectRpcWrapper {
  public:
    ReadRpc(RamCloud* ramcloud, uint64_t tableId, const void* key,
            uint16_t keyLength, Buffer* value,
            const RejectRules* rejectRules = NULL);
    ~ReadRpc() {}
    void wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ReadRpc);
};

/**
 * Encapsulates the state of a RamCloud::read operation,
 * allowing it to execute asynchronously. The difference from
 * ReadRpc is in the contents of the returned buffer.
 */
class ReadKeysAndValueRpc : public ObjectRpcWrapper {
  public:
    ReadKeysAndValueRpc(RamCloud* ramcloud, uint64_t tableId, const void* key,
            uint16_t keyLength, ObjectBuffer* value,
            const RejectRules* rejectRules = NULL);
    ~ReadKeysAndValueRpc() {}
    void wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ReadKeysAndValueRpc);
};

/**
 * Encapsulates the state of a RamCloud::remove operation,
 * allowing it to execute asynchronously.
 */
class RemoveRpc : public ObjectRpcWrapper {
  public:
    RemoveRpc(RamCloud* ramcloud, uint64_t tableId, const void* key,
            uint16_t keyLength, const RejectRules* rejectRules = NULL);
    ~RemoveRpc() {}
    void wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RemoveRpc);
};

/**
 * Encapsulates the state of a RamCloud::objectServerControl operation,
 * allowing it to execute asynchronously.
 */
class ObjectServerControlRpc : public ObjectRpcWrapper {
  public:
    ObjectServerControlRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength, WireFormat::ControlOp controlOp,
        const void* inputData = NULL, uint32_t inputLength = 0,
        Buffer* outputData = NULL);
    ~ObjectServerControlRpc() {}
    void wait();
  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ObjectServerControlRpc);
};

/**
 * Encapsulates the state of a RamCloud::serverControlAll operation,
 * allowing it to execute asynchronously.
 */
class ServerControlAllRpc : public CoordinatorRpcWrapper {
  public:
    ServerControlAllRpc(RamCloud* ramcloud, WireFormat::ControlOp controlOp,
        const void* inputData = NULL, uint32_t inputLength = 0,
        Buffer* outputData = NULL);
    ~ServerControlAllRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(context->dispatch);}
  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ServerControlAllRpc);
};

/**
 * Encapsulates the state of a RamCloud::setRuntimeOption operation,
 * allowing it to execute asynchronously.
 */
class SetRuntimeOptionRpc : public CoordinatorRpcWrapper {
  public:
    SetRuntimeOptionRpc(RamCloud* ramcloud, const char* option,
            const char* value);
    ~SetRuntimeOptionRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(context->dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SetRuntimeOptionRpc);
};

/**
 * Encapsulates the state of a RamCloud::splitTablet operation,
 * allowing it to execute asynchronously.
 */
class SplitTabletRpc : public CoordinatorRpcWrapper {
  public:
    SplitTabletRpc(RamCloud* ramcloud, const char* name,
            uint64_t splitKeyHash);
    ~SplitTabletRpc() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(context->dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SplitTabletRpc);
};

/**
 * Encapsulates the state of a RamCloud::write operation,
 * allowing it to execute asynchronously.
 */
class WriteRpc : public ObjectRpcWrapper {
  public:
    WriteRpc(RamCloud* ramcloud, uint64_t tableId, const void* key,
            uint16_t keyLength, const void* buf, uint32_t length,
            const RejectRules* rejectRules = NULL, bool async = false);
    // this constructor will be used when the object has multiple keys
    WriteRpc(RamCloud* ramcloud, uint64_t tableId,
            uint8_t numKeys, KeyInfo *keyInfo,
            const void* buf, uint32_t length,
            const RejectRules* rejectRules = NULL, bool async = false);
    ~WriteRpc() {}
    void wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(WriteRpc);
};
} // namespace RAMCloud

#endif // RAMCLOUD_RAMCLOUD_H
