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

#ifndef RAMCLOUD_RAMCLOUD_H
#define RAMCLOUD_RAMCLOUD_H

#include "Common.h"
#include "CoordinatorClient.h"
#include "MasterClient.h"
#include "ObjectFinder.h"
#include "ObjectRpcWrapper.h"
#include "ServerMetrics.h"

#include "LogMetrics.pb.h"
#include "ServerConfig.pb.h"

namespace RAMCloud {
class MultiReadObject;
class MultiRemoveObject;
class MultiWriteObject;

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
    int64_t increment(uint64_t tableId, const void* key, uint16_t keyLength,
            int64_t incrementValue, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL);
    void migrateTablet(uint64_t tableId, uint64_t firstKeyHash,
            uint64_t lastKeyHash, ServerId newOwnerMasterId);
    void multiRead(MultiReadObject* requests[], uint32_t numRequests);
    void multiRemove(MultiRemoveObject* requests[], uint32_t numRequests);
    void multiWrite(MultiWriteObject* requests[], uint32_t numRequests);
    void quiesce();
    void read(uint64_t tableId, const void* key, uint16_t keyLength,
            Buffer* value, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL);
    void remove(uint64_t tableId, const void* key, uint16_t keyLength,
            const RejectRules* rejectRules = NULL, uint64_t* version = NULL);
    void serverControl(uint64_t tableId, const void* key, uint16_t keyLength,
            WireFormat::ControlOp controlOp,
            const void* inputData, uint32_t inputLength, Buffer* outputData);
    void splitTablet(const char* name, uint64_t splitKeyHash);
    void testingFill(uint64_t tableId, const void* key, uint16_t keyLength,
            uint32_t numObjects, uint32_t objectSize);
    uint64_t testingGetServerId(uint64_t tableId, const void* key,
            uint16_t keyLength);
    string testingGetServiceLocator(uint64_t tableId, const void* key,
            uint16_t keyLength);
    void testingKill(uint64_t tableId, const void* key, uint16_t keyLength);
    void setRuntimeOption(const char* option, const char* value);
    void testingWaitForAllTabletsNormal(uint64_t timeoutNs = ~0lu);
    void write(uint64_t tableId, const void* key, uint16_t keyLength,
                const void* buf, uint32_t length,
                const RejectRules* rejectRules = NULL, uint64_t* version = NULL,
                bool async = false);
    void write(uint64_t tableId, const void* key, uint16_t keyLength,
            const char* value, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL, bool async = false);
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
        ~GetRuntimeOptionRpc(){}
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
 * Encapsulates the state of a RamCloud::increment operation,
 * allowing it to execute asynchronously.
 */
class IncrementRpc : public ObjectRpcWrapper {
  public:
    IncrementRpc(RamCloud* ramcloud, uint64_t tableId, const void* key,
            uint16_t keyLength, int64_t incrementValue,
            const RejectRules* rejectRules = NULL);
    ~IncrementRpc() {}
    int64_t wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(IncrementRpc);
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
 * Objects of this class are used to pass parameters into \c multiRead
 * and for multiRead to return result values.
 */
struct MultiReadObject : public MultiOpObject {
    /**
     * If the read for this object was successful, the Tub<Buffer>
     * will hold the contents of the desired object. If not, it will
     * not be initialized, giving "false" when the buffer is tested.
     */
    Tub<Buffer>* value;

    /**
     * The version number of the object is returned here.
     */
    uint64_t version;

    MultiReadObject(uint64_t tableId, const void* key, uint16_t keyLength,
            Tub<Buffer>* value)
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
     * The RejectRules specify when conditional writes should be aborted.
     */
    const RejectRules* rejectRules;

    /**
     * The version number of the newly written object is returned here.
     */
    uint64_t version;

    MultiWriteObject(uint64_t tableId, const void* key, uint16_t keyLength,
                 const void* value, uint32_t valueLength,
                 const RejectRules* rejectRules = NULL)
        : MultiOpObject(tableId, key, keyLength)
        , value(value)
        , valueLength(valueLength)
        , rejectRules(rejectRules)
        , version()
    {}

    MultiWriteObject()
        : MultiOpObject()
        , value()
        , valueLength()
        , rejectRules()
        , version()
    {}

    MultiWriteObject(const MultiWriteObject& other)
        : MultiOpObject(other)
        , value(other.value)
        , valueLength(other.valueLength)
        , rejectRules(other.rejectRules)
        , version(other.version)
    {}

    MultiWriteObject& operator=(const MultiWriteObject& other) {
        MultiOpObject::operator =(other);
        value = other.value;
        valueLength = other.valueLength;
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
 * Encapsulates the state of a RamCloud::serverControl operation,
 * allowing it to execute asynchronously.
 */
class ServerControlRpc : public ObjectRpcWrapper {
  public:
    ServerControlRpc(RamCloud* ramcloud, uint64_t tableId,
        const void* key, uint16_t keyLength, WireFormat::ControlOp controlOp,
        const void* inputData, uint32_t inputLength, Buffer* outputData);
    ~ServerControlRpc() {}
    void wait();
  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ServerControlRpc);
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
    ~WriteRpc() {}
    void wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(WriteRpc);
};
} // namespace RAMCloud

#endif // RAMCLOUD_RAMCLOUD_H
