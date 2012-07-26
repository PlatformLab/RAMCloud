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

#ifndef RAMCLOUD_RAMCLOUD_H
#define RAMCLOUD_RAMCLOUD_H

#include "Common.h"
#include "CoordinatorClient.h"
#include "MasterClient.h"
#include "ObjectFinder.h"
#include "ObjectRpcWrapper.h"
#include "ServerMetrics.h"

namespace RAMCloud {
class MultiReadObject;

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
    uint64_t enumerateTable(uint64_t tableId, uint64_t tabletFirstHash,
         Buffer& state, Buffer& objects);
    ServerMetrics getMetrics(uint64_t tableId, const char* key,
            uint16_t keyLength);
    ServerMetrics getMetrics(const char* serviceLocator);
    void getServerStatistics(const char* serviceLocator,
            ProtoBuf::ServerStatistics& serverStats);
    string* getServiceLocator();
    uint64_t getTableId(const char* name);
    int64_t increment(uint64_t tableId, const char* key, uint16_t keyLength,
            int64_t incrementValue, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL);
    void migrateTablet(uint64_t tableId, uint64_t firstKeyHash,
            uint64_t lastKeyHash, ServerId newOwnerMasterId);
    void multiRead(MultiReadObject* requests[], uint32_t numRequests);
    void quiesce();
    void read(uint64_t tableId, const char* key, uint16_t keyLength,
            Buffer* value, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL);
    void remove(uint64_t tableId, const char* key, uint16_t keyLength,
            const RejectRules* rejectRules = NULL, uint64_t* version = NULL);
    void splitTablet(const char* name, uint64_t startKeyHash,
            uint64_t endKeyHash, uint64_t splitKeyHash);
    void testingFill(uint64_t tableId, const char* key, uint16_t keyLength,
            uint32_t numObjects, uint32_t objectSize);
    uint64_t testingGetServerId(uint64_t tableId, const char* key,
            uint16_t keyLength);
    string testingGetServiceLocator(uint64_t tableId, const char* key,
            uint16_t keyLength);
    void testingKill(uint64_t tableId, const char* key, uint16_t keyLength);
    void testingSetRuntimeOption(const char* option, const char* value);
    void testingWaitForAllTabletsNormal();
    void write(uint64_t tableId, const char* key, uint16_t keyLength,
            const void* buf, uint32_t length,
            const RejectRules* rejectRules = NULL, uint64_t* version = NULL,
            bool async = false);
    void write(uint64_t tableId, const char* key, uint16_t keyLength,
            const char* value, const RejectRules* rejectRules = NULL,
            uint64_t* version = NULL, bool async = false);

    explicit RamCloud(const char* serviceLocator);
    RamCloud(Context& context, const char* serviceLocator);

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
    Context& clientContext;

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
 * Objects of this class are used to pass parameters into \c multiRead
 * and for multiRead to return result values.
 */
struct MultiReadObject {
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
    const char* key;
    /**
  * Length of key, in bytes.
  */
    uint16_t keyLength;
    /**
  * If the read for this object was successful, the Tub<Buffer>
  * will hold the contents of the desired object. If not, it will
  * not be initialized, giving "false" when the buffer is tested.
  */
    Tub<Buffer>* value;
    /**
  * The version number of the object is returned here
  */
    uint64_t version;
    /**
  * The status of read (either that the read succeeded, or the
  * error in case it didn't) is returned here.
  */
    Status status;

    MultiReadObject(uint64_t tableId, const char* key, uint16_t keyLength,
            Tub<Buffer>* value)
        : tableId(tableId)
        , key(key)
        , keyLength(keyLength)
        , value(value)
        , version()
        , status()
    {
    }

    MultiReadObject()
        : tableId()
        , key()
        , keyLength()
        , value()
        , version()
        , status()
    {
    }
};

/**
 * Encapsulates the state of a RamCloud::createTable operation,
 * allowing it to execute asynchronously.
 */
class CreateTableRpc2 : public CoordinatorRpcWrapper {
  public:
    CreateTableRpc2(RamCloud& ramcloud, const char* name,
            uint32_t serverSpan = 1);
    ~CreateTableRpc2() {}
    uint64_t wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(CreateTableRpc2);
};

/**
 * Encapsulates the state of a RamCloud::dropTable operation,
 * allowing it to execute asynchronously.
 */
class DropTableRpc2 : public CoordinatorRpcWrapper {
  public:
    DropTableRpc2(RamCloud& ramcloud, const char* name);
    ~DropTableRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(DropTableRpc2);
};

/**
 * Encapsulates the state of a RamCloud::enumerateTable
 * request, allowing it to execute asynchronously.
 */
class EnumerateTableRpc2 : public ObjectRpcWrapper {
  public:
    EnumerateTableRpc2(RamCloud& ramcloud, uint64_t tableId,
            uint64_t tabletFirstHash, Buffer& iter, Buffer& objects);
    ~EnumerateTableRpc2() {}
    uint64_t wait(Buffer& nextIter);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(EnumerateTableRpc2);
};

/**
 * Encapsulates the state of a RamCloud::testingFill operation,
 * allowing it to execute asynchronously.
 */
class FillWithTestDataRpc2 : public ObjectRpcWrapper {
  public:
    FillWithTestDataRpc2(RamCloud& ramcloud, uint64_t tableId, const char* key,
            uint16_t keyLength, uint32_t numObjects, uint32_t objectSize);
    ~FillWithTestDataRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*ramcloud.clientContext.dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(FillWithTestDataRpc2);
};

/**
 * Encapsulates the state of a RamCloud::getMetrics operation,
 * allowing it to execute asynchronously.
 */
class GetMetricsRpc2 : public ObjectRpcWrapper {
  public:
    GetMetricsRpc2(RamCloud& ramcloud, uint64_t tableId,
            const char* key, uint16_t keyLength);
    ~GetMetricsRpc2() {}
    ServerMetrics wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetMetricsRpc2);
};

/**
 * Encapsulates the state of a RamCloud::getMetrics operation that
 * uses a service locator to identify the server rather than an object.
 */
class GetMetricsLocatorRpc : public RpcWrapper {
  public:
    GetMetricsLocatorRpc(RamCloud& ramcloud, const char* serviceLocator);
    ~GetMetricsLocatorRpc() {}
    ServerMetrics wait();

  PRIVATE:
    RamCloud& ramcloud;
    DISALLOW_COPY_AND_ASSIGN(GetMetricsLocatorRpc);
};

/**
 * Encapsulates the state of a RamCloud::getServerStatistics operation,
 * allowing it to execute asynchronously.
 */
class GetServerStatisticsRpc2 : public RpcWrapper {
  public:
    GetServerStatisticsRpc2(RamCloud& ramcloud, const char* serviceLocator);
    ~GetServerStatisticsRpc2() {}
    void wait(ProtoBuf::ServerStatistics& serverStats);

  PRIVATE:
    RamCloud& ramcloud;
    DISALLOW_COPY_AND_ASSIGN(GetServerStatisticsRpc2);
};

/**
 * Encapsulates the state of a RamCloud::getTableId operation,
 * allowing it to execute asynchronously.
 */
class GetTableIdRpc2 : public CoordinatorRpcWrapper {
  public:
    GetTableIdRpc2(RamCloud& ramcloud, const char* name);
    ~GetTableIdRpc2() {}
    uint64_t wait();

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(GetTableIdRpc2);
};

/**
 * Encapsulates the state of a RamCloud::increment operation,
 * allowing it to execute asynchronously.
 */
class IncrementRpc2 : public ObjectRpcWrapper {
  public:
    IncrementRpc2(RamCloud& ramcloud, uint64_t tableId, const char* key,
            uint16_t keyLength, int64_t incrementValue,
            const RejectRules* rejectRules = NULL);
    ~IncrementRpc2() {}
    int64_t wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(IncrementRpc2);
};

/**
 * Encapsulates the state of a RamCloud::testingKill operation.  This
 * RPC should never be waited for!!  If you do, it will track the object
 * around the cluster, killing the server currently holding the object,
 * waiting for the object to reappear on a different server, then killing
 * that server, and so on forever.
 */
class KillRpc2 : public ObjectRpcWrapper {
  public:
    KillRpc2(RamCloud& ramcloud, uint64_t tableId, const char* key,
            uint16_t keyLength);
    ~KillRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*ramcloud.clientContext.dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(KillRpc2);
};

/**
 * Encapsulates the state of a RamCloud::migrateTablet operation,
 * allowing it to execute asynchronously.
 */
class MigrateTabletRpc2 : public ObjectRpcWrapper {
  public:
    MigrateTabletRpc2(RamCloud& ramcloud, uint64_t tableId,
            uint64_t firstKeyHash, uint64_t lastKeyHash,
            ServerId newMasterOwnerId);
    ~MigrateTabletRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*ramcloud.clientContext.dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(MigrateTabletRpc2);
};

/**
 * Encapsulates the state of a RamCloud::quiesce operation,
 * allowing it to execute asynchronously.
 */
class QuiesceRpc2 : public CoordinatorRpcWrapper {
  public:
    explicit QuiesceRpc2(RamCloud& ramcloud);
    ~QuiesceRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(QuiesceRpc2);
};

/**
 * Encapsulates the state of a RamCloud::read operation,
 * allowing it to execute asynchronously.
 */
class ReadRpc2 : public ObjectRpcWrapper {
  public:
    ReadRpc2(RamCloud& ramcloud, uint64_t tableId, const char* key,
            uint16_t keyLength, Buffer* value,
            const RejectRules* rejectRules = NULL);
    ~ReadRpc2() {}
    void wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(ReadRpc2);
};

/**
 * Encapsulates the state of a RamCloud::remove operation,
 * allowing it to execute asynchronously.
 */
class RemoveRpc2 : public ObjectRpcWrapper {
  public:
    RemoveRpc2(RamCloud& ramcloud, uint64_t tableId, const char* key,
            uint16_t keyLength, const RejectRules* rejectRules = NULL);
    ~RemoveRpc2() {}
    void wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(RemoveRpc2);
};

/**
 * Encapsulates the state of a RamCloud::testingSetRuntimeOption operation,
 * allowing it to execute asynchronously.
 */
class SetRuntimeOptionRpc2 : public CoordinatorRpcWrapper {
  public:
    SetRuntimeOptionRpc2(RamCloud& ramcloud, const char* option,
            const char* value);
    ~SetRuntimeOptionRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SetRuntimeOptionRpc2);
};

/**
 * Encapsulates the state of a RamCloud::splitTablet operation,
 * allowing it to execute asynchronously.
 */
class SplitTabletRpc2 : public CoordinatorRpcWrapper {
  public:
    SplitTabletRpc2(RamCloud& ramcloud, const char* name,
            uint64_t startKeyHash, uint64_t endKeyHash,
            uint64_t splitKeyHash);
    ~SplitTabletRpc2() {}
    /// \copydoc RpcWrapper::docForWait
    void wait() {simpleWait(*context.dispatch);}

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(SplitTabletRpc2);
};

/**
 * Encapsulates the state of a RamCloud::write operation,
 * allowing it to execute asynchronously.
 */
class WriteRpc2 : public ObjectRpcWrapper {
  public:
    WriteRpc2(RamCloud& ramcloud, uint64_t tableId, const char* key,
            uint16_t keyLength, const void* buf, uint32_t length,
            const RejectRules* rejectRules = NULL, bool async = false);
    ~WriteRpc2() {}
    void wait(uint64_t* version = NULL);

  PRIVATE:
    DISALLOW_COPY_AND_ASSIGN(WriteRpc2);
};
} // namespace RAMCloud

#endif // RAMCLOUD_RAMCLOUD_H
