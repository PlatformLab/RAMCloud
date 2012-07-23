/* Copyright (c) 2010-2011 Stanford University
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

#ifndef RAMCLOUD_MASTERCLIENT_H
#define RAMCLOUD_MASTERCLIENT_H

#include "Client.h"
#include "Common.h"
#include "CoordinatorClient.h"
#include "Transport.h"
#include "Buffer.h"
#include "ServerId.h"
#include "ServerIdRpcWrapper.h"
#include "ServerStatistics.pb.h"
#include "Tub.h"
#include "LogTypes.h"

namespace RAMCloud {

/**
 * Provides methods for invoking RPCs to RAMCloud masters.  The invoking
 * machine is typically another RAMCloud server (either master or backup)
 * or the cluster coordinator; these methods are not as frequently used
 * by RAMCloud applications.
 */
class MasterClient : public Client {
  public:
    class IsReplicaNeededRpc2 : public ServerIdRpcWrapper {
      public:
        IsReplicaNeededRpc2(Context& context,
                            ServerId id,
                            ServerId backupServerId,
                            uint64_t segmentId);
        ~IsReplicaNeededRpc2() {}
        bool wait();

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(IsReplicaNeededRpc2);
    };

    /**
     * Encapsulates the state of a CoordinatorClient::enlistServer
     * request, allowing it to execute asynchronously.
     */
    class TakeTabletOwnershipRpc2 : public ServerIdRpcWrapper {
      public:
        TakeTabletOwnershipRpc2(Context& context, ServerId id,
                uint64_t tableId, uint64_t firstKey, uint64_t lastKey);
        ~TakeTabletOwnershipRpc2() {}
        /// \copydoc ServerIdRpcWrapper::waitAndCheckErrors
        void wait() {waitAndCheckErrors();}

      PRIVATE:
        DISALLOW_COPY_AND_ASSIGN(TakeTabletOwnershipRpc2);
    };
    static void takeTabletOwnership(Context& context, ServerId id,
            uint64_t tableId, uint64_t firstKey, uint64_t lastKey);

    //-------------------------------------------------------
    // OLD: everything below here should eventually go away.
    //-------------------------------------------------------

    /// An asynchronous version of #enumeration().
    class Enumeration {
      public:
        Enumeration(MasterClient& client,
                    uint64_t tableId,
                    uint64_t tabletStartHash, uint64_t* nextTabletStartHash,
                    Buffer* iter, Buffer* nextIter,
                    Buffer* objects);
        void cancel() { state.cancel(); }
        bool isReady() { return state.isReady(); }
        void operator()();
      private:
        MasterClient& client;
        Buffer requestBuffer;
        Buffer responseBuffer;
        uint64_t* nextTabletStartHash;
        Buffer& nextIter;
        Buffer& objects;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(Enumeration);
    };

    /// An asynchronous version of #read().
    class Read {
      public:
        Read(MasterClient& client,
             uint64_t tableId, const char* key, uint16_t keyLength,
             Buffer* value, const RejectRules* rejectRules,
             uint64_t* version);
        void cancel() { state.cancel(); }
        bool isReady() { return state.isReady(); }
        void operator()();
      private:
        MasterClient& client;
        uint64_t* version;
        Buffer requestBuffer;
        Buffer& responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(Read);
    };

    class Recover {
      public:
        Recover(MasterClient& client,
                uint64_t recoveryId,
                ServerId crashedServerId,
                uint64_t partitionId,
                const ProtoBuf::Tablets& tablets,
                const RecoverRpc::Replica* replicas,
                uint32_t numReplicas);
        bool isReady() { return state.isReady(); }
        void operator()();
      private:
        MasterClient& client;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(Recover);
    };

    /// An asynchronous version of #write().
    class Write {
      public:
        Write(MasterClient& client,
              uint64_t tableId, const char* key, uint16_t keyLength,
              Buffer& buffer,
              const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL, bool async = false);
        Write(MasterClient& client,
              uint64_t tableId, const char* key, uint16_t keyLength,
              const void* buf, uint32_t length,
              const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL, bool async = false);
        bool isReady() { return state.isReady(); }
        void operator()();
      private:
        MasterClient& client;
        uint64_t* version;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(Write);
    };

    explicit MasterClient(Transport::SessionRef session) : session(session) {}
    void enumeration(uint64_t tableId,
                     uint64_t tabletStartHash, uint64_t* nextTabletStartHash,
                     Buffer* iter, Buffer* nextIter,
                     Buffer* objects);
    void fillWithTestData(uint32_t numObjects, uint32_t objectSize);
    void increment(uint64_t tableId, const char* key, uint16_t keyLength,
                   int64_t incrementValue,
                   const RejectRules* rejectRules = NULL,
                   uint64_t* version = NULL, int64_t* newValue = NULL);
    bool isReplicaNeeded(ServerId backupServerId, uint64_t segmentId);
    LogPosition getHeadOfLog();
    void getServerStatistics(ProtoBuf::ServerStatistics& serverStats);
    void read(uint64_t tableId, const char* key, uint16_t keyLength,
              Buffer* value, const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL);
    void prepForMigration(uint64_t tableId,
                          uint64_t firstKey,
                          uint64_t lastKey,
                          uint64_t expectedObjects,
                          uint64_t expectedBytes);
    void receiveMigrationData(uint64_t tableId,
                              uint64_t firstKey,
                              const void* segment,
                              uint32_t segmentBytes);
    void migrateTablet(uint64_t tableId,
                       uint64_t firstKey,
                       uint64_t lastKey,
                       ServerId newMasterOwnerId);
    void recover(uint64_t recoveryId,
                 ServerId crashedServerId, uint64_t partitionId,
                 const ProtoBuf::Tablets& tablets,
                 const RecoverRpc::Replica* replicas, uint32_t numReplicas);
    void remove(uint64_t tableId, const char* key, uint16_t keyLength,
                const RejectRules* rejectRules = NULL,
                uint64_t* version = NULL);
    void dropTabletOwnership(uint64_t tabletId,
                             uint64_t firstKey,
                             uint64_t lastKey);
    void splitMasterTablet(uint64_t tableId,
                           uint64_t startKeyHash,
                           uint64_t endKeyHash,
                           uint64_t splitKeyHash);
    void write(uint64_t tableId, const char* key, uint16_t keyLength,
               const void* buf, uint32_t length,
               const RejectRules* rejectRules = NULL, uint64_t* version = NULL,
               bool async = false);

  protected:
    Transport::SessionRef session;
    DISALLOW_COPY_AND_ASSIGN(MasterClient);
};
} // namespace RAMCloud

#endif // RAMCLOUD_MASTERCLIENT_H
