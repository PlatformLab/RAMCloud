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


#ifndef RAMCLOUD_RECOVERY_H
#define RAMCLOUD_RECOVERY_H

#include "Common.h"
#include "CoordinatorServerList.h"
#include "CycleCounter.h"
#include "Log.h"
#include "RawMetrics.h"
#include "ProtoBuf.h"
#include "ServerList.pb.h"
#include "Tablets.pb.h"

namespace RAMCloud {

namespace RecoveryInternal {
struct MasterStartTask;
}

/// Used to allow custom mocks of recovery in unit testing.
class BaseRecovery {
  public:
    BaseRecovery() : masterId() {}
    explicit BaseRecovery(ServerId masterId) : masterId(masterId) {}
    virtual ~BaseRecovery() {}
    virtual void start()
    {}
    /**
     * Used to check constructor args for mock recoveries.  A normal
     * constructor cannot be used because the mocks are instantiated
     * early.
     */
    virtual void operator()(ServerId masterId,
                            const ProtoBuf::Tablets& will,
                            const CoordinatorServerList& serverList)
    {}
    virtual bool tabletsRecovered(const ProtoBuf::Tablets& tablets)
    { return true; }

    /// The id of the crashed master whose is being recovered.
    ServerId masterId;

    DISALLOW_COPY_AND_ASSIGN(BaseRecovery);
};

/**
 * A Recovery from the perspective of the CoordinatorService.
 */
class Recovery : public BaseRecovery {
  public:
    Recovery(ServerId masterId,
             const ProtoBuf::Tablets& will,
             const CoordinatorServerList& serverList);
    ~Recovery();

    void buildSegmentIdToBackups();
    void start();
    bool tabletsRecovered(const ProtoBuf::Tablets& tablets);

  PRIVATE:
    // Only used in Recovery::buildSegmentIdToBackups().
    class BackupStartTask {
      PUBLIC:
        BackupStartTask(const CoordinatorServerList::Entry& backupHost,
             ServerId crashedMasterId,
             const ProtoBuf::Tablets& partitions, uint64_t minOpenSegmentId);
        bool isDone() const { return done; }
        bool isReady() { return rpc && rpc->isReady(); }
        void send();
        void filterOutInvalidReplicas();
        void wait();
        const CoordinatorServerList::Entry backupHost;
      PRIVATE:
        const ServerId crashedMasterId;
        const ProtoBuf::Tablets& partitions;
        const uint64_t minOpenSegmentId;
        Tub<BackupClient> client;
        Tub<BackupClient::StartReadingData> rpc;
      PUBLIC:
        BackupClient::StartReadingData::Result result;
      PRIVATE:
        bool done;
        DISALLOW_COPY_AND_ASSIGN(BackupStartTask);
    };

    class SegmentAndDigestTuple {
      public:
        SegmentAndDigestTuple(const char* backupServiceLocator,
                              uint64_t segmentId, uint32_t segmentLength,
            const void* logDigestPtr, uint32_t logDigestBytes)
            : backupServiceLocator(backupServiceLocator),
              segmentId(segmentId),
              segmentLength(segmentLength),
              logDigest(logDigestPtr, logDigestBytes)
        {
        }

        const char* backupServiceLocator;
        uint64_t  segmentId;
        uint32_t  segmentLength;
        LogDigest logDigest;
    };

    CycleCounter<RawMetric> recoveryTicks;

    /**
     * A mapping of segmentIds to backup host service locators.
     * Created from #hosts in createBackupList().
     */
    vector<RecoverRpc::Replica> replicaLocations;

    /// The list of all masters.
    const CoordinatorServerList& serverList;

    /// Number of tablets left to recover before done.
    uint32_t tabletsUnderRecovery;

    /// A partitioning of tablets for the crashed master.
    const ProtoBuf::Tablets& will;

    friend class RecoveryInternal::MasterStartTask;
    DISALLOW_COPY_AND_ASSIGN(Recovery);
};

} // namespace RAMCloud

#endif
