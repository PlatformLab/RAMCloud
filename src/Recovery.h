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


#ifndef RAMCLOUD_RECOVERY_H
#define RAMCLOUD_RECOVERY_H

#include <map>
#include <boost/unordered_map.hpp>

#include "Common.h"
#include "CycleCounter.h"
#include "Log.h"
#include "Metrics.h"
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
    BaseRecovery() {}
    virtual ~BaseRecovery() {}
    virtual void start()
    {}
    /**
     * Used to check constructor args for mock recoveries.  A normal
     * constructor cannot be used because the mocks are instantiated
     * early.
     */
    virtual void operator()(uint64_t masterId,
                            const ProtoBuf::Tablets& will,
                            const ProtoBuf::ServerList& masterHosts,
                            const ProtoBuf::ServerList& backupHosts)
    {}
    virtual bool tabletsRecovered(const ProtoBuf::Tablets& tablets)
    { return true; }
    DISALLOW_COPY_AND_ASSIGN(BaseRecovery);
};

/**
 * A Recovery from the perspective of the CoordinatorServer.
 */
class Recovery : public BaseRecovery {
  public:
    Recovery(uint64_t masterId,
             const ProtoBuf::Tablets& will,
             const ProtoBuf::ServerList& masterHosts,
             const ProtoBuf::ServerList& backupHosts);
    ~Recovery();

    void buildSegmentIdToBackups();
    void createBackupList(ProtoBuf::ServerList& backups) const;
    void start();
    bool tabletsRecovered(const ProtoBuf::Tablets& tablets);

  private:
    // Only used in Recovery::buildSegmentIdToBackups().
    struct BackupStartTask {
        BackupStartTask(const ProtoBuf::ServerList::Entry& backupHost,
             uint64_t crashedMasterId,
             const ProtoBuf::Tablets& partitions)
            : backupHost(backupHost)
            , response()
            , client()
            , rpc()
            , result()
            , done()
        {
            response.construct();
            client.construct(
                transportManager.getSession(
                    backupHost.service_locator().c_str()));
            rpc.construct(*client, crashedMasterId, partitions);
            LOG(DEBUG, "Starting startReadingData on %s",
                backupHost.service_locator().c_str());
        }

        bool isDone() const { return done; }
        bool isReady() { return rpc && rpc->isReady(); }

        void
        operator()()
        {
            (*rpc)(&result);
            rpc.destroy();
            client.destroy();
            response.destroy();
            done = true;
        }

        const ProtoBuf::ServerList::Entry& backupHost;
        Tub<Buffer> response;
        Tub<BackupClient> client;
        Tub<BackupClient::StartReadingData> rpc;
        BackupClient::StartReadingData::Result result;
        bool done;
        DISALLOW_COPY_AND_ASSIGN(BackupStartTask);
    };

    class SegmentAndDigestTuple {
      public:
        SegmentAndDigestTuple(uint64_t segmentId, uint32_t segmentLength,
            const void* logDigestPtr, uint32_t logDigestBytes)
            : segmentId(segmentId),
              segmentLength(segmentLength),
              logDigest(logDigestPtr, logDigestBytes)
        {
        }

        uint64_t  segmentId;
        uint32_t  segmentLength;
        LogDigest logDigest;
    };

    CycleCounter<Metric> recoveryTicks;

    /**
     * A mapping of segmentIds to backup host service locators.
     * Created from #hosts in createBackupList().
     */
    ProtoBuf::ServerList backups;

    /// The list of all masters.
    const ProtoBuf::ServerList& masterHosts;

    /// The list of all backups.
    const ProtoBuf::ServerList& backupHosts;

    /// The id of the crashed master whose is being recovered.
    uint64_t masterId;

    /// Number of tablets left to recover before done.
    uint32_t tabletsUnderRecovery;

    /// A partitioning of tablets for the crashed master.
    const ProtoBuf::Tablets& will;

    /// List of asynchronous startReadingData tasks and their replies
    Tub<BackupStartTask> *tasks;

    friend class RecoveryTest;
    friend class RecoveryInternal::MasterStartTask;
    DISALLOW_COPY_AND_ASSIGN(Recovery);
};

} // namespace RAMCloud

#endif
