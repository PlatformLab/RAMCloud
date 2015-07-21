/* Copyright (c) 2009-2015 Stanford University
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

#ifndef RAMCLOUD_BACKUPMASTERRECOVERY_H
#define RAMCLOUD_BACKUPMASTERRECOVERY_H

#include "Common.h"
#include "BackupStorage.h"
#include "Log.h"
#include "ProtoBuf.h"
#include "Segment.h"
#include "ServerId.h"
#include "TaskQueue.h"
#include "WireFormat.h"

namespace RAMCloud {

class BackupReplicaMetadata;

/**
 * All aspects of a single recovery of a crashed master including algorithms
 * and state. This includes both phases of recovery: discovery of log
 * data and replicas by the coordinator and collection of objects by the
 * recovery masters. This class is structed as a Task so that primary
 * replicas can be loaded and filtered in the background.
 * Parts of recovery that must muck with the contents of replicas has been
 * pulled out into RecoverySegmentBuilder for testing convenience as much
 * as anything else.
 *
 * Notice, if this same master is recovered again by the coordinator
 * (because the original recovery wasn't completely successful) then this
 * instance must be replaced with another for the new recovery; the state
 * from this recovery cannot be reused for future recoveries of the same
 * master.
 *
 * BackupService uses this class by creating an instance and calling start().
 * The recovery works in the background and the BackupService makes calls
 * to getRecoverySegment() to collect the output. The service may also call
 * free() to ask the recovery to clean itself up at any time.
 *
 * Concurrecy/thread-safety:
 * This class only safe under a couple of strong assumptions. Individual
 * methods are not thread-safe unless stated otherwise. Assumptions this
 * class relies on:
 * 1) There is exactly one backup worker thread.
 * 2) Calls to performTask() are serialized.
 * 3) FrameRefs delivered to start() remain valid until destruction.
 *
 * Primary replicas are ONLY filtered by the task queue thread serially.
 * Secondary replicas are ONLY filtered by the sole backup worked thread
 * (and, hence, serially, as well).
 * The only miniscule synchronization it to ensure that all built
 * recovery segment information is flushed to main memory before it is used
 * by getRecoverySegment().
 *
 * Destruction is non-trivial because the instance may become redundant while
 * it is filtering a replica. To solve this, users call free() which instructs
 * the recovery to cleanup at its earliest convenience.
 */
class BackupMasterRecovery : public Task {
  PUBLIC:
    typedef WireFormat::BackupStartReadingData::Response StartResponse;

    BackupMasterRecovery(TaskQueue& taskQueue,
                         uint64_t recoveryId,
                         ServerId crashedMasterId,
                         uint32_t segmentSize);
    void start(const std::vector<BackupStorage::FrameRef>& frames,
               Buffer* buffer,
               StartResponse* response);
    void setPartitionsAndSchedule(ProtoBuf::RecoveryPartition partitions);
    Status getRecoverySegment(uint64_t recoveryId,
                              uint64_t segmentId,
                              int partitionId,
                              Buffer* buffer,
                              SegmentCertificate* certificate);
    void free();
    uint64_t getRecoveryId();
    void performTask();

  PRIVATE:
    void populateStartResponse(Buffer* buffer,
                               StartResponse* response);
    struct Replica;
    void buildRecoverySegments(Replica& replica);
    bool getLogDigest(Replica& replica, Buffer* digestBuffer);

    /**
     * Which master recovery this is for. The coordinator may schedule
     * multiple recoveries for a single master (though, it only schedules one
     * at a time). The backup only keeps one recovery around for a particular
     * master at a time, calling free() any BackupMasterRecovery for before
     * creating a new one. This id is how the backup distiguishes between
     * retries for the start of a recovery from the start of a new recovery.
     * See BackupService::startReadingData().
     */
    uint64_t recoveryId;

    /**
     * Server id of the master which has crashed whose replicas should be
     * processed and made available to recovery masters.
     */
    ServerId crashedMasterId;

    /**
     * Describes how the coordinator would like the backup to split up the
     * contents of the replicas for delivery to different recovery masters.
     * The partition ids inside each entry act as an index describing which
     * recovery segment for a particular replica each object should be placed
     * in.
     */
    Tub<ProtoBuf::RecoveryPartition> partitions;

    /**
     * Size of the replicas on storage. Needed for bounds-checking on the
     * SegmentIterators which walk the stored replicas.
     */
    uint32_t segmentSize;

    /**
     * Number of distinct partitions in #partitions. Computed immediately
     * at the start of the constructor from #partitions. Notice, this is
     * NOT the same at the number of entries in #partitions; multiple entries
     * in #partitions may have the same partition id. One more than the
     * largest id found among the partitions.
     */
    int numPartitions;

    /**
     * Keeps all state for each replica that is part of this master recovery.
     * All information used by the backup to create recovery segments is
     * taken from #frame and #metadata to try to keep the information
     * used in the recovery as close to what is on storage as possible in
     * an attempt to reduce surprises. Once the replica has been filtered
     * either #recoverySegments or #recoveryException is populated.
     * Concurrency on these replicas is hairy. Primary replicas are ALWAYS
     * filtered by the task queue thread; secondary threads are ALWAYS
     * filtered by the backup service worker thread. There is no
     * locking; once #built is set the backup worker thread can safely
     * check #recoverySegments and #recoveryException (after an lfence,
     * which it does ONLY in BackupMasterRecovery::getRecoverySegment()).
     */
    struct Replica {
        explicit Replica(const BackupStorage::FrameRef& frame);

        /**
         * Reference counted pointer to a storage frame from which replica
         * data and metadata is to be extracted. Because frames are only
         * freed for reuse on storage when their refcount drops to zero
         * this recovery can be sure the frame will stay valid throughout
         * the duration of recovery.
         */
        BackupStorage::FrameRef frame;

        /**
         * Convenient name for getting at and interpreting the metadata block
         * associated with the replica.
         */
        const BackupReplicaMetadata* metadata;

        /**
         * Once filtered points to the start of an array of #numPartitions
         * recovery segments. Constructed in buildRecoverySegments() and
         * appended to rpc response buffers in getRecoverySegment().
         * Because primary replicas are constructed on another thread
         * access to this field must be synchronized through #built
         * (since this isn't a raw pointer setting/testing it is not atomic).
         * An sfence is performed after recovery segments are constructed.
         * After filtering either this field is set or #recoveryException
         * is set.
         */
        std::unique_ptr<Segment[]> recoverySegments;

        /**
         * Set if a there was a problem filtering a replica. For example,
         * if the replica checksum doesn't match what is found on storage.
         * This is exception is returned to recovery masters to let them
         * know the replica won't become available on this backup. This
         * field has all the same synchronization issues as those mentioned
         * above for #recoverySegments. After filtering, if this is set
         * then #recoverySegments remains unset.
         */
        std::unique_ptr<SegmentRecoveryFailedException> recoveryException;

        /**
         * Set when the replica has been filtered and all recovery segment
         * information has been flushed to main memory (via sfence).
         * Set in buildRecoverySegments(); checked in getRecoverySegment().
         * A corresponding lfence must be done before checking this field
         * to ensure the local cpu's cache is consistent with the recovery
         * segments in main memory.
         */
        bool built;

        DISALLOW_COPY_AND_ASSIGN(Replica);
    };

    /**
     * Recovery state for each replica that is part of the recovery.
     * Stored in two "halves". The first part contains all replicas
     * which are marked as primaries in a random order which helps masters
     * to stay busy even if the log contains long sequences of back-to-back
     * replicas that only have objects for a particular partition. The
     * second part contains all secondary replicas; secondary order is
     * irrelevant and is left in the order provided to start().
     * Primary replicas are queued for load from storage in the order
     * they appear here. Secondary replicas are only loaded and filtered
     * on demand. Populated in start().
     */
    std::deque<Replica> replicas;

    /**
     * Tracks which primary replica is the next to be filtered in the
     * background. Set initially in start() when replicas is constructed,
     * and used/incremented in performTask() as replicas are filtered.
     */
    std::deque<Replica>::iterator nextToBuild;

    /**
     * Points to the first secondary replica in #replicas. Lets background
     * filtering know where it should stop when pre-loading/filtering replicas.
     * Set in start() when replicas is constructed.
     */
    std::deque<Replica>::iterator firstSecondaryReplica;

    /**
     * Maps segment ids to the corresponding replica in #replicas.
     * Populated in start() along with #replicas. Used by getRecoverySegment()
     * to find the Replica for which a recovery segment is being requested
     * by a recovery master.
     */
    std::unordered_map<uint64_t, Replica*> segmentIdToReplica;

    /**
     * Caches the log digest extracted from the replicas for this
     * crashed master, if any.
     */
    Buffer logDigest;

    /**
     * Caches the segment id stored in the metadata for the replica from
     * which the log digest was extracted for this crashed master, if any.
     */
    uint64_t logDigestSegmentId;

    /**
     * Caches the replica epoch stored in the metadata for the replica from
     * which the log digest was extracted for this crashed master, if any.
     * Used by the coordinator to detect if the replica from which this
     * digest was extracted could have been inconsistent. If it might have
     * been, then the coordinator will discard the returned digest.
     */
    uint64_t logDigestSegmentEpoch;

    /**
     * Caches the table stats digest from the replicas for this crashed master,
     * if any.
     */
    Buffer tableStatsDigest;

    /**
     * Indicates whether start() should scan all the replicas to extract
     * information or whether the results are already cached.
     * Used by start() to ensure replica information is only extracted once.
     * Note, this is NOT just an optimization; this is used to ensure
     * that the StartReadingData rpc is idempotent. That is, replicas
     * don't come and go across repeated calls for the same recovery.
     */
    bool startCompleted;

    /**
     * If true inidcates that this recovery should clean up and release
     * all reousrces and delete itself on the next call to performTask().
     * Set by free() which also schedules this task to be invoked.
     */
    bool freeQueued;

    /**
     * Times each recovery.
     */
    Tub<CycleCounter<RawMetric>> recoveryTicks;

    /**
     * Times duration that replicas are being read from disk.
     */
    Tub<CycleCounter<RawMetric>> readingDataTicks;

    /**
     * Tracks when this task was scheduled to start background filtering
     * of replicas. Used in performTask() to determine how long filtering
     * took after the last replica has been filtered.
     */
    uint64_t buildingStartTicks;

    /**
     * If set call this function instead of
     * RecoverySegmentBuilder::extractDigest() during start().
     */
    bool (*testingExtractDigest)(uint64_t segmentId,
                                 Buffer* digestBuffer,
                                 Buffer* tableStatsBuffer);

    /**
     * If true skip calls to RecoverySegmentBuilder::build() in
     * buildRecoverySegments().
     */
    bool testingSkipBuild;
    DISALLOW_COPY_AND_ASSIGN(BackupMasterRecovery);
};

/**
 * Metadata stored along with each replica on storage.
 * Contains:
 *  1) A checksum to check the integrity of the fields of this struct.
 *  2) A certificate which is used to check the integrity of the internal
 *     log entry metadata in the replica associated with this metadata struct.
 *  3) Any details needed about the replica during master recovery (even across
 *     restart).
 */
class BackupReplicaMetadata {
  PUBLIC:
    /**
     * Create metadata and seal it with a checksum.
     * See below for the meaning of each of the fields.
     */
    BackupReplicaMetadata(const SegmentCertificate& certificate,
                          uint64_t logId,
                          uint64_t segmentId,
                          uint32_t segmentCapacity,
                          uint64_t segmentEpoch,
                          bool closed,
                          bool primary)
        : certificate(certificate)
        , logId(logId)
        , segmentId(segmentId)
        , segmentCapacity(segmentCapacity)
        , segmentEpoch(segmentEpoch)
        , closed(closed)
        , primary(primary)
        , checksum()
    {
        Crc32C calculatedChecksum;
        calculatedChecksum.update(this, static_cast<unsigned>
                                  (sizeof(*this) - sizeof(checksum)));
        checksum = calculatedChecksum.getResult();
    }

    /**
     * Checksum the fields of this metadata and compare them to the
     * checksum that is stored are part of the metadata. Used to
     * ensure the fields weren't corrupted on storage. Only used
     * on backup startup, which is the only time metadata is ever
     * loaded from storage.
     */
    bool checkIntegrity() const {
        Crc32C calculatedChecksum;
        calculatedChecksum.update(this, static_cast<unsigned>
                                  (sizeof(*this) - sizeof(checksum)));
        return calculatedChecksum.getResult() == checksum;
    }

    /**
     * Used to check the integrity of the replica stored in the same
     * storage frame as this metadata. Supplied by masters on calls
     * to append data to replicas.
     */
    SegmentCertificate certificate;

    /**
     * Particular log the replica stored in the same storage frame
     * as this metadata is part of. Used to restart to take
     * inventory of which replicas are (likely) on storage and
     * will be available for recoveries.
     */
    uint64_t logId;

    /**
     * Particular segment the replica stored in the same storage frame
     * as this metadata is a replica of. Used to restart to take
     * inventory of which replicas are (likely) on storage and
     * will be available for recoveries.
     */
    uint64_t segmentId;

    /**
     * Space on storage allocated to the replica. Used to ensure that
     * if metadata is somehow found on disk after restarting a
     * backup with a different segment size the replica isn't used.
     */
    uint32_t segmentCapacity;

    /**
     * Which epoch of the segment the replica was most recently written
     * during. Used during recovery by the coordinator to filter out
     * replicas which may have become inconsistent. When a master loses
     * contact with a backup to which it was replicating an open segment
     * the master increments this epoch, ensures that it has a sufficient
     * number of replicas tagged with the new epoch number, and then tells
     * the coordinator of the new epoch so the coordinator can filter out
     * the stale replicas from earlier epochs.
     */
    uint64_t segmentEpoch;

    /**
     * Whether the replica was closed by the master which
     * created it. Only has meaning to higher level recovery code
     * which uses it to preserve consistency properties of a
     * master's replicated log.
     */
    bool closed;

    /**
     * Whether the replica is a primary and should be loaded in at the start
     * of recovery. Used by the backup to determine which replicas to load
     * and by the coordinator to order recovery segment requests to try to
     * recover entirely from primary replicas.
     */
    bool primary;

  PRIVATE:
    /**
     * Checksum of all the above fields. Must come last in the class.
     * Populated on construction; can be checked with checkIntegrity().
     * Protects the fields of this structure from corruption on/by
     * storage.
     */
    Crc32C::ResultType checksum;
} __attribute__((packed));
// Substitute for std::is_trivially_copyable until we have real C++11.
static_assert(sizeof(BackupReplicaMetadata) == 42,
              "Unexpected padding in BackupReplicaMetadata");

} // namespace RAMCloud

#endif
