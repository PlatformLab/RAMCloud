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

#include "Atomic.h"
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
  * This class represents a mechanism for deleting other tasks. After it is
  * scheduled, the target task should never again be scheduled. It lives in
  * this file because it is currently used only by BackupMasterRecovery.
  */
class TaskKiller : public Task {
  PUBLIC:
    TaskKiller(TaskQueue& taskQueue, Task* taskToKill);
    void performTask();
  PRIVATE:
    Task* taskToKill;
    DISALLOW_COPY_AND_ASSIGN(TaskKiller);
};


/**
 * All aspects of a single recovery of a crashed master including algorithms
 * and state. This includes both phases of recovery: discovery of log
 * data and replicas by the coordinator and collection of objects by the
 * recovery masters. This class is structed as a Task so that replicas can be
 * loaded and filtered in the background. Parts of recovery that must muck with
 * the contents of replicas has been pulled out into RecoverySegmentBuilder for
 * testing convenience as much as anything else.
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
 * Both primary and secondary replicas are ONLY filtered by the task queue
 * thread serially. The only difference between primary and secondary replicas
 * is that primary replicas are loaded automatically and secondary replicas are
 * not loaded until they are requested.
 *
 * The number of replicas in memory at any given time is limited to prevent
 * out-of-memory errors. In the case of extreme speed differences between
 * recovery masters, this can cause longer recovery times, but under normal
 * circumstances it shouldn't be noticeable.
 *
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
                         uint32_t segmentSize,
                         uint32_t readSpeed,
                         uint32_t maxReplicasInMemory);
    ~BackupMasterRecovery();
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
     * Concurrency on these replicas is hairy. Both primary and secondary
     * replicas are ALWAYS filtered by the task queue thread. There is only
     * locking when a replica's access data is being altered by
     * CyclicReplicaBuffer; once #built is set the backup worker thread can
     * safely check #recoverySegments and #recoveryException (after an lfence,
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
         * Because all replicas are constructed on another thread
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

        /**
         * Used by CyclicReplicaBuffer to keep track of the last time
         * information from this replica was sent to a recovery master. See
         * CyclicReplicaBuffer::bufferNext for details.
         */
        Atomic<uint64_t> lastAccessTime;

        /**
         * Used by CyclicReplicaBuffer::ActiveReplica to keep track of how many
         * getRecoveryData RPCs are currently being served for this replica.
         */
        Atomic<int> refCount;

        /**
         * Used by CyclicReplicaBuffer to keep track of how many times the data
         * for this replica has been returned to a server. Over the lifetime of
         * a recovery, we expect the fetchCount for each replica to equal the
         * number of recovery partitions.
         */
        Atomic<int> fetchCount;

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
     * Maps segment ids to the corresponding replica in #replicas.
     * Populated in start() along with #replicas. Used by getRecoverySegment()
     * to find the Replica for which a recovery segment is being requested
     * by a recovery master.
     */
    std::unordered_map<uint64_t, Replica*> segmentIdToReplica;

    /**
     * This class limits the number of replicas stored in memory at any given
     * point during recovery. It exists for the entire lifetime of a recovery,
     * and throughout the course of a recovery will store each replica in memory
     * at least once. The order in which replicas are added to the buffer is the
     * same order in which they are provided to the recovery (i.e., the order
     * servers will request them in).
     *
     * Under normal operation, the life of a replica in this buffer is as
     * follows:
     *   1. The replica is enqueued at NORMAL priority at the start of recovery.
     *      An enqueued replica is not considered part of the buffer yet because
     *      it has no data stored in memory.
     *   2. The enqueued replica is transferred to the buffer via bufferNext().
     *      At this point, an asynchronous disk read is scheduled for this
     *      replica. When complete, we can access the replica's frame for data.
     *   3. After the frame is in memory, the replica is partitioned into
     *      recovery segments by buildNext() and its frame data is freed.
     *   4. The replica's partitioned segments are fetched by each of the
     *      servers performing recovery.
     *   5. The partitioned data is freed and the replica is replaced by another
     *      enqueued replica.
     *
     * The eviction of replicas in the buffer happens in a cyclic fashion, based
     * on a timeout. If a replica is evicted before every server has requested
     * its data, the replica will be enqueued again with HIGH priority when its
     * data is requested again. For details about eviction policy, see
     * bufferNext().
     */
    class CyclicReplicaBuffer {
      PUBLIC:
        CyclicReplicaBuffer(uint32_t maxNumEntries, uint32_t segmentSize,
                            uint32_t readSpeed, BackupMasterRecovery* recovery);
        ~CyclicReplicaBuffer();

        size_t size();
        bool contains(Replica* replica);

        enum Priority { NORMAL, HIGH };
        void enqueue(Replica* replica, Priority priority);
        bool bufferNext();
        bool buildNext();

        void logState();

        /**
         * This class keeps track of access to replicas. For a given replica,
         * the number of ActiveReplicas in existence equals the number of
         * getRecoveryData RPCs currently being served for that replica. The
         * lastAccessTime for a replica is the last time it had an ActiveReplica
         * constructed. A replica's refCount is incremented by 1 for the
         * lifetime of its ActiveReplica.
         *
         * This class needs to be in CyclicReplicaBuffer to access the buffer's
         * private mutex, to prevent a race where a replica is evicted while
         * active.
         */
        class ActiveReplica {
          PUBLIC:
            ActiveReplica(Replica* replica, CyclicReplicaBuffer* buffer)
                : replica(replica)
                , buffer(buffer)
            {
                SpinLock::Guard lock(buffer->mutex);
                replica->refCount++;
                replica->lastAccessTime = Cycles::rdtsc();
            }

            ~ActiveReplica()
            {
                replica->refCount--;
            }

          PRIVATE:
            Replica* replica;
            CyclicReplicaBuffer* buffer;

            DISALLOW_COPY_AND_ASSIGN(ActiveReplica);
        };

      PRIVATE:
        /**
         * Used to lock all operations on this buffer.
         */
        SpinLock mutex;

        /**
         * The maximum number of replicas with data in memory at any given time.
         */
        uint32_t maxReplicasInMemory;

        /**
         * A circularly ordered list of replicas either currently in memory or
         * with a disk read pending. Replicas in this vector are considered in
         * the buffer.
         */
        std::vector<Replica*> inMemoryReplicas;

        /**
         * The index into replicasInMemory of the oldest replica in the buffer.
         * The age of the replicas proceeds in descending order starting at this
         * index and wrapping around.
         */
        size_t oldestReplicaIdx;

        /**
         * A queue containing replias enqueued with normal priority. See
         * enqueue() for details about normal vs. high priority.
         */
        std::deque<Replica*> normalPriorityQueuedReplicas;

        /**
         * A queue containing replicas enqueued with high priority. See
         * enqueue() for details about normal vs. high priority.
         */
        std::deque<Replica*> highPriorityQueuedReplicas;

        /**
         * The amount of time (in seconds) it takes to read maxReplicasInMemory
         * replicas from disk. This is used by bufferNext() to decide on an
         * eviction timeout.
         */
        double bufferReadTime;

        /**
         * A reference to the recovery that owns this buffer.
         */
        BackupMasterRecovery* recovery;

        DISALLOW_COPY_AND_ASSIGN(CyclicReplicaBuffer);
    };
    CyclicReplicaBuffer replicaBuffer;

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

    /**
     * The Task that is scheduled to delete this BackupMasterRecovery when it
     * is no longer needed.
     */
    TaskKiller destroyer;

    /**
     * When this flag is set, a deletion task has been enqueued, and this objct
     * should cease to reschedule itself.
     */
    bool pendingDeletion;

    /**
     * This SpinLock is used to ensure that pendingDeletion is set atomically
     * with the schedule of destroyer. Otherwise, we may get into the following
     * scenario:
     * 1. performTask reads the flag as clear
     * 2. free() function sets the flag.
     * 3. free() function enqueues the deletion task.
     * 4. performTask schedules itself.
     *
     * Given that only one task can be executed at a time, we can share the
     * same lock across multiple tasks.
     */
    static SpinLock deletionMutex;

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
     * Whether the replica is a primary and should be scheduled for loading at
     * the start of recovery. Used by the backup to determine which replicas to
     * load and by the coordinator to order recovery segment requests to try to
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
