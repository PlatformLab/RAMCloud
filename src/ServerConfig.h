/* Copyright (c) 2012-2015 Stanford University
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

#ifndef RAMCLOUD_SERVERCONFIG_H
#define RAMCLOUD_SERVERCONFIG_H

#include "Log.h"
#include "Seglet.h"
#include "Segment.h"
#include "ServiceMask.h"
#include "HashTable.h"

#include "ServerConfig.pb.h"

namespace RAMCloud {

/**
 * Configuration details for a Server; describes which coordinator to enlist
 * with, which services to run and advertise, and the configuration details
 * for each of those services.
 *
 * Used for command-line arguments along with some additional details.  Also
 * used for configuring lightweight servers for unit testing (see MockCluster).
 *
 * Instances for unit-testing are created using the forTesting() static method
 * which tries to provide some sane defaults for testing.
 *
 * A protocol buffer containing all of the same fields is used to serialize a
 * configuration and return it to curious client machines. Whenever parameters
 * are added to or removed from this file, please update ServerConfig.proto.
 */
struct ServerConfig {
  private:
    /// Piece of junk used internally to select between (private) constructors.
    static struct Testing {} testing;

    /**
     * Private constructor used to generate a ServerConfig well-suited to most
     * unit tests.  See forTesting() to actually create a ServerConfig.
     */
    ServerConfig(Testing) // NOLINT
        : coordinatorLocator()
        , localLocator()
        , clusterName("__unnamed__")
        , services{WireFormat::MASTER_SERVICE, WireFormat::BACKUP_SERVICE,
                   WireFormat::MEMBERSHIP_SERVICE}
        , preferredIndex(0)
        , detectFailures(false)
        , pinMemory(false)
        , segmentSize(128 * 1024)
        , segletSize(128 * 1024)
        , maxObjectDataSize(segmentSize / 4)
        , maxObjectKeySize((64 * 1024) - 1)
        , maxCores(2)
        , master(testing)
        , backup(testing)
    {}

    /**
     * Private constructor used to generate a ServerConfig for running a real
     * RAMCloud server.  The defaults here mean very little; main() sets
     * these fields explicitly.  See forExecution() to actually create a
     * ServerConfig.
     */
    ServerConfig()
        : coordinatorLocator()
        , localLocator()
        , clusterName("__unnamed__")
        , services{WireFormat::MASTER_SERVICE, WireFormat::BACKUP_SERVICE,
                   WireFormat::PING_SERVICE, WireFormat::MEMBERSHIP_SERVICE}
        , preferredIndex(0)
        , detectFailures(true)
        , pinMemory(true)
        , segmentSize(Segment::DEFAULT_SEGMENT_SIZE)
        , segletSize(Seglet::DEFAULT_SEGLET_SIZE)
        , maxObjectDataSize(segmentSize / 8)
        , maxObjectKeySize((64 * 1024) - 1)
        , maxCores(2)
        , master()
        , backup()
    {}

  public:
    /**
     * Return a ServerConfig which is well-suited for most unit tests.
     * Callers will want to override some of the fields before passing it
     * on to MockCluster::addServer().  In particular, it is important
     * to select which services will run as part of the server via the
     * #services field.
     *
     * See ServerConfig(Testing) for the default values.
     */
    static ServerConfig forTesting() {
        return {testing};
    }

    /**
     * Return a ServerConfig which suited for running a real RAMCloud
     * server; used in main() to hold many of the command-line options.
     *
     * See ServerConfig() for the default values, though these mean little
     * since main() sets the fields explicitly based on command-line
     * args.
     */
    static ServerConfig forExecution() {
        return {};
    }

    /**
     * Serialize this ServerConfiguration to the given protocol buffer. This is
     * typically used to peek at a running server's configuration from a remote
     * machine.
     */
    void
    serialize(ProtoBuf::ServerConfig& config) const
    {
        config.set_coordinator_locator(coordinatorLocator);
        config.set_local_locator(coordinatorLocator);
        config.set_cluster_name(clusterName);
        config.set_services(services.toString());
        config.set_detect_failures(detectFailures);
        config.set_pin_memory(pinMemory);
        config.set_segment_size(segmentSize);
        config.set_seglet_size(segletSize);
        config.set_max_object_data_size(maxObjectDataSize);
        config.set_max_object_key_size(maxObjectKeySize);
        config.set_max_cores(maxCores);

        if (services.has(WireFormat::MASTER_SERVICE))
            master.serialize(*config.mutable_master());

        if (services.has(WireFormat::BACKUP_SERVICE))
            backup.serialize(*config.mutable_backup());
    }

    /// A locator the server can use to contact the cluster coordinator.
    string coordinatorLocator;

    /// The locator the server should listen for incoming messages on.
    string localLocator;

    /**
     * Controls the reuse of replicas stored on this backup.  'Tags' replicas
     * created on this backup with this cluster name.  This has two effects.
     * First, any replicas found in storage are discarded unless they are
     * tagged with an identical cluster name. Second, any replicas created by
     * the backup process will only be reused by future backup processes if the
     * cluster name on the stored replica matches the cluster name of future
     * process. The name '__unnamed__' is special and never matches any cluster
     * name (even itself), so it guarantees all stored replicas are discarded
     * on start and that all replicas created by this process are discarded by
     * future backups.  This is convenient for testing.
     */
    string clusterName;

    /// Which services this server should run and advertise to the cluster.
    ServiceMask services;

    /// If nonzero, indicates a particular index number that this server
    /// would like for its server id, if available.
    uint32_t preferredIndex;

    /// Whether the failure detection thread should be started.
    bool detectFailures;

    /**
     * Whether the entire address space of the server should be pinned
     * after initialization has finished.  This can take awhile so we want
     * to skip it for unit tests.
     */
    bool pinMemory;

    /**
     * Size of segment in the master log and of  replicas which will be stored
     * on the backup.  Using small segmentSize (~64 KB) improves unit testing
     * time substantially.
     */
    uint32_t segmentSize;

    /**
     * Size of each virtually contiguous chunk of a segment. segmentSize must be
     * an integer multiple of this value. Smaller seglets decrease fragmentation,
     * but cause more objects to be stored discontiguously.
     */
    uint32_t segletSize;

    /**
     * Largest allowable RAMCloud object, in bytes.  It's not clear whether
     * we will always need a size limit, or what the limit should be. For now
     * this guarantees that an object will fit inside a single rpc and segment.
     */
    uint32_t maxObjectDataSize;

    /**
     * Largest allowable key for a RAMCloud object, in bytes.
     */
    uint16_t maxObjectKeySize;

    /**
     *  The maximum number of cores that this server should use under
     * normal conditions for the dispatcher and worker threads. The
     * server may need to exceed this number occasionally (e.g. to
     * avoid distribute deadlocks), but it should limit itself to this
     * value as much as possible.
     */
    uint32_t maxCores;

    /**
     * Configuration details specific to the MasterService on a server,
     * if any.  If !config.has(MASTER_SERVICE) then this field is ignored.
     */
    struct Master {
        /**
         * Constructor used to generate a configuration well-suited to most
         * unit tests.
         */
        Master(Testing) // NOLINT
            : logBytes(40 * 1024 * 1024)
            , hashTableBytes(1 * 1024 * 1024)
            , disableLogCleaner(true)
            , disableInMemoryCleaning(true)
            , diskExpansionFactor(1.0)
            , cleanerBalancer("tombstoneRatio:0.40")
            , cleanerWriteCostThreshold(0)
            , cleanerThreadCount(1)
            , numReplicas(0)
            , useMinCopysets(false)
            , allowLocalBackup(false)
        {}

        /**
         * Constructor used to generate a configuration for running a real
         * RAMCloud server.  The values here are mostly irrelevant since
         * fields are set explicitly from command-line arguments in main().
         */
        Master()
            : logBytes()
            , hashTableBytes()
            , disableLogCleaner()
            , disableInMemoryCleaning()
            , diskExpansionFactor()
            , cleanerBalancer()
            , cleanerWriteCostThreshold()
            , cleanerThreadCount()
            , numReplicas()
            , useMinCopysets()
            , allowLocalBackup()
        {}

        /**
         * Serialize this master configuration to the provided protocol buffer.
         */
        void
        serialize(ProtoBuf::ServerConfig_Master& config) const
        {
            config.set_log_bytes(logBytes);
            config.set_hash_table_bytes(hashTableBytes);
            config.set_disable_log_cleaner(disableLogCleaner);
            config.set_disable_in_memory_cleaning(disableInMemoryCleaning);
            config.set_backup_disk_expansion_factor(diskExpansionFactor);
            config.set_cleaner_balancer(cleanerBalancer);
            config.set_cleaner_write_cost_threshold(cleanerWriteCostThreshold);
            config.set_cleaner_thread_count(cleanerThreadCount);
            config.set_num_replicas(numReplicas);
            config.set_use_mincopysets(useMinCopysets);
            config.set_use_local_backup(allowLocalBackup);
        }

        /**
         * Deserialize the provided protocol buffer into this master
         * configuration.
         */
        void
        deserialize(ProtoBuf::ServerConfig_Master& config)
        {
            logBytes = config.log_bytes();
            hashTableBytes = config.hash_table_bytes();
            disableLogCleaner = config.disable_log_cleaner();
            disableInMemoryCleaning = config.disable_in_memory_cleaning();
            diskExpansionFactor = config.backup_disk_expansion_factor();
            cleanerBalancer = config.cleaner_balancer();
            cleanerWriteCostThreshold = config.cleaner_write_cost_threshold();
            cleanerThreadCount = config.cleaner_thread_count();
            numReplicas = config.num_replicas();
            useMinCopysets = config.use_mincopysets();
            allowLocalBackup = config.use_local_backup();
        }

        /// Total number bytes to use for the in-memory Log.
        uint64_t logBytes;

        /// Total number of bytes to use for the HashTable.
        uint64_t hashTableBytes;

        /// If true, disable the log cleaner entirely.
        bool disableLogCleaner;

        /// If true, the cleaner will not compact in memory and always cleans
        /// on both in memory and on disk.
        bool disableInMemoryCleaning;

        /// Specifies how many segments may be allocated on backup disks beyond
        /// the server's memory capacity. For instance, a value of 2.0 means
        /// that for every full segment's worth of space in the server's memory,
        /// we may allocate two segments on backup disks.
        double diskExpansionFactor;

        /// String specifying which LogCleaner::Balancer to use to schedule
        /// disk cleaning and memory compaction.
        string cleanerBalancer;

        /// If in-memory cleaning is enabled, this specifies the balance between
        /// in-memory and disk cleaning.
        uint32_t cleanerWriteCostThreshold;

        /// If log cleaning is enabled, determines the maximum number of threads
        /// the cleaner will use. Higher values may increase write throughput
        /// at the expense of CPU cycles.
        uint32_t cleanerThreadCount;

        /// Number of replicas to keep per segment stored on backups.
        uint32_t numReplicas;

        /// Specifies whether to use MinCopysets replication or random
        /// replication.
        bool useMinCopysets;

        /// If true, allow replication to local backup.
        bool allowLocalBackup;
    } master;

    /**
     * Configuration details specific to the BackupService a server,
     * if any.  If !config.has(BACKUP_SERVICE) then this field is ignored.
     */
    struct Backup {
        /**
         * Constructor used to generate a configuration well-suited to most
         * unit tests.
         */
        Backup(Testing) // NOLINT
            : gc(false)
            , inMemory(true)
            , sync(false)
            , numSegmentFrames(4)
            , maxNonVolatileBuffers(0)
            , file()
            , strategy(1)
            , mockSpeed(100)
            , writeRateLimit(0)
        {}

        /**
         * Constructor used to generate a configuration for running a real
         * RAMCloud server.  The values here are mostly irrelevant since
         * fields are set explicitly from command-line arguments in main().
         */
        Backup()
            : gc(true)
            , inMemory(false)
            , sync(false)
            , numSegmentFrames(512)
            , maxNonVolatileBuffers(0)
            , file("/var/tmp/backup.log")
            , strategy(1)
            , mockSpeed(0)
            , writeRateLimit(0)
        {}

        /**
         * Serialize this backup configuration to the provided protocol buffer.
         */
        void
        serialize(ProtoBuf::ServerConfig_Backup& config) const
        {
            config.set_gc(gc);
            config.set_in_memory(inMemory);
            config.set_num_segment_frames(numSegmentFrames);
            config.set_max_non_volatile_buffers(maxNonVolatileBuffers);
            if (!inMemory)
                config.set_file(file);
            config.set_strategy(strategy);
            config.set_mock_speed(mockSpeed);
            config.set_write_rate_limit(writeRateLimit);
        }

        /**
         * Deserialize the provided protocol buffer into this backup
         * configuration.
         */
        void
        deserialize(ProtoBuf::ServerConfig_Backup& config)
        {
            gc = config.gc();
            inMemory = config.in_memory();
            numSegmentFrames = config.num_segment_frames();
            maxNonVolatileBuffers = config.max_non_volatile_buffers();
            if (!inMemory)
                file = config.file();
            strategy = config.strategy();
            mockSpeed = config.mock_speed();
            writeRateLimit = config.write_rate_limit();
        }

        /**
         * Whether the BackupService should periodically try to free replicas
         * from its storage that may have become disassociated from their
         * creating master.  This disassociation can happen due to failures of
         * backups.
         */
        bool gc;

        /// Whether the BackupService should store replicas in RAM or on disk.
        bool inMemory;

        /**
         * If true backups block until data from calls to writeSegment have
         * been written to storage. Setting this to false is only safe if
         * backups buffer writes in non-volatile storage (which, for now,
         * they certainly don't).
         */
        bool sync;

        /**
         * Number of replica-sized storage chunks to allocate on the backup's
         * backing store.
         */
        uint32_t numSegmentFrames;

        /**
         * When using disk-based storage for replicas, this specifies the
         * maximum number of segments that may be queued in memory while
         * waiting for the disk to drain writes.
         *
         * If 0, the backup will allow as many buffers as there are segment
         * frames on disk (that is, this parameter will equal numSegmentFrames).
         */
        uint32_t maxNonVolatileBuffers;

        /// Path to a file to use for the backing store if inMemory is false.
        string file;

        /**
         * BackupStrategy to use for balancing replicas across backups.
         * Backups communicate this choice back to MasterServices.
         */
        int strategy;

        /**
         * If mockSpeed is 0 then benchmark the backup's storage and report
         * that to MasterServices.
         * If mockSpeed is non-0 then skip the (slow) benchmarking phase and
         * just report the performance as mockSpeed; in MB/s.
         */
        uint32_t mockSpeed;

        /**
         * If non-0, limit writes to backup to this many megabytes per second.
         */
        size_t writeRateLimit;
    } backup;

  public:
    /**
     * Used in option parsing in main() to help set master.logBytes and
     * master.hashTableBytes.  Only here because there isn't a better place for
     * it.
     *
     * Figure out the Master Server's memory requirements. This means computing
     * the number of bytes to use for the log and the hash table.
     *
     * The user may dictate these parameters by specifying the total memory
     * given to the server, as well as the amount of that to spend on the hash
     * table. The rest is given to the log.
     *
     * Both parameters are string options. If a "%" character is present, they
     * are interpreted as percentages, otherwise they are interpreted as
     * megabytes.
     *
     * \param[in] masterTotalMemory
     *      A string representing the total amount of memory allocated to the
     *      Server. E.g.: "10%" means 10 percent of total system memory, whereas
     *      "256" means 256 megabytes. Only integer quantities are acceptable.
     * \param[in] hashTableMemory
     *      The amount of masterTotalMemory to be used for the hash table. This
     *      may also be a percentage, as above. Only integer quantities are
     *      acceptable.
     * \throw Exception
     *      An exception is thrown if the parameters given are invalid, or
     *      if the total system memory cannot be determined.
     */
    void
    setLogAndHashTableSize(string masterTotalMemory, string hashTableMemory)
    {
        uint64_t masterBytes, hashTableBytes;

        if (masterTotalMemory.find("%") != string::npos) {
            string str = masterTotalMemory.substr(
                0, masterTotalMemory.find("%"));
            uint64_t pct = strtoull(str.c_str(), NULL, 10);
            if (pct <= 0 || pct > 90)
                throw Exception(HERE,
                    "invalid `MasterTotalMemory' option specified: "
                    "not within range 1-90%");
            masterBytes = getTotalSystemMemory();
            if (masterBytes == 0) {
                throw Exception(HERE,
                    "Cannot determine total system memory - "
                    "`MasterTotalMemory' option must not be used");
            }
            masterBytes = (masterBytes * pct) / 100;
        } else {
            masterBytes = strtoull(masterTotalMemory.c_str(), NULL, 10);
            masterBytes *= (1024 * 1024);
        }

        if (hashTableMemory.find("%") != string::npos) {
            string str = hashTableMemory.substr(0, hashTableMemory.find("%"));
            uint64_t pct = strtoull(str.c_str(), NULL, 10);
            if (pct <= 0 || pct > 50) {
                throw Exception(HERE,
                    "invalid HashTableMemory option specified: "
                    "not within range 1-50%");
            }
            hashTableBytes = (masterBytes * pct) / 100;
        } else {
            hashTableBytes = strtoull(hashTableMemory.c_str(), NULL, 10);
            hashTableBytes *= (1024 * 1024);
        }

        if (hashTableBytes > masterBytes) {
            throw Exception(HERE,
                            "invalid `MasterTotalMemory' and/or "
                            "`HashTableMemory' options - HashTableMemory "
                            "cannot exceed MasterTotalMemory!");
        }

        uint64_t logBytes = masterBytes - hashTableBytes;
        uint64_t numSegments = logBytes / Segment::DEFAULT_SEGMENT_SIZE;
        if (numSegments < 1) {
            throw Exception(HERE,
                            "invalid `MasterTotalMemory' and/or "
                            "`HashTableMemory' options - insufficient memory "
                            "left for the log!");
        }

        uint64_t numHashTableLines =
            hashTableBytes / HashTable::bytesPerCacheLine();
        if (numHashTableLines < 1) {
            throw Exception(HERE,
                            "invalid `MasterTotalMemory' and/or "
                            "`HashTableMemory' options - insufficient memory "
                            "left for the hash table!");
        }

        RAMCLOUD_LOG(NOTICE,
                     "Master to allocate %lu bytes total, %lu of which for the "
                     "hash table", masterBytes, hashTableBytes);
        RAMCLOUD_LOG(NOTICE, "Master will have %lu segments and %lu lines in "
                     "the hash table", numSegments, numHashTableLines);

        master.logBytes = logBytes;
        master.hashTableBytes = hashTableBytes;
    }
};

} // namespace RAMCloud

#endif
