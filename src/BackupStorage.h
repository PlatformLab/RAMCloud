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

#ifndef RAMCLOUD_BACKUPSTORAGE_H
#define RAMCLOUD_BACKUPSTORAGE_H

#include <boost/dynamic_bitset.hpp>

#include "Buffer.h"
#include "ServerId.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * Used for selecting various backup placement strategies at runtime.
 * These are primarily used for testing weird segment placement
 * and balancing ideas.
 */
enum BackupStrategy {
    RANDOM_REFINE_MIN,
    RANDOM_REFINE_AVG,
    EVEN_DISTRIBUTION,
};

struct BackupStorageException : public Exception {
    explicit BackupStorageException(const CodeLocation& where)
        : Exception(where) {}
    BackupStorageException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    BackupStorageException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    BackupStorageException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};

/**
 * The base class for all storage backends for backup.  This includes
 * SingleFileStorage for storing and recovering from disk and
 * InMemoryStorage for storing and recovering from RAM.
 */
class BackupStorage {
  PUBLIC:
    /**
     * Format for persisent metadata.  Includes the serverId of the backup that
     * uses this storage along with the name of the cluster to which this
     * backup belongs.
     */
    struct Superblock {
        /**
         * Distinguishes redundant copies of the superblock from one another
         * on storage so that only the most recent is used.
         */
        uint64_t version;

        /**
         * Server id of the backup which owns the replicas in the storage.
         * Before enlistment,  this contains the former server id that operated
         * using this storage.  After enlistment, this contains the server id
         * which is currently storing data in this storage.
         */
        uint64_t serverId;

        /**
         * Controls the reuse of replicas stored on this backup.  'Tags'
         * replicas created on this backup with this cluster name.  This has
         * two effects.  First, any replicas found in storage are discarded
         * unless they are tagged with an identical cluster name. Second, any
         * replicas created by the backup process will only be reused by future
         * backup processes if the cluster name on the stored replica matches
         * the cluster name of future process. The name '__unnamed__' is
         * special and never matches any cluster name (even itself), so it
         * guarantees all stored replicas are discarded on start and that all
         * replicas created by this process are discarded by future backups.
         * This is convenient for testing.
         */
        char clusterName[256];

        /**
         * Create a default superblock image; used in the case none is found
         * on storage.
         */
        Superblock()
            : version(0)
            , serverId(ServerId().getId())
            , clusterName()
        {
            const char* unnamed = "__unnamed__";
            assert(strlen(unnamed) < sizeof(clusterName) - 1);
            strncpy(clusterName, unnamed, sizeof(clusterName) - 1);
        }

        /**
         * Create a superblock image.
         */
        Superblock(uint64_t version, ServerId serverId, const char* name)
            : version(version)
            , serverId(serverId.getId())
            , clusterName()
        {
            assert(strlen(clusterName) < sizeof(clusterName) - 1);
            strncpy(clusterName, name, sizeof(clusterName) - 1);
        }

        /// Return '\0' terminated cluster name in this superblock.
        const char* getClusterName() { return clusterName; }

        /// Return the server id in this superblock.
        ServerId getServerId() { return ServerId(serverId); }
    } __attribute__((packed));

    /**
     * An opaque frame used to access a stored segment. All concrete
     * implementations of BackupStorage will subclass this to contain the
     * state needed to access storage for replicas.
     */
    class Frame {
      PUBLIC:
        // XXX: All these need docs.
        virtual void loadMetadata() = 0;
        virtual const void* getMetadata() = 0;

        virtual void startLoading() = 0;
        virtual bool isLoaded() = 0;
        virtual void* load() = 0;

        virtual void append(Buffer& source,
                            size_t sourceOffset,
                            size_t length,
                            size_t destinationOffset,
                            const void* metadata,
                            size_t metadataLength) = 0;
        virtual void close() = 0;
        /**
         * Free the storage for a replica for reuse. Freed data may still
         * appear during future recoveries if this backup crashes and another
         * restarts using the same storage.
         *
         * IMPORTANT: frame is no longer valid after this call. The caller
         * must take care not to use it.
         */
        virtual void free() = 0;
        virtual ~Frame() {}
        Frame() {}
      DISALLOW_COPY_AND_ASSIGN(Frame);
    };

    virtual ~BackupStorage() {}

    // XXX: All these need docs.
    /**
     * Set aside storage for a specific segment and give a frame back
     * for working with that storage.
     */
    virtual Frame* open(bool sync) = 0;
    virtual uint32_t benchmark(BackupStrategy backupStrategy);
    virtual size_t getMetadataSize() = 0;
    virtual std::vector<Frame*> loadAllMetadata() = 0;
    virtual void resetSuperblock(ServerId serverId,
                                 const string& clusterName,
                                 uint32_t frameSkipMask = 0) = 0;
    virtual Superblock loadSuperblock() = 0;
    virtual void quiesce() = 0;
    virtual void fry() = 0;

    /// See #storageType.
    enum class Type { UNKNOWN = 0, MEMORY = 1, DISK = 2 };

  PROTECTED:
    /**
     * Specify the segment size this BackupStorage will operate on.  Used
     * only by the implementers of the BackupStorage interface.
     *
     * \param segmentSize
     *      The segment size this BackupStorage operates on.
     * \param storageType
     *      The storage type corresponding with the concrete implementation of
     *      this class.
     */
    explicit BackupStorage(size_t segmentSize, Type storageType)
        : segmentSize(segmentSize)
        , storageType(storageType)
    {
    }

    /// Maximum length in bytes of a replica.
    size_t segmentSize;

  PUBLIC:
    /// Used in RawMetrics to print out the backup storage type.
    const Type storageType;

    DISALLOW_COPY_AND_ASSIGN(BackupStorage);
};

} // namespace RAMCloud

#endif
