/* Copyright (c) 2011 Stanford University
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

/**
 * \file
 * This file defines the CoordinatorServerList class.
 */

#ifndef RAMCLOUD_COORDINATORSERVERLIST_H
#define RAMCLOUD_COORDINATORSERVERLIST_H

#include "ServerList.pb.h"
#include "Tablets.pb.h"

#include "Rpc.h"
#include "ServiceMask.h"
#include "ServerId.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * A CoordinatorServerList allocates ServerIds and holds Coordinator
 * state associated with live servers. It is closely related to the
 * ServerList and ServerTracker classes in that it essentially consists
 * of a map of ServerIds to some data. However, unlike ServerLists it
 * does not propagate events to trackers, and unlike ServerTrackers it
 * is not an asynchronous model of queued updates.
 *
 * There's probably some clever class hierarchy that could be used for
 * all of these related bits, but I'm not sure that it would offer much
 * in terms of code reuse or simplifications.
 */
class CoordinatorServerList {
  PUBLIC:
    /**
     * This class represents one entry in the CoordinatorServerList. Each
     * entry describes a specific server in the system and contains the
     * state that the Coordinator is maintain on its behalf.
     *
     * Note that pointer members are not allocated or freed by this class.
     * It's up to the user to ensure proper memory management, and they're
     * free to copy entries all they want.
     */
    class Entry {
      public:
        Entry(ServerId serverId,
              string serviceLocatorString,
              ServiceMask serviceMask);
        Entry(const Entry& other) = default;
        Entry& operator=(const Entry& other) = default;
        void serialise(ProtoBuf::ServerList_Entry& dest,
                       bool isInCluster) const;

        bool isMaster() const { return serviceMask.has(MASTER_SERVICE); }
        bool isBackup() const { return serviceMask.has(BACKUP_SERVICE); }

        /// ServerId of the server (uniquely identifies it, never reused).
        ServerId serverId;

        /// The ServiceLocator of the server (used to address it).
        string serviceLocator;

        /// Which services this particular server is running.
        ServiceMask serviceMask;

        /// The master's will (only to be set if the serviceMask includes
        /// MASTER_SERVICE).
        ProtoBuf::Tablets* will;

        /// The backup's read speed in megabytes per second (only to set
        /// set if serviceMask includes BACKUP_SERVICE).
        uint32_t backupReadMBytesPerSec;

        // Fields below this point are maintained on the coordinator only
        // and are not transmitted to members' ServerLists.

        /**
         * Any open replicas found during recovery are considered invalid
         * if they have a segmentId less than this.  This is used by masters
         * to invalidate replicas they have lost contact with while actively
         * writing to them.
         */
        uint64_t minOpenSegmentId;
    };

    CoordinatorServerList();
    ~CoordinatorServerList();
    ServerId add(string serviceLocator,
                 ServiceMask serviceMask,
                 uint32_t readSpeed,
                 ProtoBuf::ServerList& update);
    void remove(ServerId serverId,
                ProtoBuf::ServerList& update);
    void incrementVersion(ProtoBuf::ServerList& update);
    const Entry& operator[](const ServerId& serverId) const;
    Entry& operator[](const ServerId& serverId);
    const Entry* operator[](size_t index) const;
    Entry* operator[](size_t index);
    bool contains(ServerId serverId) const;
    size_t size() const;
    uint32_t masterCount() const;
    uint32_t backupCount() const;
    uint32_t nextMasterIndex(uint32_t startIndex) const;
    uint32_t nextBackupIndex(uint32_t startIndex) const;
    void serialise(ProtoBuf::ServerList& protoBuf) const;
    void serialise(ProtoBuf::ServerList& protobuf,
                   ServiceMask services) const;

  PRIVATE:
    /**
     * The list of servers is just a vector of the following structure,
     * containing a permanent generation number that increments each
     * time an index is reused, and a Tubbed Entry, which describes the
     * server currently allocated to that slot, if there is one.
     */
    class GenerationNumberEntryPair {
      public:
        GenerationNumberEntryPair()
            : nextGenerationNumber(0),
              entry()
        {
        }

        /// The next generation number to be assigned in this slot.
        uint32_t nextGenerationNumber;

        /// If allocated, the entry associated with the ServerId in this slot.
        Tub<Entry> entry;
    };

    uint32_t firstFreeIndex();
    const Entry& getReferenceFromServerId(const ServerId& serverId) const;
    const Entry* getPointerFromIndex(size_t index) const;

    /// Slots in the server list.
    std::vector<GenerationNumberEntryPair> serverList;

    // TODO(Rumble): This is only a temporary hack until we clean up enlistment.
  PUBLIC:
    /// Number of masters in the server list.
    uint32_t numberOfMasters;

    /// Number of backups in the server list.
    uint32_t numberOfBackups;

  PRIVATE:
    /// Incremented each time the server list is modified (i.e. when add or
    /// remove is called). Since we usually send delta updates to clients,
    /// they can use this to determine if any previous RPC was missed and
    /// then re-fetch the latest list in its entirety to get back on track.
    uint64_t versionNumber;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerList);
};

} // namespace RAMCloud

#endif // !RAMCLOUD_COORDINATORSERVERLIST_H
