/* Copyright (c) 2012-2016 Stanford University
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

#ifndef RAMCLOUD_TABLETMANAGER_H
#define RAMCLOUD_TABLETMANAGER_H

#include <unordered_map>

#include "Common.h"
#include "Object.h"
#include "HashTable.h"
#include "ServerStatistics.pb.h"
#include "SpinLock.h"
#include "Tablets.pb.h"

namespace RAMCloud {

/**
 * This class is used by master servers to keep track of the tablets assigned
 * to them. For example, on every object operation, the TabletManager must be
 * checked to see if the master owns a tablet corresponding to that object. If
 * not, the operation is rejected. These checks are necessary because objects
 * may exist in the hash table temporarily for tablets that are not yet owned.
 * This happens, for instance, during crash recovery and tablet migration.
 *
 * This class is thread-safe (a monitor lock is used to support concurrent
 * access). When looking up tablets (see the getTablet() methods) a snapshot of
 * the current tablet's data is returned to the caller. This copying avoids the
 * need for atomic operations or other synchronization each time a field is
 * read. The downside, of course, is that the caller needs to be aware that the
 * actual state may be permuted at any time and will not be reflected in the
 * cached copy obtained during the lookup.
 */
class TabletManager {
  PUBLIC:
    /**
     * Each tablet is in one particular state at any point in time. This state
     * is used only within each master's TabletManager. We never send or receive
     * the value of this enum directly to/from another master or coordinator.
     * This is different from Tablet.status which is used within coordinator and
     * attached external storage.
     */
    enum TabletState {
        /// The tablet is available.
        NORMAL = 0,
        /// The tablet is being re-constructed yet. (eg. migration and recovery)
        NOT_READY = 1,
        /// Migration of tablet is requested. Cannot take new writes.
        LOCKED_FOR_MIGRATION = 2,
    };

    /**
     * Each tablet owned by a master is described by the fields in this class.
     * Tablets describe contiguous ranges of key hash space within a particular
     * table. A table may consist of one or many tablets, and tablets may be
     * assigned to multiple different master servers.
     *
     * TabletManagers maintain the canonical copies of these classes and they
     * are copied when callers access them (via getTablet() methods).
     */
    class Tablet {
      PUBLIC:
        Tablet()
            : tableId(-1)
            , startKeyHash(-1)
            , endKeyHash(-1)
            , state(NOT_READY)
            , readCount(-1)
            , writeCount(-1)
        {
        }

        Tablet(uint64_t tableId,
               uint64_t startKeyHash,
               uint64_t endKeyHash,
               TabletState state)
            : tableId(tableId)
            , startKeyHash(startKeyHash)
            , endKeyHash(endKeyHash)
            , state(state)
            , readCount(0)
            , writeCount(0)
        {
        }

        /// The identifier of the table that this tablet describes a portion of.
        uint64_t tableId;

        /// The first key hash value in the range owned by this tablet.
        uint64_t startKeyHash;

        /// The last key hash value in the range owned by this tablet.
        uint64_t endKeyHash;

        /// The current state of the tablet. See TabletState.
        TabletState state;

        /// The number of read operations performed on objects in this tablet.
        uint64_t readCount;

        /// The number of write operations performed on objects in this tablet.
        uint64_t writeCount;
    };

    /**
     * Prevents the state of a tabletManager from being modified, and provides
     * accessors to some state of TabletManager.
     *
     * Any external module outside of TabletManager should grab this Protector
     * to access state of TabletManager. The checked condition will remain valid
     * until this Protector is destructed.
     *
     * Since this Protector holds the monitor lock of TabletManager, user should
     * only keep Protector short period, and never call TabletManager's methods
     * directly.
     */
    class Protector {
      PUBLIC:
        explicit Protector(TabletManager* tabletManager);

        bool notReadyTabletExists();
        bool getTablet(uint64_t tableId,
                       uint64_t keyHash,
                       Tablet* outTablet = NULL);

      PRIVATE:
        // Keep reference to TabletManager so that its SpinLock can be locked
        // and its state can be accessed through accessors.
        TabletManager* tabletManager;

        // Guard for locking the monitor lock of tabletManager.
        SpinLock::Guard lockGuardForTabletManager;

        DISALLOW_COPY_AND_ASSIGN(Protector);
    };

    TabletManager();
    bool addTablet(uint64_t tableId,
                   uint64_t startKeyHash,
                   uint64_t endKeyHash,
                   TabletState state);
    bool checkAndIncrementReadCount(Key& key);
    bool getTablet(Key& key,
                   Tablet* outTablet = NULL);
    bool getTablet(uint64_t tableId,
                   uint64_t keyHash,
                   Tablet* outTablet = NULL);
    bool getTablet(uint64_t tableId,
                   uint64_t startKeyHash,
                   uint64_t endKeyHash,
                   Tablet* outTablet = NULL);
    void getTablets(vector<Tablet>* outTablets);
    bool deleteTablet(uint64_t tableId,
                      uint64_t startKeyHash,
                      uint64_t endKeyHash);
    bool splitTablet(uint64_t tableId,
                     uint64_t splitKeyHash);
    bool changeState(uint64_t tableId,
                     uint64_t startKeyHash,
                     uint64_t endKeyHash,
                     TabletState oldState,
                     TabletState newState);
    void incrementReadCount(Key& key);
    void incrementReadCount(uint64_t tableId,
                            KeyHash keyHash);
    void incrementWriteCount(Key& key);
    void incrementWriteCount(uint64_t tableId,
                             KeyHash keyHash);
    void getStatistics(ProtoBuf::ServerStatistics* serverStatistics);
    size_t getNumTablets();
    string toString();

  PRIVATE:
    /// Tablets are stored in a multimap that is indexed by table identifier.
    /// The assumption is that we are likely to have many tablets, but
    /// relatively few for the same table.
    typedef std::unordered_multimap<uint64_t, Tablet> TabletMap;

    TabletMap::iterator lookup(uint64_t tableId, uint64_t keyHash,
                               const SpinLock::Guard& lock);

    /// This unordered_multimap is used to store and access all tablet data.
    TabletMap tabletMap;

    /// Monitor spinlock used to protect the tabletMap from concurrent access.
    SpinLock lock;

    /// Count of tablets whose status is NOT_READY. Used to determine if there
    /// is any ongoing recovery.
    /// Main use case is to prevent UnackedRpcResult::cleanByTimeout() from
    /// accidentally garbage collect the RpcResults of an expired client
    /// before corresponding transaction to complete.
    int numLoadingTablets;

    DISALLOW_COPY_AND_ASSIGN(TabletManager);
};

} // namespace RAMCloud

#endif // RAMCLOUD_TABLETMANAGER_H
