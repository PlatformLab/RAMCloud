/* Copyright (c) 2013 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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

#if ENABLE_LOGCABIN

#ifndef RAMCLOUD_LOGCABINSTORAGE_H
#define RAMCLOUD_LOGCABINSTORAGE_H

#include <LogCabin/Client.h>

#include "ExternalStorage.h"
#include "WorkerTimer.h"

namespace RAMCloud {

/**
 * This class provides a wrapper around the LogCabin storage system to
 * implement the ExternalStorage interface. See ExternalStorage for API
 * documentation. This class is thread-safe.
 *
 * Internally, this class uses two keys for the leader lease. The first is the
 * ownerKey, which is set to a random value along with the leader's locator.
 * The second is a keepAliveKey, which is touched every renewLeaseIntervalMs by
 * the leaseRenewer thread.
 *
 * If a server observes the keepAliveKey hasn't changed for a
 * checkLeaderIntervalMs period, all while the same value is in the ownerKey,
 * it may overwrite the ownerKey, gaining the lock. Once a server has the lock,
 * every subsequent operation uses the condition that ownerKey is still set to
 * its value (this is what motivates two keys: the value of the key used in the
 * condition can remain stable over time, while the owner can still show signs
 * of life on the keepAliveKey).
 *
 * The reason the ownerKey has a random value in addition to the leader's
 * locator is to distinguish a server's Nth leadership term from its N+1th
 * leadership term. Consider the following two scenarios:
 * - A died
 * - read(ownerKey) -> A
 * - read(keepAliveKey) -> X
 * - wait(checkLeaderIntervalMs)
 * - read(keepAliveKey) -> X
 * - set(ownerKey to me if A)
 * vs.
 * - A died and restarts
 * - read(ownerKey) -> A
 * - read(keepAliveKey) -> X
 * - B has been observing keepAliveKey for a while; overwrites ownerKey to B
 * - B dies
 * - wait(checkLeaderIntervalMs)
 * - read(keepAliveKey) -> X
 * - A also observed keepAliveKey hadn't changed; overwrites ownerKey to A
 * - set(ownerKey to me if A)
 * This may not be necessary in other systems such as ZooKeeper that expose a
 * version number along with every key. As LogCabin currently only exposes the
 * value (no version field), we need to alter the ownerKey value to be able to
 * distinguish A's first lease from A's second lease.
 */
class LogCabinStorage : public ExternalStorage {
  PUBLIC:
    explicit LogCabinStorage(const std::string& serverInfo);
    explicit LogCabinStorage(LogCabin::Client::Cluster cluster);
    virtual ~LogCabinStorage();
    virtual void becomeLeader(const char* name, const std::string& leaderInfo);
    virtual bool get(const char* name, Buffer* value);
    virtual void getChildren(const char* name,
                             std::vector<ExternalStorage::Object>* children);
    virtual bool getLeaderInfo(const char* name, Buffer* value);
    virtual const char* getWorkspace();
    virtual void remove(const char* name);
    virtual void set(Hint flavor, const char* name, const char* value,
                     int valueLength = -1);
    virtual void setWorkspace(const char* pathPrefix);

  PRIVATE:
    /// Clock used for lease renewal.
    typedef std::chrono::system_clock Clock;
    typedef Clock::time_point TimePoint;

    /**
     * becomeLeader() is written as a state machine; these are its possible
     * states.
     */
    enum class BecomeLeaderState {
        /**
         * The start state assumes nothing about the lease.
         * readExistingLease() is called from this state.
         */
        INITIAL,
        /**
         * At this point the current owner is known, but it could be dead or
         * alive. The owner key's value has been read into the 'tree'
         * condition, and the keepalive key's value has been read into
         * 'keepAliveValue'.
         * waitAndCheckLease() is called from this state.
         */
        OTHERS_LEASE_OBSERVED,
        /**
         * At this point, the current owner is known to be dead, and this
         * server should try to assert the lease.
         * takeOverLease() is called from this state.
         */
        OTHERS_LEASE_EXPIRED,
        /**
         * This server has gained the lease: becomeLeader() has completed.
         */
        LEASE_ACQUIRED,
    };

    // Helpers for becomeLeader. These are normally invoked in order, though
    // setbacks may occur and waitAndCheckLease() is skipped if no owner key
    // exists.
    BecomeLeaderState readExistingLease();
    BecomeLeaderState waitAndCheckLease();
    BecomeLeaderState takeOverLease();

    void leaseRenewerMain(TimePoint start);
    void makeParents(const char* name);
    void renewLease(TimePoint deadline);

    /**
     * Normally just an alias for usleep(), but overridden in some unit tests.
     */
    std::function<void(unsigned int)> mockableUsleep;

    /**
     * When a server is waiting to become a leader, it checks the leader
     * keepalive object this often (units: milliseconds); if the object hasn't
     * changed since the last check, the current server attempts to become
     * leader.
     *
     * const except for testing.
     */
    uint64_t checkLeaderIntervalMs;

    /**
     * When a server is leader, it renews its lease this often
     * (units: milliseconds). This value must be quite a bit less than
     * checkLeaderIntervalMs. A value of zero is used during testing;
     * it means "don't renew the lease or even start the timer thread".
     *
     * const except for testing.
     */
    uint64_t renewLeaseIntervalMs;

    /**
     * When a server is leader, it considers its lease to be lost after this
     * long since it started its last successful keep-alive (units:
     * milliseconds). This value must be between renewLeaseIntervalMs and
     * checkLeaderIntervalMs.
     *
     * const except for testing.
     */
    uint64_t expireLeaseIntervalMs;

    /**
     * Protects #isExiting and #exiting.
     */
    std::mutex exitingMutex;

    /**
     * Set to true when #leaseRenewer thread should exit.
     */
    bool isExiting;

    /**
     * Notified by the destructor when #leaseRenewer thread should exit.
     */
    std::condition_variable exiting;

    /**
     * Handle to invoke LogCabin cluster-level operations.
     */
    LogCabin::Client::Cluster cluster;

    /**
     * Handle to LogCabin data structure. If a workspace has been set through
     * #setWorkspace, this tree will have its working directory set there.
     */
    LogCabin::Client::Tree tree;

    /**
     * Name of the key that is used to synchronize leader election,
     * as passed to becomeLeader(). Only set once becomeLeader() is called.
     */
    std::string ownerKey;

    /**
     * Value to be stored in ownerKey along with a random number,
     * as passed to becomeLeader().  Only set once becomeLeader() is called.
     */
    std::string leaderInfo;

    /**
     * The name of the key that #leaseRenewer periodically writes to.
     */
    std::string keepAliveKey;

    /**
     * In BecomeLeaderState::OTHERS_LEASE_OBSERVED, this is the value of
     * keepAliveKey that was previously read. Undefined in other states.
     */
    std::string keepAliveValue;

    /**
     * Used in testing renewLease only.
     */
    uint64_t lastTimeoutNs;

    /// Used to update leader keep-alive key.
    std::thread leaseRenewer;

    DISALLOW_COPY_AND_ASSIGN(LogCabinStorage);
};

} // namespace RAMCloud

#endif // RAMCLOUD_LOGCABINSTORAGE_H

#endif // ENABLE_LOGCABIN
