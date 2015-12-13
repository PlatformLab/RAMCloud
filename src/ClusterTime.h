/* Copyright (c) 2015 Stanford University
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

#ifndef RAMCLOUD_CLUSTERTIME_H
#define RAMCLOUD_CLUSTERTIME_H

#include "Minimal.h"
#include "Atomic.h"

namespace RAMCloud {

// Forward declaration of ClusterTime
class ClusterTime;

/**
 * ClusterTimeDuration represents the elapsed logical time between two points in
 * cluster-time (the cluster-wide logical notion of time).  In conjuction with
 * ClusterTime, this class allows some basic "arithmetic" to be performed on the
 * logical notion of cluster-time and also provides approximate conversions
 * between elapsed cluster-time and elapsed of wall-clock-time (real time).
 */
class ClusterTimeDuration {
    friend class ClusterTime;
  PUBLIC:
    /**
     * Converts from a real wall-clock-time duration (nanoseconds) to an
     * approximately equivalent cluster-clock duration (ClusterTimeDuration).
     *
     * \param nanoseconds
     *      Length of the real wall-clock-time duration in nanoseconds that
     *      the returned ClusterTimeDelta will approximate.
     * \return
     *      ClusterTimeDuration object with a cluster-time duration
     *      approximately as long as the provided nanosecond duration.
     */
    static CONSTEXPR_FUNC ClusterTimeDuration fromNanoseconds(
            int64_t nanoseconds) {
        // Do conversion from nanoseconds to internal length representation
        // (currently not necessary as nanoseconds to length is 1-to-1).
        return ClusterTimeDuration(nanoseconds);
    }

    /**
     * Return the approximate length of this ClusterTimeDuration in nanoseconds.
     */
    int64_t toNanoseconds() const {
        return length;
    }

    /**
     * Return the difference between two cluster-time durations as a duration in
     * cluster-time.  This method does not check for underflow.
     */
    ClusterTimeDuration operator-(const ClusterTimeDuration &other) const {
        int64_t duration = length - other.length;
        return ClusterTimeDuration(duration);
    }

    /**
     * Relational operators to allow comparisons between cluster-time durations
     * For example, if lhs is "less than" rhs then lhs represents a shorter
     * duration than rhs.
     */
    friend bool operator< (const ClusterTimeDuration& lhs,
                           const ClusterTimeDuration& rhs) {
        return lhs.length < rhs.length;
    }
    friend bool operator> (const ClusterTimeDuration& lhs,
                           const ClusterTimeDuration& rhs) {
        return rhs < lhs;
    }
    friend bool operator<=(const ClusterTimeDuration& lhs,
                           const ClusterTimeDuration& rhs) {
        return !(lhs > rhs);
    }
    friend bool operator>=(const ClusterTimeDuration& lhs,
                           const ClusterTimeDuration& rhs) {
        return !(lhs < rhs);
    }
    friend bool operator==(const ClusterTimeDuration& lhs,
                           const ClusterTimeDuration& rhs) {
        return lhs.length == rhs.length;
    }
    friend bool operator!=(const ClusterTimeDuration& lhs,
                           const ClusterTimeDuration& rhs) {
        return !(lhs == rhs);
    }

  PRIVATE:
    /**
     * Private constructor used to construct a ClusterTimeDuration given a
     * duration in cluster-time units.
     */
    CONSTEXPR_FUNC explicit ClusterTimeDuration(int64_t duration)
        : length(duration)
    {}

    /// Internal representation of cluster-time duration in cluster-time units.
    int64_t length;
};


/**
 * ClusterTime represents a specific point in cluster-time (the cluster-wide
 * logical notion of time) and provides operations on cluster-time.  For
 * instance, given two points in cluster-time timestamps we can ask if one time
 * occurred logically before the other.  ClusterTime is used/managed by the
 * CoordinatorClusterClock and ClusterClock modules and is primarily used for
 * defining ClientLease expiration times.
 *
 * This class provides certain atomic operations to facilitate its use in
 * thread-safe modules like CoordinatorClusterClock and ClusterClock.
 */
class ClusterTime {
  PUBLIC:
    /**
     * Constructs a ClusterTime object which represents the initial cluster-time
     * point (cluster-time zero).
     */
    ClusterTime()
        : timestamp(0)
    {}

    /**
     * Constructs a ClusterTime object from an encoded ClusterTime object;
     * normally used to reconstruct a ClusterTime object that comes "off the
     * wire" from an RPC or ExternalStorage.
     *
     * \param encodedClusterTime
     *      The encoding of a ClusterTime object the is to be reconstructed.
     */
    explicit ClusterTime(uint64_t encodedClusterTime)
        : timestamp(static_cast<int64_t>(encodedClusterTime))
    {}

    /**
     * Return an encoded representation of this ClusterTime value.  Used to
     * serialize a ClusterTime object into an RPC or onto ExternalStorage.
     */
    uint64_t getEncoded() const {
        return static_cast<uint64_t>(timestamp.load());
    }

    /**
     * Add a duration to a cluster-time to get a new cluster-time in the future.
     *
     * \param duration
     *      The duration to be added.
     * \return
     *      ClusterTime that is a given duration after the given ClusterTime.
     */
    ClusterTime operator+(const ClusterTimeDuration &duration) const {
        ClusterTime newClusterTime;
        newClusterTime.timestamp = this->timestamp.load() + duration.length;
        return newClusterTime;
    }

    /**
     * Return the difference between two cluster-time points as a duration in
     * cluster-time.  This method does not check for underflow.
     */
    ClusterTimeDuration operator-(const ClusterTime &other) const {
        int64_t duration = timestamp.load() - other.timestamp.load();
        return ClusterTimeDuration(duration);
    }

    /**
     * Atomically compare the current cluster-time with a test cluster-time and,
     * if they match, replace the current cluster-time with a new cluster-time.
     *
     * \param test
     *      Replace the cluster-time only if its current value equals this.
     * \param newTime
     *      This cluster-time will replace the current value.
     * \result
     *      The previous cluster-time.
     */
    ClusterTime compareExchange(ClusterTime test, ClusterTime newTime)
    {
        return ClusterTime(timestamp.compareExchange(test.timestamp,
                                                     newTime.timestamp));
    }

    /**
     * Relational operators to allow comparisons between points in cluster-time.
     * For example, if lhs is "less than" rhs then lhs represents an earlier
     * cluster-time than rhs.
     */
    friend bool operator< (const ClusterTime& lhs, const ClusterTime& rhs) {
        return lhs.timestamp.load() < rhs.timestamp.load();
    }
    friend bool operator> (const ClusterTime& lhs, const ClusterTime& rhs) {
        return rhs < lhs;
    }
    friend bool operator<=(const ClusterTime& lhs, const ClusterTime& rhs) {
        return !(lhs > rhs);
    }
    friend bool operator>=(const ClusterTime& lhs, const ClusterTime& rhs) {
        return !(lhs < rhs);
    }
    friend bool operator==(const ClusterTime& lhs, const ClusterTime& rhs) {
        return lhs.timestamp.load() == rhs.timestamp.load();
    }
    friend bool operator!=(const ClusterTime& lhs, const ClusterTime& rhs) {
        return !(lhs == rhs);
    }

  PRIVATE:

    /// Internal representation of cluster-time in cluster-time units.
    Atomic<int64_t> timestamp;
};

} // namespace RAMCloud

#endif // RAMCLOUD_CLUSTERTIME_H
