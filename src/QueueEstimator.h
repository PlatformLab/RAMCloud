/* Copyright (c) 2016-2017 Stanford University
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

#ifndef RAMCLOUD_QUEUEESTIMATOR_H
#define RAMCLOUD_QUEUEESTIMATOR_H

#include "Cycles.h"

namespace RAMCloud {

/**
 * This class is used to estimate the current number of bytes still
 * awaiting transmission in a NIC's transmit queue. It does this by
 * tracking when output packets are passed to the NIC, and then using
 * the network bandwidth to estimate how many bytes have actually been
 * transmitted. It assumes that this class has complete knowledge of all
 * packets given to the NIC, so it may underestimate queue length in
 * situations where packets can be queued (e.g. by other processes)
 * without the knowledge of this class.
 */
class QueueEstimator {
  PUBLIC:

    /**
     * Describes the state of the NIC's transmit queue; used to export this
     * information from drivers up to transports. Mainly used for performance
     * debugging.
     */
    struct TransmitQueueState {
        /// # rdtsc ticks for which the transmit queue has been empty.
        /// Must be 0 if #outstandingBytes > 0.
        uint64_t idleTime;

        /// Estimate of # bytes waiting to be sent at the transmit queue.
        /// Must be 0 if #idleTime > 0.
        uint32_t outstandingBytes;
    };

    /**
     * Construct a QueueEstimator; the NIC is assumed to be idle when
     * this method is invoked.
     * \param mBitsPerSecond
     *      Bandwidth of the network, in Mbits per second.
     */
    explicit QueueEstimator(uint32_t mBitsPerSecond = 10000)
        : bandwidth()
        , currentTime(0)
        , idleSince(0)
        , queueSize(0)
    {
        bandwidth = (static_cast<double>(mBitsPerSecond)*1e06/8.0)
                / Cycles::perSecond();
    }

    /**
     * This method must be invoked whenever a packet is added to the queue for
     * the NIC.
     * \param length
     *      Total number of bytes in packet(s) that were just added to the
     *      NIC's queue.
     * \param transmitTime
     *      Time when the packet was queued in the NIC, in Cycles::rdtsc ticks.
     * \param[out] txQueueState
     *      Used to retrieve state of the NIC's transmit queue just before
     *      this packet was added to the NIC. NULL means the caller doesn't
     *      care about this value.
     */
    void
    packetQueued(uint32_t length, uint64_t transmitTime,
            TransmitQueueState* txQueueState = NULL)
    {
        getQueueSize(transmitTime);
        if (txQueueState != NULL) {
            txQueueState->idleTime =
                    (queueSize > 0) ? 0 : transmitTime - idleSince;
            txQueueState->outstandingBytes = queueSize;
        }
        queueSize += length;
    }

    /**
     * Returns an estimate of the number of untransmitted bytes still
     * present in the NIC's queue.
     * \param time
     *      Current time, in Cycles::rdtsc ticks.
     */
    uint32_t
    getQueueSize(uint64_t time)
    {
        // If the caller passes in a stale timestamp, just return the latest
        // queue size.
        if (time > currentTime) {
            double newSize = queueSize
                    - static_cast<double>(time - currentTime) * bandwidth;
            uint32_t oldQueueSize = queueSize;
            queueSize = (newSize < 0) ? 0 : static_cast<uint32_t>(newSize);
            currentTime = time;
            if ((oldQueueSize > 0) && (queueSize == 0)) {
                // The transmit queue became empty at some point between
                // `currentTime` and `time`.
                idleSince = time - (uint64_t)(-newSize / bandwidth);
            }
        }
        return queueSize;
    }

    /**
     * Called to provide information about the network bandwidth, which is
     * used to estimate how quickly packets are being transmitted.
     * \param mBitsPerSecond
     *      Bandwidth of the network, in Mbits per second.
     */
    void
    setBandwidth(uint32_t mBitsPerSecond)
    {
        bandwidth = (static_cast<double>(mBitsPerSecond)*1e06/8.0)
                / Cycles::perSecond();
    }

    /**
     * This method may be invoked to indicate that the NIC queue is known
     * to be a particular length at a particular time.
     * \param numBytes
     *      Number of bytes known to be in the NIC queue now.
     * \param time
     *      The current time, in Cycles::rdtsc ticks.
     */
    void
    setQueueSize(uint32_t numBytes, uint64_t time)
    {
        currentTime = time;
        queueSize = numBytes;
    }

    // PROTECTED:
    /// Network bandwidth, measured in bytes per Cycles::rdtsc tick.
    double bandwidth;

    /// A Cycles::rdtsc ticks value indicating the latest time when queueSize
    /// was calculated.
    uint64_t currentTime;

    /// A Cycles::rdtsc ticks value indicating the time since when the
    /// transmit queue has been empty, only defined when the transmit queue
    /// is empty at currentTime (i.e. queueSize == 0).
    uint64_t idleSince;

    /// The number of bytes in the transmit queue at currentTime.
    uint32_t queueSize;

    DISALLOW_COPY_AND_ASSIGN(QueueEstimator);
};

} // namespace RAMCloud

#endif // RAMCLOUD_QUEUEESTIMATOR_H

