/* Copyright (c) 2010-2017 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_DRIVER_H
#define RAMCLOUD_DRIVER_H

#include <memory>
#include <vector>

#include "Common.h"
#include "Buffer.h"
#include "Dispatch.h"
#include "QueueEstimator.h"

#undef CURRENT_LOG_MODULE
#define CURRENT_LOG_MODULE RAMCloud::TRANSPORT_MODULE

namespace RAMCloud {

class ServiceLocator;

/**
 * Used by transports to send and receive unreliable datagrams. This is an
 * abstract class; see UdpDriver for an example of a concrete implementation.
 */
class Driver {
  public:

    /**
     * A base class for Driver-specific network addresses.
     */
    class Address {
      protected:
        Address() {}
        Address(const Address& other) {}

      public:
        virtual ~Address() {}

        /**
         * Return a 64-bit unsigned integer hash of this Address (for debugging,
         * logging, and fast equality test).
         */
        virtual uint64_t getHash() const = 0;

        /**
         * Return a string describing the contents of this Address (for
         * debugging, logging, etc).
         */
        virtual string toString() const = 0;
    };

  PROTECTED:
    /**
     * Holds an incoming packet plus the address from which it came.
     *
     * This struct template is only visible to the Driver sub-classes;
     * Transport module interfaces with Received instead.
     *
     * PacketBuf and Received are separated because we would like to have
     * fixed sized PacketBuf objects allocated from an object pool in a
     * specific driver implementation, while Transport has to work with any
     * driver whose packet size is not known statically.
     *
     * \tparam T
     *      type of the sender's address
     * \tparam N
     *      the maximum size of the payload
     * \tparam M
     *      the maximum size of the headroom space
     */
    template<typename T, uint32_t N, uint32_t M = 0>
    struct PacketBuf {
        PacketBuf()
            : sender()
            // No need to initialize payload
        {}

        virtual ~PacketBuf() {}

        /// Address of sender (used to send reply).
        Tub<T> sender;

        /// Optional small headroom space (used to embed extra metadata).
        char headroom[M];

        /// Packet data (may not fill all of the allocated space).
        char payload[N];
    };

  public:
    /**
     * Represents an incoming packet.
     *
     * A Received typically refers to resources owned by the driver, such
     * as a packet buffer. These resources will be returned to the driver
     * when the Received is destroyed. However, if the transport wishes to
     * retain ownership of the packet buffer after the Received is destroyed
     * (e.g. while the RPC is being processed), then it may call the
     * steal method to take over responsibility for the packet buffer.
     * If it does this, it must eventually call the Driver's release
     * method to return the packet buffer.
     */
    class Received {
      public:
        Received(const Address* sender, Driver *driver, uint32_t len,
                char *payload)
            : sender(sender)
            , driver(driver)
            , len(len)
            , payload(payload)
        {}

        // Move constructor (needed for using in std::vectors)
        Received(Received&& other)
            : sender(other.sender)
            , driver(other.driver)
            , len(other.len)
            , payload(other.payload)
        {
            other.len = 0;
            other.payload = NULL;
        }

        ~Received();

        void* getRange(uint32_t offset, uint32_t length);

        /**
         * Allows data at a given offset into the Received to be treated as a
         * specific type.
         *
         * \tparam T
         *      The type to treat the data at payload + offset as.
         * \param offset
         *      Offset in bytes of the desired object within the payload.
         * \return
         *      Pointer to a T object at the desired offset, or NULL if
         *      the requested object doesn't fit entirely within the payload.
         */
        template<typename T>
        T*
        getOffset(uint32_t offset)
        {
            return static_cast<T*>(getRange(offset, sizeof(T)));
        }

        char *steal(uint32_t *len);

        /// Address from which this data was received. The object referred
        /// to by this pointer will be stable as long as the packet data
        /// is stable (i.e., if steal() is invoked, then the Address will
        /// live until release is invoked).
        const Address* sender;

        /// Driver the packet came from, where resources should be returned.
        Driver* const driver;

        /// Length in bytes of received data.
        uint32_t len;

        /// The start of the received data.  If non-NULL then we must return
        /// this storage to the driver when this object is deleted.  NULL means
        /// someone else (e.g. the Transport module) has taken responsibility
        /// for it.
        char *payload;

        /// Counts the total number of times that steal has been invoked
        /// across all Received objects. Intended for unit testing; only
        /// updated if compiled for TESTING.
        static uint32_t stealCount;

      private:
        DISALLOW_COPY_AND_ASSIGN(Received);
    };

    /**
     * A Buffer::Chunk that is comprised of memory for incoming packets,
     * owned by the Driver but loaned to a Buffer during the processing of an
     * incoming RPC so the message doesn't have to be copied.
     *
     * PayloadChunk behaves like any other Buffer::Chunk except it returns
     * its memory to the Driver when the Buffer is deleted.
     */
    class PayloadChunk : public Buffer::Chunk {
      public:
        static PayloadChunk* appendToBuffer(Buffer* buffer,
                                            char* data,
                                            uint32_t dataLength,
                                            Driver* driver,
                                            char* payload);
        ~PayloadChunk();
      private:
        PayloadChunk(void* data,
                     uint32_t dataLength,
                     Driver* driver,
                     char* const payload);

        /// Return the PayloadChunk memory here.
        Driver* const driver;

        /// The memory backing the chunk and which is to be returned.
        char* const payload;

        friend class Buffer;      // allocAux must call private constructor.
        DISALLOW_COPY_AND_ASSIGN(PayloadChunk);
    };

    /// Create a protected short alias to be used in Driver subclasses.
    using TransmitQueueState = QueueEstimator::TransmitQueueState;

    /**
     * Driver constructor.
     *
     * \param context
     *      Overall information about the RAMCloud server or client.
     */
    explicit Driver(Context* context)
        : context(context)
        , lastTransmitTime(0)
        , maxTransmitQueueSize(0)
        , packetsToRelease()
        , queueEstimator(0)
    {
        // Default: no throttling of transmissions (probably not a good idea).
        maxTransmitQueueSize = 10000000;
    }

    virtual ~Driver() {};

    /// \copydoc Transport::dumpStats
    virtual void dumpStats() {}

    /**
     * Returns the highest packet priority level this Driver supports (0 is
     * the lowest priority level). The larger the number, the more priority
     * levels are available. For example, if the highest priority level is 7
     * then the Driver has 8 priority levels, ranging from 0 (lowest priority)
     * to 7 (highest priority).
     */
    virtual int getHighestPacketPriority()
    {
        // Default: support only one priority level.
        return 0;
    }

    /**
     * The maximum number of bytes this Driver can transmit in a single
     * packet, including both header and payload.
     */
    virtual uint32_t getMaxPacketSize() = 0;

    /**
     * Returns the bandwidth of the network in Mbits/second. If the
     * driver cannot determine the network bandwidth, then it returns 0. 
     */
    virtual uint32_t getBandwidth()
    {
        return 0;
    }

    /**
     * This method provides a hint to transports about how many bytes
     * they should send. The driver will operate most efficiently if
     * transports don't send more bytes than indicated by the return value.
     * This will keep the output queue relatively short, which allows better
     * scheduling (e.g., a new short message or control packet can preempt
     * a long ongoing message). At the same time, it will allow enough
     * buffering so that the output queue doesn't completely run dry
     * (resulting in unused network bandwidth) before the transport's poller
     * checks this method again.
     * \param currentTime
     *      Current time, in Cycles::rdtsc ticks.
     * \return
     *      Number of bytes that can be transmitted without creating
     *      a long output queue in the driver. Value may be negative
     *      if transport has ignored this method and transmitted too
     *      many bytes.
     */
    VIRTUAL_FOR_TESTING int
    getTransmitQueueSpace(uint64_t currentTime)
    {
        return static_cast<int>(maxTransmitQueueSize) -
                queueEstimator.getQueueSize(currentTime);
    }

    /**
     * The most recent time that the driver handed a packet to the NIC.
     * Mainly used for performance debugging.
     *
     * \return
     *      Last transmit time, in Cycles::rdtsc ticks
     */
    uint64_t getLastTransmitTime()
    {
        return lastTransmitTime;
    }

    /**
     * Returns the total protocol overhead involved in transmitting one
     * packet, in bytes. This is equal to # bytes in the physical layer
     * data packet on the wire (e.g., preamble, inter-packet gaps,etc.)
     * minus the size of the payload passed to the #sendPacket method.
     * 0 means this feature has not been implemented by the driver.
     */
    virtual uint32_t getPacketOverhead()
    {
        // Default: not implemented
        return 0;
    }

    /**
     * Release the packet buffers previously enqueued by Driver::returnPacket
     * so their associated resources can be recycled.
     *
     * Invoked by a transport in its poll method.
     */
    virtual void release() = 0;

    /**
     * Return ownership of a packet buffer back to the driver so the packet
     * buffer can be safely released later.
     *
     * This method is usually invoked by a transport when it has finished
     * processing the data in an incoming packet. This method can also be
     * invoked by an RPC client when it has finished processing the RPC reply.
     * Note that this method does *NOT* actually release the packet buffer;
     * the transport must invoke Driver::release in its poll method for that
     * to happen.
     *
     * Defining this method here as inline allows the compiler to remove
     * unnecessary synchronization when it's called from the dispatch thread,
     * which is the common case.
     *
     * \param payload
     *      The first byte of a packet that was previously "stolen"
     *      (i.e., the payload field from the Received object used to
     *      pass the packet to the transport when it was received).
     * \param isDispatchThread
     *      True if this method is invoked from the dispatch thread.
     */
    VIRTUAL_FOR_TESTING void
    returnPacket(void* payload, bool isDispatchThread = true)
    {
        if (isDispatchThread) {
            assert(context->dispatch->isDispatchThread());
            packetsToRelease.push_back(static_cast<char*>(payload));
        } else {
            // Must sync with the dispatch thread, since this method could
            // be invoked from a worker thread (e.g. in ~PayloadChunk).
            Dispatch::Lock _(context->dispatch);
            packetsToRelease.push_back(static_cast<char*>(payload));
        }
    }

    /**
     * Return a new Driver-specific network address for the given service
     * locator.
     * This function may be called from worker threads and should contain any
     * necessary synchronization.
     * \param serviceLocator
     *      See above.
     * \return
     *      An address that must be deleted later by the caller.
     * \throw NoSuchKeyException
     *      Service locator option missing.
     * \throw BadValueException
     *      Service locator option malformed.
     */
    virtual Address* newAddress(const ServiceLocator* serviceLocator) = 0;

    /**
     * Checks to see if any packets have arrived that have not already
     * been returned by this method; if so, it returns some or all of
     * them.
     * \param maxPackets
     *      The maximum number of packets that should be returned by
     *      this application
     * \param receivedPackets
     *      Returned packets are appended to this vector, one Received
     *      object per packet, in order of packet arrival.
     */
    virtual void receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets) = 0;

    /**
     * Associates a contiguous region of memory to a NIC so that the memory
     * addresses within that region become direct memory accessible (DMA) for
     * the NIC. This method must be implemented in the driver code if
     * the NIC needs to do zero copy transmit of buffers within that region of
     * memory.
     * \param base
     *     pointer to the beginning of the memory region that is to be
     *     registered to the NIC.
     * \param bytes
     *     The total size in bytes of the memory region starting at \a base
     *     that is to be registered with the NIC.
     */
    virtual void registerMemory(void* base, size_t bytes) {}

    /**
     * Send a single packet out over this Driver. The packet will not
     * necessarily have been transmitted before this method returns.  If an
     * error occurs, this method will log the error and return without
     * sending anything; this method does not throw exceptions.
     *
     * header provides a means to slip data onto the front of the packet
     * without having to pay for a prepend to the Buffer containing the
     * packet payload data.
     *
     * \param recipient
     *      The address the packet should go to.
     * \param header
     *      Bytes placed in the packet ahead of those from payload. The
     *      driver will make a copy of this data, so the caller need not
     *      preserve it after the method returns, even if the packet hasn't
     *      yet been transmitted.
     * \param headerLen
     *      Length in bytes of the data in header.
     * \param payload
     *      A buffer iterator describing the bytes for the payload (the
     *      portion of the packet after the header).  May be NULL to
     *      indicate "no payload". Note: caller must preserve the buffer
     *      data (but not the actual iterator) even after the method returns,
     *      since the data may not yet have been transmitted.
     * \param priority
     *      The priority level of this packet. 0 is the lowest priority.
     * \param[out] txQueueState
     *      Used to retrieve state of the NIC's transmit queue just before
     *      this packet was handed to the NIC. NULL means the caller doesn't
     *      care about this value.
     */
    virtual void sendPacket(const Address* recipient,
                            const void* header,
                            uint32_t headerLen,
                            Buffer::Iterator* payload,
                            int priority = 0,
                            TransmitQueueState* txQueueState = NULL) = 0;

    /**
     * Alternate form of sendPacket.
     *
     * \param recipient
     *      Where to send the packet.
     * \param header
     *      Contents of this object will be placed in the packet ahead
     *      of payload.  The driver will make a copy of this data, so
     *      the caller need not preserve it after the method returns, even
     *      if the packet hasn't yet been transmitted.
     * \param payload
     *      A buffer iterator positioned at the bytes for the payload to
     *      follow the headerLen bytes from header.  May be NULL to
     *      indicate "no payload". Note: caller must preserve the buffer
     *      data (but not the actual iterator) even after the method returns,
     *      since the data may not yet have been transmitted.
     * \param priority
     *      The priority level of this packet. 0 is the lowest priority.
     * \param[out] txQueueState
     *      Used to retrieve state of the NIC's transmit queue just before
     *      this packet was handed to the NIC. NULL means the caller doesn't
     *      care about this value.
     */
    template<typename T>
    void sendPacket(const Address* recipient,
                    const T* header,
                    Buffer::Iterator* payload,
                    int priority = 0,
                    TransmitQueueState* txQueueState = NULL)
    {
        sendPacket(recipient, header, sizeof(T), payload, priority,
                txQueueState);
    }

    /**
     * Return the ServiceLocator for this Driver (which shouldn't contain
     * any transport-level information). If the Driver was not provided
     * static parameters (e.g. fixed TCP or UDP port), this function will
     * return a SerivceLocator with those dynamically allocated attributes.
     *
     * Enlisting the dynamic ServiceLocator with the Coordinator permits
     * other hosts to contact dynamically addressed services.
     */
    virtual string getServiceLocator() = 0;

    /**
     * The maximum amount of time it should take to drain the transmit
     * queue for this driver when it is completely full (i.e.,
     * getTransmitQueueSpace returns 0).  See the documentation for
     * getTransmitQueueSpace for motivation. Specified in units of
     * nanoseconds.
     */
    static const uint32_t MAX_DRAIN_TIME = 2000;

  PROTECTED:
    /// Shared RAMCloud information.
    Context* context;

    /// The most recent time that the driver handed a packet to the NIC,
    /// in rdtsc ticks.
    uint64_t lastTransmitTime;

    /// Upper limit on how many bytes should be queued for transmission
    /// at any given time.
    uint32_t maxTransmitQueueSize;

    /// Holds incoming packets that have been processed by the transport and
    /// can be safely released by the driver.
    std::vector<char*> packetsToRelease;

    /// Used to estimate # bytes outstanding in the NIC's transmit queue.
    QueueEstimator queueEstimator;

    DISALLOW_COPY_AND_ASSIGN(Driver)
};

/**
 * Thrown if a Driver cannot be initialized properly.
 */
struct DriverException: public Exception {
    explicit DriverException(const CodeLocation& where)
        : Exception(where) {}
    DriverException(const CodeLocation& where, std::string msg)
        : Exception(where, msg) {}
    DriverException(const CodeLocation& where, int errNo)
        : Exception(where, errNo) {}
    DriverException(const CodeLocation& where, string msg, int errNo)
        : Exception(where, msg, errNo) {}
};


}  // namespace RAMCloud

#endif
