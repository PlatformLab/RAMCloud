/* Copyright (c) 2010-2015 Stanford University
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
#include "Common.h"
#include "Buffer.h"

#undef CURRENT_LOG_MODULE
#define CURRENT_LOG_MODULE RAMCloud::TRANSPORT_MODULE

namespace RAMCloud {

class ServiceLocator;

/**
 * Used by transports such as FastTransport to send and receive unreliable
 * datagrams. This is an abstract class; see UdpDriver for an example of a
 * concrete implementation.
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
         * Copies an address.
         * \return
         *      An address that the caller must free later.
         */
        virtual Address* clone() const = 0;

        /**
         * Return a string describing the contents of this Address (for
         * debugging, logging, etc).
         */
        virtual string toString() const = 0;
    };

    typedef std::unique_ptr<Address> AddressPtr;

    /**
     * Represents an incoming packet.
     *
     * Provides a few safety and convenience methods for accessing packet data
     * and, most importantly, it returns memory resources to the Driver that
     * the packet data occupies.
     *
     * steal() allows higher-level code to take over the responsiblity of
     * returning the memory to the Driver (PayloadChunk, for example, allows
     * memory to be placed in a Buffer which is returned to the Driver when
     * its containing Buffer is destroyed).
     */
    class Received {
      public:
        Received();
        VIRTUAL_FOR_TESTING ~Received();
        // Note: getOffset is defined in this header file.
        template<typename T> T* getOffset(uint32_t offset);
        void* getRange(uint32_t offset, uint32_t length);
        VIRTUAL_FOR_TESTING char *steal(uint32_t *len);

        /// Address from which this data was received. The object referred
        /// to by this pointer will be stable as long as the packet data
        /// is stable (i.e., if steal() is invoked, then the Address will
        /// live until release is invoked).
        const Address* sender;

        /// Driver the packet came from, where resources should be returned.
        Driver *driver;

        /// Length in bytes of received data.
        uint32_t len;

        /// The start of the received data.  If non-NULL then we must return
        /// this storage to the driver when this object is deleted.  NULL means
        /// someone else (e.g. the Transport module) has taken responsibility
        /// for it.
        char *payload;

      private:
        DISALLOW_COPY_AND_ASSIGN(Received);
    };

    /// See #connect().
    struct IncomingPacketHandler {
        virtual ~IncomingPacketHandler() {}
        virtual void handlePacket(Received* received) = 0;
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
        static PayloadChunk* prependToBuffer(Buffer* buffer,
                                             char* data,
                                             uint32_t dataLength,
                                             Driver* driver,
                                             char* payload);
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


    virtual ~Driver();

    /// \copydoc Transport::dumpStats
    virtual void dumpStats() {}

    /**
     * The maximum number of bytes this Driver can transmit in a single call
     * to sendPacket including both header and payload.
     */
    virtual uint32_t getMaxPacketSize() = 0;

    /**
     * Invoked by a transport when it has finished processing the data
     * in an incoming packet; used by drivers to recycle packet buffers
     * at a safe time.
     *
     * \param payload
     *      The payload field from the Received object used to pass the
     *      packet to the transport when it was received.
     */
    virtual void release(char *payload);

    /**
     * Invoked by a transport to associate itself with this
     * driver, so that the driver can invoke the transport's
     * incoming packet handler whenever packets arrive.
     * \param incomingPacketHandler
     *      A functor which will be invoked for each incoming packet.
     *      This should be allocated with new and this Driver instance will
     *      take care of deleting it.
     */
    virtual void connect(IncomingPacketHandler* incomingPacketHandler) = 0;

    /**
     * Breaks the association between this driver and a particular
     * transport instance, if there was one. Once this method has
     * returned incoming packets will be ignored or discarded until
     * #connect is invoked again.
     */
    virtual void disconnect() = 0;

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
     */
    virtual void sendPacket(const Address* recipient,
                            const void* header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload) = 0;

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
     */
    template<typename T>
    void sendPacket(const Address* recipient,
                            const T* header,
                            Buffer::Iterator *payload)
    {
        sendPacket(recipient, header, sizeof(T), payload);
    }

    /**
     * Return the ServiceLocator for this Driver. If the Driver
     * was not provided static parameters (e.g. fixed TCP or UDP port),
     * this function will return a SerivceLocator with those dynamically
     * allocated attributes.
     *
     * Enlisting the dynamic ServiceLocator with the Coordinator permits
     * other hosts to contact dynamically addressed services.
     */
    virtual string getServiceLocator() = 0;
};

/**
 * Allows data at some offset into the Received to be treated as a
 * specific type.
 *
 * This must be in the header file because the template is instantiated
 * in multiple files.
 *
 * Example:  Header* header = getOffset<Header>(0);
 *
 * \tparam T
 *      The type to treat the data at payload + offset as.
 * \param offset
 *      An offset in the the Received payload to treat as a T.
 * \return
 *      A pointer to a T inside the Received payload.
 */
template<typename T>
T*
Driver::Received::getOffset(uint32_t offset)
{
    return static_cast<T*>(getRange(offset, sizeof(T)));
}

/**
 * Thrown if a Driver cannot be initialized properly or if it encounters
 * a problem sending or receiving a packet.
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
