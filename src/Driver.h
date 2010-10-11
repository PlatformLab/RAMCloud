/* Copyright (c) 2010 Stanford University
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

#include "Common.h"
#include "Buffer.h"

#undef CURRENT_LOG_MODULE
#define CURRENT_LOG_MODULE RAMCloud::TRANSPORT_MODULE

namespace RAMCloud {

class ServiceLocator;

/**
 * Sends and receives packets.  Abstract class.
 *
 * For use with FastTransport.  See UdpDriver for an example of a concrete
 * implementation.
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
    };

    typedef std::auto_ptr<Address> AddressPtr;

    /**
     * Represents a received packet.
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
    struct Received {
        Received();
        VIRTUAL_FOR_TESTING ~Received();
        // Note: getOffset is defined in this header file.
        template<typename T> T* getOffset(uint32_t offset);
        void* getRange(uint32_t offset, uint32_t length);
        VIRTUAL_FOR_TESTING char *steal(uint32_t *len);

        /// Address from which this data was received.
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

    virtual ~Driver();

    /**
     * The maximum number of bytes this Driver can transmit in a single call
     * to sendPacket including both header and payload.
     */
    virtual uint32_t getMaxPacketSize() = 0;

    virtual void release(char *payload, uint32_t len);

    /**
     * Return a new Driver-specific network address for the given service
     * locator.
     * \param serviceLocator
     *      See above.
     * \return
     *      An address that must be deleted later by the caller.
     * \throw NoSuchKeyException
     *      Service locator option missing.
     * \throw BadValueException
     *      Service locator option malformed.
     */
    virtual Address* newAddress(const ServiceLocator& serviceLocator) = 0;

    /**
     * Send a single packet out over this Driver.
     *
     * header provides a means to slip data onto the front of the packet
     * without having to pay for a prepend to the Buffer containing the
     * packet payload data.
     *
     * \param recipient
     *      The address the packet should go to.
     * \param header
     *      Bytes placed in the packet ahead of those from payload.
     * \param headerLen
     *      Length in bytes of the data in header.
     * \param payload
     *      A buffer iterator positioned at the bytes for the payload to
     *      follow the headerLen bytes from header.
     */
    virtual void sendPacket(const Address* recipient,
                            const void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload) = 0;

    /**
     * Try to receive a packet off the network.
     *
     * \param[out] received
     *      A Received wrapping the received data.  The object will return
     *      allocated resources to this driver when destroyed unless steal()
     *      is called.  See release() and steal() for more detail.
     * \return
     *      true if a packet was received.
     *      false otherwise.
     */
    virtual bool tryRecvPacket(Received *received) = 0;
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
 * a serious problem causing it to panic.
 */
struct UnrecoverableDriverException: public Exception {
    UnrecoverableDriverException() : Exception() {}
    explicit UnrecoverableDriverException(std::string msg)
        : Exception(msg) {}
    explicit UnrecoverableDriverException(int errNo) : Exception(errNo) {}
};


}  // namespace RAMCloud

#endif
