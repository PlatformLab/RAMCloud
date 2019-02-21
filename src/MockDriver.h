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

#include <string>
#include "Driver.h"
#include "ServiceLocator.h"

#ifndef RAMCLOUD_MOCKDRIVER_H
#define RAMCLOUD_MOCKDRIVER_H

namespace RAMCloud {

/**
 * A Driver that allows unit tests to run without a network or a
 * remote counterpart.  It logs output packets and provides a mechanism for
 * prespecifying input packets.
 */
class MockDriver : public Driver {
  public:
    struct MockAddress : public Address {
        explicit MockAddress(string address)
            : address(address) {}
        MockAddress(const MockAddress& other)
            : Address(other)
            , address(other.address) {}
        MockAddress* clone() const {
            return new MockAddress(address);
        }
        bool operator==(const MockAddress& other) const {
            return (this->address == other.address);
        }
        uint64_t getHash() const {
            return std::hash<std::string>{}(address);
        }
        string toString() const {
            return address;
        }
        string address;
      private:
        void operator=(const MockAddress&);
    };

    /**
     * Holds an incoming packet and its Address.
     */
    static const int MAX_PAYLOAD_SIZE  = 1400;
    struct PacketBuf {
        MockAddress address;                   /// Address of sender.
        uint32_t length;                       /// Number of valid bytes in
                                               /// payload.
        char payload[MAX_PAYLOAD_SIZE];        /// Packet data (may not fill all
                                               /// of the allocated space).
        PacketBuf(const char* sender, const void* header,
                uint32_t headerLength, const char* body);
        ~PacketBuf()
        {
            // Trash the payload to cause problems for anyone trying to use
            // it after it has been freed.
            memset(payload, 'x', MAX_PAYLOAD_SIZE);
        }
    };

    /// The type of a customer header serializer.  See headerToString.
    typedef string (*HeaderToString)(const void*, uint32_t);

    explicit MockDriver(Context* context, HeaderToString headerToString);
    virtual ~MockDriver();
    virtual int getHighestPacketPriority() { return 7; }
    virtual uint32_t getMaxPacketSize() { return MAX_PAYLOAD_SIZE; }
#if TESTING
    virtual int getTransmitQueueSpace(uint64_t currentTime) {
        return transmitQueueSpace;
    }
#endif
    virtual void receivePackets(uint32_t maxPackets,
            std::vector<Received>* receivedPackets);
    virtual void release()
    {
        // Nothing to do; packet bufs are directly released in returnPacket
        // when testing.
        assert(packetsToRelease.empty());
    }
#if TESTING
    /**
     * Counts number of times returnPacket is called to allow unit tests
     * to check that Driver resources are properly reclaimed.
     */
    virtual void returnPacket(void* payload, bool isDispatchThread) {
        delete reinterpret_cast<PacketBuf*>(static_cast<char*>(payload) -
                OFFSET_OF(PacketBuf, payload));
        releaseCount++;
    }
#endif
    virtual void sendPacket(const Address* addr,
                            const void* header,
                            uint32_t headerLen,
                            Buffer::Iterator* payload,
                            int priority = 0,
                            TransmitQueueState* txQueueState = NULL);
    virtual string getServiceLocator();

    /**
     * Simulates the arrival of a packet in the driver.
     * @param sender
     *      String that will be treated as the Address for the sender.
     * @param header
     *      Object supplying the initial bytes of the packet.
     * @param body
     *      Null-terminated string that will be concatenated with the header
     *      to form the packet.  NULL means no body.
     */
    template<typename T>
    void
    packetArrived(const char* sender, T header, const char* body = NULL)
    {
        PacketBuf* packet = new PacketBuf(sender, &header, sizeof32(header),
                body);
        incomingPackets.push_back(packet);
    }

    virtual Address* newAddress(const ServiceLocator* serviceLocator) {
        return new MockAddress(serviceLocator->originalString);
    }

    /**
     * A function that serializes the header using a specific string format.
     * Headers aren't included in the log string if this is NULL.
     */
    HeaderToString headerToString;

    /**
     * Records information from each call to send/recv packets.
     */
    string outputLog;

    // The following variables count calls to various methods, for use
    // by tests.
    uint32_t sendPacketCount;
    uint32_t releaseCount;

    // Pointers to all of the packets passed that were created by
    // packetArrived but haven't yet been passed on to a transport.
    // These are all dynamically allocated objects.
    std::vector<PacketBuf*> incomingPackets;

    // Returned as the result of getTransmitQueueSpace.
    int transmitQueueSpace;

    DISALLOW_COPY_AND_ASSIGN(MockDriver);
};

}  // namespace RAMCloud

#endif
