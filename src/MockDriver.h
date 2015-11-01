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
        explicit MockAddress(const ServiceLocator* serviceLocator)
            : serviceLocator(*serviceLocator) {}
        MockAddress(const MockAddress& other)
            : Address(other), serviceLocator(other.serviceLocator) {}
        MockAddress* clone() const {
            return new MockAddress(*this);
        }
        bool operator==(const MockAddress& other) const {
            return (this->serviceLocator == other.serviceLocator);
        }
        string toString() const {
            return serviceLocator.getOriginalString();
        }
        ServiceLocator serviceLocator;
      private:
        void operator=(const MockAddress&);
    };

    class MockReceived: public Received {
      public:
        MockReceived(const char* sender, const void* header,
                uint32_t headerLength, const char* body);
        virtual ~MockReceived();
        virtual char *steal(uint32_t *len);

        // Locator corresponding to the sender argument to the constructor.
        ServiceLocator locator;

        // Address of the "sender".
        const MockAddress senderAddress;

        MockDriver* mockDriver;

        DISALLOW_COPY_AND_ASSIGN(MockReceived);
    };

    /// The type of a customer header serializer.  See headerToString.
    typedef string (*HeaderToString)(const void*, uint32_t);

    MockDriver();
    explicit MockDriver(HeaderToString headerToString);
    virtual ~MockDriver();
    virtual void connect(IncomingPacketHandler* incomingPacketHandler);
    virtual void disconnect();
    virtual uint32_t getMaxPacketSize() { return 1400; }
    virtual void release(char *payload);
    virtual void sendPacket(const Address* addr,
                            const void *header,
                            uint32_t headerLen,
                            Buffer::Iterator *payload);
    virtual string getServiceLocator();
    void receivePacket(MockReceived *received);

    template<typename T>
    inline void
    receivePacket(const char* sender, T header, const char* body = NULL)
    {
        receivePacket(new MockReceived(sender, &header, sizeof32(header),
                body));
    }

    virtual Address* newAddress(const ServiceLocator* serviceLocator) {
        return new MockAddress(serviceLocator);
    }

    /// Handler to invoke whenever packets arrive.
    std::unique_ptr<IncomingPacketHandler> incomingPacketHandler;

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
    uint32_t stealCount;
    uint32_t releaseCount;

    // Holds info about all of the MockReceived objects created, so
    // they can be freed when this object is destroyed.
    std::vector<MockReceived*> packets;

    DISALLOW_COPY_AND_ASSIGN(MockDriver);
};

}  // namespace RAMCloud

#endif
