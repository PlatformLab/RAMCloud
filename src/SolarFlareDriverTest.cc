/* Copyright (c) 2014 Stanford University
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

#include "TestUtil.h"
#include "SolarFlareDriver.h"
#include "MockSyscall.h"

namespace RAMCloud {

using namespace NetUtil; //NOLINT

// N.B. this test only runs if you have SolarFalre NIC installed on
// your machine.
class SolarFlareDriverTest : public::testing::Test {
  public:
    Context context;
    SolarFlareDriver* serverDriver;
    SolarFlareDriver* clientDriver;
    Driver::Address* serverAddress;
    MockSyscall* mockSys;
    Syscall* savedSys;
    string exceptionMsg;

    SolarFlareDriverTest()
        : context()
        , serverDriver(NULL)
        , clientDriver(NULL)
        , serverAddress(NULL)
        , mockSys(NULL)
        , savedSys(NULL)
        , exceptionMsg()
    {
        mockSys = new MockSyscall();
        savedSys = SolarFlareDriver::sys;
        SolarFlareDriver::sys = mockSys;
        serverDriver = new SolarFlareDriver(&context, NULL);
        ServiceLocator serverLocator(serverDriver->localStringLocator.c_str());
        clientDriver = new SolarFlareDriver(&context, NULL);
        serverAddress = clientDriver->newAddress(serverLocator);
    }

    ~SolarFlareDriverTest()
    {
        SolarFlareDriver::sys = savedSys;
        delete mockSys;
        delete serverAddress;
        delete serverDriver;
        delete clientDriver;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(SolarFlareDriverTest);
};

TEST_F(SolarFlareDriverTest, constructor_NullLocatorErrors) {

    // Socket error test
    TestLog::Enable _;
    mockSys->socketErrno = EPERM;
    try {
        SolarFlareDriver testDriver(&context, NULL);
    } catch (DriverException& e){
        exceptionMsg = e.message;
    }
    EXPECT_EQ(exceptionMsg, "Could not create socket for SolarFlareDriver."
        ": Operation not permitted");
    mockSys->socketErrno = 0;

    // Bind error test
    TestLog::reset();
    mockSys->bindErrno = EADDRINUSE;
    try {
        SolarFlareDriver testDriver(&context, NULL);
    } catch (DriverException& e){
        exceptionMsg = e.message;
    }
    string ipAddr = getLocalIp(SolarFlareDriver::ifName);
    string errorStr =
            format("SolarFlareDriver could not bind the socket to %s."
            ": Address already in use", ipAddr.c_str());
    EXPECT_EQ(exceptionMsg, errorStr.c_str());
    mockSys->bindErrno = 0;

    // getsockname error
    TestLog::reset();
    mockSys->getsocknameErrno = EBADF;
    try {
        SolarFlareDriver testDriver(&context, NULL);
    } catch(DriverException& e) {
        exceptionMsg = e.message;
    }
    EXPECT_EQ(exceptionMsg, "Error in binding SolarFlare socket to a"
            " Kernel socket port.: Bad file descriptor");
    mockSys->getsocknameErrno = 0;
}

TEST_F(SolarFlareDriverTest, sendPacket_zeroCopyNoPayload) {

    // Register an arbitrary memory chunk to the NIC.
    uint32_t regBytes = 4096 * (1 << 4);
    void* memoryChunk = Memory::xmemalign(HERE, 4096, regBytes);
    clientDriver->registerMemory(memoryChunk, regBytes);

    // Defining the header and sending the packet.
    TestLog::Enable _;
    SolarFlareDriver::PacketBuff* pktBuff =
        clientDriver->txBufferPool->freeBuffersVec.back();
    string hdr = "header:";
    size_t l2AndL3HdrSize =
        sizeof(EthernetHeader) + sizeof(IpHeader) + sizeof(UdpHeader);
    size_t totalHdrSize = l2AndL3HdrSize + hdr.size();
    clientDriver->sendPacket(serverAddress, hdr.c_str(),
            downCast<uint32_t>(hdr.size()), NULL);
    string logStr =
        format("sendPacket: Total number of IoVecs are 1 |"
        " sendPacket: IoVec 0 starting at %lu and size %lu",
        pktBuff->dmaBufferAddress, totalHdrSize);
    EXPECT_EQ(logStr.c_str(), TestLog::get());

    free(memoryChunk);
}

TEST_F(SolarFlareDriverTest, sendPacket_zeroCopyMultiplePayloadChunks) {

    // Register an arbitrary memory chunk to the NIC.
    uint32_t regBytes = 4096 * (1 << 4);
    void* memoryChunk = Memory::xmemalign(HERE, 4096, regBytes);
    clientDriver->registerMemory(memoryChunk, regBytes);

    // Test  there are three pieces of payload. First and last pieces are
    // not part of log memory but the middle piece is part of log memory.
    string hdr = "header:";
    size_t l2AndL3HdrSize =
        sizeof(EthernetHeader) + sizeof(IpHeader) + sizeof(UdpHeader);
    size_t totalHdrSize = l2AndL3HdrSize + hdr.size();

    Buffer buffer;
    uint32_t dataSubLen = 300;

    // The piece of payload that is not inside the registered memory region.
    char* nonRegisteredData1 = reinterpret_cast<char*>(malloc(dataSubLen));
    buffer.appendExternal(nonRegisteredData1, dataSubLen);

    // The two pieces of payload that are inside the registered memory region.
    uint32_t registeredDataOffset = 100;
    char* registeredData =
        reinterpret_cast<char*>(memoryChunk) + registeredDataOffset;
    buffer.appendExternal(registeredData, dataSubLen);

    char* nonRegisteredData2 = reinterpret_cast<char*>(malloc(dataSubLen));
    buffer.appendExternal(nonRegisteredData2, dataSubLen);

    Buffer::Iterator payload(&buffer);
    SolarFlareDriver::PacketBuff* pktBuff =
        clientDriver->txBufferPool->freeBuffersVec.back();

    TestLog::Enable _;
    TestLog::reset();
    clientDriver->sendPacket(serverAddress, hdr.c_str(),
            downCast<uint32_t>(hdr.size()), &payload);
    string logStr =
        format("sendPacket: Total number of IoVecs are 4 |"
        " sendPacket: IoVec 0 starting at %lu and size %lu |"
        " sendPacket: IoVec 1 starting at %lu and size %u |"
        " sendPacket: IoVec 2 starting at %lu and size %u |"
        " sendPacket: IoVec 3 starting at %lu and size %u",
        pktBuff->dmaBufferAddress, totalHdrSize,
        pktBuff->dmaBufferAddress + totalHdrSize, dataSubLen,
        ef_memreg_dma_addr(&clientDriver->logMemoryReg, registeredDataOffset),
        dataSubLen,
        pktBuff->dmaBufferAddress + totalHdrSize + dataSubLen, dataSubLen);
    EXPECT_EQ(logStr.c_str(), TestLog::get());

    free(nonRegisteredData1);
    free(nonRegisteredData2);
    free(memoryChunk);
}

} // namespace RAMCloud
