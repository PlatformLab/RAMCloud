/* Copyright (c) 2011-2014 Stanford University
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

#include <regex>

#include "TestUtil.h"
#include "DpdkDriver.h"
#include "PerfStats.h"

namespace RAMCloud {

class DpdkDriverTest : public ::testing::Test {
  public:
    Context context;

    DpdkDriver driver;

    TestLog::Enable logEnabler;

    DpdkDriverTest()
        : context()
        , driver()
        , logEnabler()
    {
        driver.context = &context;
    }

    ~DpdkDriverTest() {}

  private:
    DISALLOW_COPY_AND_ASSIGN(DpdkDriverTest);
};

// Currently receivePackets cannot be effectively tested with unit tests.

TEST_F(DpdkDriverTest, getHighestPacketPriority) {
    driver.lowestPriorityAvail = 0;
    driver.highestPriorityAvail = 7;
    EXPECT_EQ(7, driver.getHighestPacketPriority());

    driver.lowestPriorityAvail = 2;
    driver.highestPriorityAvail = 5;
    EXPECT_EQ(3, driver.getHighestPacketPriority());
}

TEST_F(DpdkDriverTest, sendPacket_success) {
    PerfStats::threadStats.networkOutputBytes = 0;
    driver.lowestPriorityAvail = 1;
    driver.highestPriorityAvail = 6;

    MacAddress address("ff:ff:ff:ff:ff:ff");
    string header = "ABCDEFGH";
    string payload = "abcdefgh";
    Buffer buffer;
    buffer.append(payload.c_str(), downCast<uint32_t>(payload.length()));
    Buffer::Iterator iterator(&buffer);

    TestLog::reset();
    driver.sendPacket(&address, header.c_str(), (uint32_t)header.length(),
            &iterator, 3);

    string testLog = TestLog::get();
    std::regex regex("Ethernet frame header (.*), payload (.*)");
    std::smatch match;
    std::regex_search(testLog, match, regex);
    string hexEtherFrame = match.str(1);
    string etherPayload = match.str(2);

    // src mac address
    EXPECT_EQ("ffffffffffff", hexEtherFrame.substr(0, 12));
    // dest mac address
    EXPECT_EQ("0123456789ab", hexEtherFrame.substr(12, 12));
    // VLAN tagging frame type ID in network order
    EXPECT_EQ("8100", hexEtherFrame.substr(24, 4));
    // PCP field
    EXPECT_EQ("8000", hexEtherFrame.substr(28, 4));
    // RAMCloud raw-Ethernet frame type ID in network order
    EXPECT_EQ("88b5", hexEtherFrame.substr(32, 4));
    // Ethernet frame payload content
    EXPECT_EQ("ABCDEFGHabcdefgh", etherPayload);

    uint64_t packetBufLen = DpdkDriver::ETHER_VLAN_HDR_LEN + header.length() +
            payload.length();
    EXPECT_EQ(packetBufLen, (uint64_t) driver.queueEstimator.queueSize);
    EXPECT_EQ(packetBufLen, PerfStats::threadStats.networkOutputBytes);
}

}  // namespace RAMCloud
