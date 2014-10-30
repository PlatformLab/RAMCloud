/* Copyright (c) 2014 Stanford University
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

#include <array>
#include <fstream>

#include "TestUtil.h"
#include "NetUtil.h"
#include "MockSyscall.h"

namespace RAMCloud {
namespace NetUtil {

class NetUtilTest : public::testing::Test {
  public:
    Syscall* originalSys;
    MockSyscall* mockSys;

    NetUtilTest()
        : originalSys()
        , mockSys()
    {
        mockSys = new MockSyscall();
        originalSys = SysCallWrapper::sys;
        SysCallWrapper::sys = mockSys;
    }

    ~NetUtilTest()
    {
        delete mockSys;
        SysCallWrapper::sys = originalSys;
    }

    /**
     * This function parses /proc/net/arp file to get the name of one of the
     * valid Ethernet interfaces on local machine.
     *
     * \return 
     *      The interface name found in the arp file.
     */
    string
    getValidIfname() {
        std::ifstream arpFileStream("/proc/net/arp");
        std::string line;

        std::array<char[50], 6> arpEnt;
        while (getline(arpFileStream, line)) {
            sscanf(line.c_str(), "%s %s %s %s %s %s", arpEnt[0], arpEnt[1], //NOLINT
                    arpEnt[2], arpEnt[3], arpEnt[4], arpEnt[5]);

            // Check if the device type is Ethernet
            if (strcmp(arpEnt[1], "0x1") == 0)
                break;
        }
        return arpEnt[5];
    }

    DISALLOW_COPY_AND_ASSIGN(NetUtilTest);
};

TEST_F(NetUtilTest, getLocalMac) {

    // Test for the case that the interface name is long and invalid.
    const char* ifNameInvalid = "InvalidInterfaceName_MoreThan16CharLong";
    TestLog::Enable _;
    EXPECT_THROW(getLocalMac(ifNameInvalid), RAMCloud::FatalError);
    string logOutput =
        format("getLocalMac: The interface name %s"
            " is too long. | getLocalMac: IO control request"
            " failed. Couldn't find the mac address for ifName:"
            " %s", ifNameInvalid, ifNameInvalid);
    EXPECT_EQ(TestLog::get(), logOutput.c_str());

    // Error in opening socket.
    TestLog::reset();
    mockSys->socketErrno = -1;
    const char* ifName = "ETH_ERROR";
    EXPECT_THROW(getLocalMac(ifName), RAMCloud::FatalError);
    logOutput = "getLocalMac: Can't open socket for doing IO control.";
    EXPECT_EQ(TestLog::get(), logOutput.c_str());
    mockSys->socketErrno = 0;

    // Error in performing ioctl.
    TestLog::reset();
    mockSys->ioctlErrno = -1;
    EXPECT_THROW(getLocalMac(ifName), RAMCloud::FatalError);
    logOutput = format("getLocalMac: IO control request failed."
            " Couldn't find the mac address for ifName: %s", ifName);
    EXPECT_EQ(TestLog::get(), logOutput.c_str());
    mockSys->socketErrno = 0;

    // Find a valid interface name and check that the function returns
    // successfully.
    TestLog::disable();
    string validIfname = getValidIfname();
    EXPECT_NO_THROW(validIfname.c_str());
}

TEST_F(NetUtilTest, getLocalIp) {

    // Test for the case that the interface name is long and invalid.
    const char* ifNameInvalid = "InvalidInterfaceName_MoreThan16CharLong";
    TestLog::Enable _;
    EXPECT_THROW(getLocalIp(ifNameInvalid), RAMCloud::FatalError);
    string logOutput =
        format("getLocalIp: The interface name %s"
            " is too long. | getLocalIp: IO control request"
            " failed. Couldn't find the mac address for ifName:"
            " %s", ifNameInvalid, ifNameInvalid);
    EXPECT_EQ(TestLog::get(), logOutput.c_str());

    // Error in opening socket.
    TestLog::reset();
    mockSys->socketErrno = -1;
    const char* ifName = "ifName_Error";
    EXPECT_THROW(getLocalIp(ifName), RAMCloud::FatalError);
    logOutput = "getLocalIp: Can't open socket for doing IO control.";
    EXPECT_EQ(TestLog::get(), logOutput.c_str());
    mockSys->socketErrno = 0;

    // Error in performing ioctl.
    TestLog::reset();
    mockSys->ioctlErrno = -1;
    EXPECT_THROW(getLocalIp(ifName), RAMCloud::FatalError);
    logOutput = format("getLocalIp: IO control request failed."
            " Couldn't find the mac address for ifName: %s", ifName);
    EXPECT_EQ(TestLog::get(), logOutput.c_str());
    mockSys->socketErrno = 0;

    // Find a valid interface name and check that the function returns
    // successfully.
    TestLog::disable();
    string validIfname = getValidIfname();
    EXPECT_NO_THROW(validIfname.c_str());

}

} // namespace NetUtil
} // namespace RAMCloud
