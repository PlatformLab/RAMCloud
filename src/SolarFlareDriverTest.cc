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
#include "MockSyscall.h"
#include "SolarFlareDriver.h"

namespace RAMCloud {

using namespace NetUtil; //NOLINT

// N.B. this test only runs if you have SolarFalre NIC installed on
// your machine.
class SolarFlareDriverTest : public::testing::Test {
  public:
    Context context;
    Syscall* savedSys;
    MockSyscall* mockSys;
    string exceptionMsg;

    SolarFlareDriverTest()
        : context()
        , savedSys(NULL)
        , mockSys(NULL)
        , exceptionMsg()
    {
        mockSys = new MockSyscall();
        savedSys = SolarFlareDriver::sys;
        SolarFlareDriver::sys = mockSys;
    }

    ~SolarFlareDriverTest()
    {
        delete mockSys;
        SolarFlareDriver::sys = savedSys;
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

} // namespace RAMCloud
