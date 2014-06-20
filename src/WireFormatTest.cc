/* Copyright (c) 2010-2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.lie
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <string.h>
#include "TestUtil.h"
#include "WireFormat.h"

namespace RAMCloud {

class WireFormatTest : public ::testing::Test {
  public:
    WireFormatTest() { }

  private:
    DISALLOW_COPY_AND_ASSIGN(WireFormatTest);
};

TEST_F(WireFormatTest, serviceTypeSymbol) {
    EXPECT_STREQ("MASTER_SERVICE",
            WireFormat::serviceTypeSymbol(WireFormat::MASTER_SERVICE));
    EXPECT_STREQ("BACKUP_SERVICE",
            WireFormat::serviceTypeSymbol(WireFormat::BACKUP_SERVICE));
    EXPECT_STREQ("INVALID_SERVICE",
            WireFormat::serviceTypeSymbol(WireFormat::INVALID_SERVICE));

    // Test out-of-range values.
    EXPECT_STREQ("INVALID_SERVICE",
            WireFormat::serviceTypeSymbol(static_cast<WireFormat::ServiceType>
            (WireFormat::INVALID_SERVICE + 1)));
}

TEST_F(WireFormatTest, getStatus) {
    Buffer buffer;
    EXPECT_EQ(STATUS_RESPONSE_FORMAT_ERROR, WireFormat::getStatus(&buffer));
    WireFormat::ResponseCommon* header =
                buffer.emplaceAppend<WireFormat::ResponseCommon>();
    header->status = STATUS_WRONG_VERSION;
    EXPECT_EQ(STATUS_WRONG_VERSION, WireFormat::getStatus(&buffer));
}

TEST_F(WireFormatTest, opcodeSymbol_integer) {
    // Sample a few opcode values.
    EXPECT_STREQ("PING", WireFormat::opcodeSymbol(WireFormat::PING));
    EXPECT_STREQ("GET_TABLE_CONFIG", WireFormat::opcodeSymbol(
            WireFormat::GET_TABLE_CONFIG));
    EXPECT_STREQ("ILLEGAL_RPC_TYPE", WireFormat::opcodeSymbol(
            WireFormat::ILLEGAL_RPC_TYPE));

    // Test out-of-range values.
    EXPECT_STREQ("unknown(69)", WireFormat::opcodeSymbol(
            WireFormat::ILLEGAL_RPC_TYPE+1));

    // Make sure the next-to-last value is defined (this will fail if
    // someone adds a new opcode and doesn't update opcodeSymbol).
    EXPECT_EQ(string(WireFormat::opcodeSymbol(
            WireFormat::ILLEGAL_RPC_TYPE-1)).find("unknown"), string::npos);
}

TEST_F(WireFormatTest, opcodeSymbol_buffer) {
    // First, try an empty buffer with a valid header.
    Buffer b;
    EXPECT_STREQ("null", WireFormat::opcodeSymbol(&b));

    // Now try a buffer with a valid header.
    WireFormat::RequestCommon* header =
                b.emplaceAppend<WireFormat::RequestCommon>();
    header->opcode = WireFormat::PING;
    EXPECT_STREQ("PING", WireFormat::opcodeSymbol(&b));
}

}  // namespace RAMCloud
