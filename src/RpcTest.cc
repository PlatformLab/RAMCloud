/* Copyright (c) 2010 Stanford University
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
#include "Rpc.h"

namespace RAMCloud {

class RpcTest : public ::testing::Test {
  public:
    RpcTest() { }

  private:
    DISALLOW_COPY_AND_ASSIGN(RpcTest);
};

TEST_F(RpcTest, serviceTypeSymbol) {
    EXPECT_STREQ("MASTER_SERVICE", Rpc::serviceTypeSymbol(MASTER_SERVICE));
    EXPECT_STREQ("BACKUP_SERVICE", Rpc::serviceTypeSymbol(BACKUP_SERVICE));
    EXPECT_STREQ("INVALID_SERVICE", Rpc::serviceTypeSymbol(INVALID_SERVICE));

    // Test out-of-range values.
    EXPECT_STREQ("INVALID_SERVICE",
        Rpc::serviceTypeSymbol(static_cast<ServiceType>(INVALID_SERVICE + 1)));
}

TEST_F(RpcTest, opcodeSymbol_integer) {
    // Sample a few opcode values.
    EXPECT_STREQ("PING", Rpc::opcodeSymbol(PING));
    EXPECT_STREQ("GET_TABLET_MAP", Rpc::opcodeSymbol(GET_TABLET_MAP));
    EXPECT_STREQ("ILLEGAL_RPC_TYPE", Rpc::opcodeSymbol(ILLEGAL_RPC_TYPE));

    // Test out-of-range values.
    EXPECT_STREQ("unknown(48)", Rpc::opcodeSymbol(ILLEGAL_RPC_TYPE+1));

    // Make sure the next-to-last value is defined (this will fail if
    // someone adds a new opcode and doesn't update opcodeSymbol).
    EXPECT_EQ(string(Rpc::opcodeSymbol(ILLEGAL_RPC_TYPE-1)).find("unknown"),
        string::npos);
}

TEST_F(RpcTest, opcodeSymbol_buffer) {
    // First, try an empty buffer with a valid header.
    Buffer b;
    EXPECT_STREQ("null", Rpc::opcodeSymbol(b));

    // Now try a buffer with a valid header.
    RpcRequestCommon* header = new(&b, APPEND) RpcRequestCommon;
    header->opcode = PING;
    EXPECT_STREQ("PING", Rpc::opcodeSymbol(b));
}

}  // namespace RAMCloud
