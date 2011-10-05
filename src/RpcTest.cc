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

TEST_F(RpcTest, opcodeToSymbol) {
    // Sample a few opcode values.
    EXPECT_STREQ("PING", Rpc::opcodeToSymbol(PING));
    EXPECT_STREQ("GET_TABLET_MAP", Rpc::opcodeToSymbol(GET_TABLET_MAP));
    EXPECT_STREQ("ILLEGAL_RPC_TYPE", Rpc::opcodeToSymbol(ILLEGAL_RPC_TYPE));

    // Test out-of-range values.
    EXPECT_STREQ("unknown(37)",
            Rpc::opcodeToSymbol(RpcOpcode(ILLEGAL_RPC_TYPE+1)));

    // Make sure the next-to-last value is defined (this will fail if
    // someone adds a new opcode and doesn't update opcodeToSymbol).
    EXPECT_TRUE(NULL == strstr(Rpc::opcodeToSymbol(
            RpcOpcode(ILLEGAL_RPC_TYPE-1)), "unknown"));
}

}  // namespace RAMCloud
