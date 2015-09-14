/* Copyright (c) 2015 Stanford University
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

#include "TestUtil.h"
#include "RpcLevel.h"

namespace RAMCloud {

TEST(RpcLevelTest, checkCall) {
    TestLog::Enable _;
    RpcLevel::setCurrentOpcode(WireFormat::Opcode::BACKUP_WRITE);
    RpcLevel::checkCall(WireFormat::Opcode::WRITE);
    EXPECT_EQ("checkCall: Unexpected RPC from BACKUP_WRITE to WRITE "
            "creates potential for deadlock; must update 'callees' "
            "table in scripts/genLevels.py", TestLog::get());

    TestLog::reset();
    RpcLevel::setCurrentOpcode(WireFormat::Opcode::WRITE);
    RpcLevel::checkCall(WireFormat::Opcode::BACKUP_WRITE);
    EXPECT_EQ("", TestLog::get());
}

TEST(RpcLevelTest, getLevel) {
    EXPECT_EQ(2, RpcLevel::getLevel(WireFormat::Opcode::CREATE_TABLE));
}

TEST(RpcLevelTest, maxLevel) {
    RpcLevel::savedMaxLevel = 11;
    EXPECT_EQ(11, RpcLevel::maxLevel());

    RpcLevel::savedMaxLevel = -1;
    EXPECT_EQ(2, RpcLevel::maxLevel());
    EXPECT_EQ(2, RpcLevel::savedMaxLevel);
}

}  // namespace RAMCloud
