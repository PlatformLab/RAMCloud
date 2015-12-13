/* Copyright (c) 2015 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.xx
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
#include "TimeTraceUtil.h"

namespace RAMCloud {

class TimeTraceUtilTest : public ::testing::Test {
  public:

    TimeTraceUtilTest()
    {
    }

    ~TimeTraceUtilTest()
    {
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(TimeTraceUtilTest);
};

TEST_F(TimeTraceUtilTest, statusMsg) {
    EXPECT_STREQ("Worker finished Read in thread 6",
            TimeTraceUtil::statusMsg(6, WireFormat::Opcode::READ,
            TimeTraceUtil::RequestStatus::WORKER_DONE));
    EXPECT_STREQ("Worker finished Read in unknown thread",
            TimeTraceUtil::statusMsg(78, WireFormat::Opcode::READ,
            TimeTraceUtil::RequestStatus::WORKER_DONE));
}

TEST_F(TimeTraceUtilTest, queueLengthMsg) {
    EXPECT_STREQ("Master queue length is 21",
            TimeTraceUtil::queueLengthMsg(
            WireFormat::ServiceType::MASTER_SERVICE, 21));
    EXPECT_STREQ("Backup queue length is 100",
            TimeTraceUtil::queueLengthMsg(
            WireFormat::ServiceType::BACKUP_SERVICE, 100));
    EXPECT_STREQ("Ping queue length is > 100",
            TimeTraceUtil::queueLengthMsg(
            WireFormat::ServiceType::PING_SERVICE, 101));
}

TEST_F(TimeTraceUtilTest, initStatusMessages_opcodes) {
    EXPECT_STREQ("Worker finished Read in thread 6",
            TimeTraceUtil::statusMsg(6, WireFormat::Opcode::READ,
            TimeTraceUtil::RequestStatus::WORKER_DONE));
    EXPECT_STREQ("Worker finished Write in thread 6",
            TimeTraceUtil::statusMsg(6, WireFormat::Opcode::WRITE,
            TimeTraceUtil::RequestStatus::WORKER_DONE));
    EXPECT_STREQ("Worker finished BackupWrite in thread 6",
            TimeTraceUtil::statusMsg(6, WireFormat::Opcode::BACKUP_WRITE,
            TimeTraceUtil::RequestStatus::WORKER_DONE));
    EXPECT_STREQ("Worker finished UnknownRPC in thread 6",
            TimeTraceUtil::statusMsg(6, WireFormat::Opcode::DROP_TABLE,
            TimeTraceUtil::RequestStatus::WORKER_DONE));
}
TEST_F(TimeTraceUtilTest, initStatusMessages_statuses) {
    EXPECT_STREQ("Handoff to worker for Read in thread 6",
            TimeTraceUtil::statusMsg(6, WireFormat::Opcode::READ,
            TimeTraceUtil::RequestStatus::HANDOFF));
    EXPECT_STREQ("Worker sleeping after Write in thread 6",
            TimeTraceUtil::statusMsg(6, WireFormat::Opcode::WRITE,
            TimeTraceUtil::RequestStatus::WORKER_SLEEP));
}
TEST_F(TimeTraceUtilTest, initStatusMessages_threadIds) {
    EXPECT_STREQ("Handoff to worker for Read in thread 0",
            TimeTraceUtil::statusMsg(0, WireFormat::Opcode::READ,
            TimeTraceUtil::RequestStatus::HANDOFF));
    EXPECT_STREQ("Worker sleeping after Write in thread 20",
            TimeTraceUtil::statusMsg(20, WireFormat::Opcode::WRITE,
            TimeTraceUtil::RequestStatus::WORKER_SLEEP));
    EXPECT_STREQ("Worker sleeping after Write in unknown thread",
            TimeTraceUtil::statusMsg(21, WireFormat::Opcode::WRITE,
            TimeTraceUtil::RequestStatus::WORKER_SLEEP));
}

TEST_F(TimeTraceUtilTest, initQueueLengthMessages_serviceTypes) {
    EXPECT_STREQ("Master queue length is 16",
            TimeTraceUtil::queueLengthMsg(
            WireFormat::ServiceType::MASTER_SERVICE, 16));
    EXPECT_STREQ("Membership queue length is 16",
            TimeTraceUtil::queueLengthMsg(
            WireFormat::ServiceType::MEMBERSHIP_SERVICE, 16));
}
TEST_F(TimeTraceUtilTest, initQueueLengthMessages_queueLengths) {
    EXPECT_STREQ("Master queue length is 100",
            TimeTraceUtil::queueLengthMsg(
            WireFormat::ServiceType::MASTER_SERVICE, 100));
    EXPECT_STREQ("Master queue length is > 100",
            TimeTraceUtil::queueLengthMsg(
            WireFormat::ServiceType::MASTER_SERVICE, 101));
}


}  // namespace RAMCloud
