/* Copyright (c) 2012 Stanford University
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
#include "ServiceMask.h"

namespace RAMCloud {

class ServiceMaskTest : public ::testing::Test {
  public:
    ServiceMaskTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(ServiceMaskTest);
};

TEST_F(ServiceMaskTest, listConstructor) {
    EXPECT_EQ("", (ServiceMask{}.toString()));
    EXPECT_EQ("MASTER_SERVICE", (ServiceMask{MASTER_SERVICE}.toString()));
    EXPECT_EQ("MASTER_SERVICE, BACKUP_SERVICE",
              (ServiceMask{BACKUP_SERVICE, MASTER_SERVICE}.toString()));
}

TEST_F(ServiceMaskTest, has) {
    EXPECT_TRUE((ServiceMask{MASTER_SERVICE}.has(MASTER_SERVICE)));
    EXPECT_TRUE((ServiceMask{BACKUP_SERVICE}.has(BACKUP_SERVICE)));
    EXPECT_TRUE((ServiceMask{MASTER_SERVICE,
                             BACKUP_SERVICE}.has(BACKUP_SERVICE)));
    EXPECT_FALSE((ServiceMask{INVALID_SERVICE}.has(INVALID_SERVICE)));
}

TEST_F(ServiceMaskTest, toString) {
    EXPECT_EQ("MASTER_SERVICE",
              ServiceMask{MASTER_SERVICE}.toString());
    EXPECT_EQ("MASTER_SERVICE, BACKUP_SERVICE",
              (ServiceMask{MASTER_SERVICE, BACKUP_SERVICE}.toString()));
    EXPECT_EQ("MEMBERSHIP_SERVICE",
              ServiceMask{MEMBERSHIP_SERVICE}.toString());
    EXPECT_EQ("",
              ServiceMask{INVALID_SERVICE}.toString());
}

TEST_F(ServiceMaskTest, serialize) {
    EXPECT_EQ(0u, (ServiceMask{INVALID_SERVICE}.serialize()));
    EXPECT_EQ(1u, (ServiceMask{MASTER_SERVICE}.serialize()));
    EXPECT_EQ(3u, (ServiceMask{MASTER_SERVICE, BACKUP_SERVICE}.serialize()));
}

TEST_F(ServiceMaskTest, deserialize) {
    ServiceMask mask = ServiceMask::deserialize(0u);
    EXPECT_FALSE(mask.has(MASTER_SERVICE));
    EXPECT_FALSE(mask.has(BACKUP_SERVICE));
    mask = ServiceMask::deserialize(1u);
    EXPECT_TRUE(mask.has(MASTER_SERVICE));
    EXPECT_FALSE(mask.has(BACKUP_SERVICE));
    mask = ServiceMask::deserialize(2u);
    EXPECT_FALSE(mask.has(MASTER_SERVICE));
    EXPECT_TRUE(mask.has(BACKUP_SERVICE));
    mask = ServiceMask::deserialize(3u);
    EXPECT_TRUE(mask.has(MASTER_SERVICE));
    EXPECT_TRUE(mask.has(BACKUP_SERVICE));
    mask = ServiceMask::deserialize(1 << (INVALID_SERVICE - 1u));
    EXPECT_TRUE(mask.has(static_cast<ServiceType>(INVALID_SERVICE - 1u)));
    TestLog::Enable _;
    mask = ServiceMask::deserialize(1u << INVALID_SERVICE);
    EXPECT_EQ(
        "deserialize: Unexpected high-order bits set in SerializedServiceMask "
        "being deserialized which do not correspond to a valid "
        "ServiceType; ignoring the extra bits; you might want to "
        "check your code closely; something is wrong.",
        TestLog::get());
}

} // namespace RAMCloud
