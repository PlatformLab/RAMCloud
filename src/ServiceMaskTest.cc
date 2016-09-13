/* Copyright (c) 2012-2016 Stanford University
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
    EXPECT_EQ("MASTER_SERVICE",
        (ServiceMask{WireFormat::MASTER_SERVICE}.toString()));
    EXPECT_EQ("MASTER_SERVICE, BACKUP_SERVICE",
              (ServiceMask{WireFormat::BACKUP_SERVICE,
                           WireFormat::MASTER_SERVICE}.toString()));
}

TEST_F(ServiceMaskTest, has) {
    EXPECT_TRUE((ServiceMask{WireFormat::MASTER_SERVICE}.has(
        WireFormat::MASTER_SERVICE)));
    EXPECT_TRUE((ServiceMask{WireFormat::BACKUP_SERVICE}.has(
        WireFormat::BACKUP_SERVICE)));
    EXPECT_TRUE((ServiceMask{WireFormat::MASTER_SERVICE,
                             WireFormat::BACKUP_SERVICE}.has(
        WireFormat::BACKUP_SERVICE)));
    EXPECT_FALSE((ServiceMask{WireFormat::INVALID_SERVICE}.has(
        WireFormat::INVALID_SERVICE)));
}

TEST_F(ServiceMaskTest, hasAll) {
    ServiceMask master{WireFormat::MASTER_SERVICE};
    ServiceMask backup{WireFormat::BACKUP_SERVICE};
    ServiceMask both{WireFormat::MASTER_SERVICE, WireFormat::BACKUP_SERVICE};
    ServiceMask none{};

    EXPECT_FALSE(master.hasAll(both));
    EXPECT_TRUE(both.hasAll(both));
    EXPECT_TRUE(both.hasAll(master));
    EXPECT_TRUE(master.hasAll(none));
}

TEST_F(ServiceMaskTest, hasAny) {
    ServiceMask master{WireFormat::MASTER_SERVICE};
    ServiceMask backup{WireFormat::BACKUP_SERVICE};
    ServiceMask both{WireFormat::MASTER_SERVICE, WireFormat::BACKUP_SERVICE};
    ServiceMask none{};

    EXPECT_TRUE(master.hasAny(both));
    EXPECT_TRUE(both.hasAny(both));
    EXPECT_TRUE(both.hasAny(master));
    EXPECT_FALSE(master.hasAny(none));
}

TEST_F(ServiceMaskTest, toString) {
    EXPECT_EQ("MASTER_SERVICE",
              ServiceMask{WireFormat::MASTER_SERVICE}.toString());
    EXPECT_EQ("MASTER_SERVICE, BACKUP_SERVICE",
              (ServiceMask{WireFormat::MASTER_SERVICE,
                           WireFormat::BACKUP_SERVICE}.toString()));
    EXPECT_EQ("ADMIN_SERVICE",
              ServiceMask{WireFormat::ADMIN_SERVICE}.toString());
    EXPECT_EQ("",
              ServiceMask{WireFormat::INVALID_SERVICE}.toString());
}

TEST_F(ServiceMaskTest, serialize) {
    EXPECT_EQ(0u, (ServiceMask{WireFormat::INVALID_SERVICE}.serialize()));
    EXPECT_EQ(1u, (ServiceMask{WireFormat::MASTER_SERVICE}.serialize()));
    EXPECT_EQ(3u, (ServiceMask{WireFormat::MASTER_SERVICE,
                               WireFormat::BACKUP_SERVICE}.serialize()));
}

TEST_F(ServiceMaskTest, deserialize) {
    ServiceMask mask = ServiceMask::deserialize(0u);
    EXPECT_FALSE(mask.has(WireFormat::MASTER_SERVICE));
    EXPECT_FALSE(mask.has(WireFormat::BACKUP_SERVICE));
    mask = ServiceMask::deserialize(1u);
    EXPECT_TRUE(mask.has(WireFormat::MASTER_SERVICE));
    EXPECT_FALSE(mask.has(WireFormat::BACKUP_SERVICE));
    mask = ServiceMask::deserialize(2u);
    EXPECT_FALSE(mask.has(WireFormat::MASTER_SERVICE));
    EXPECT_TRUE(mask.has(WireFormat::BACKUP_SERVICE));
    mask = ServiceMask::deserialize(3u);
    EXPECT_TRUE(mask.has(WireFormat::MASTER_SERVICE));
    EXPECT_TRUE(mask.has(WireFormat::BACKUP_SERVICE));
    mask = ServiceMask::deserialize(1 << (WireFormat::INVALID_SERVICE - 1u));
    EXPECT_TRUE(mask.has(static_cast<WireFormat::ServiceType>(
        WireFormat::INVALID_SERVICE - 1u)));
    TestLog::Enable _;
    mask = ServiceMask::deserialize(1u << WireFormat::INVALID_SERVICE);
    EXPECT_EQ(
        "deserialize: Unexpected high-order bits set in SerializedServiceMask "
        "being deserialized which do not correspond to a valid "
        "ServiceType; ignoring the extra bits; you might want to "
        "check your code closely; something is wrong.",
        TestLog::get());
}

} // namespace RAMCloud
