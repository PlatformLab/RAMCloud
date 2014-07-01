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
#include "Util.h"

namespace RAMCloud {

TEST(UtilTest, timespecLess) {
    EXPECT_TRUE(Util::timespecLess({52, 50}, {53, 50}));
    EXPECT_TRUE(Util::timespecLess({10, 50}, {10, 51}));
    EXPECT_FALSE(Util::timespecLess({10, 50}, {10, 50}));
    EXPECT_FALSE(Util::timespecLess({10, 50}, {10, 30}));
    EXPECT_FALSE(Util::timespecLess({30, 50}, {29, 100}));
}

TEST(UtilTest, timespecLessEqual) {
    EXPECT_TRUE(Util::timespecLessEqual({52, 50}, {53, 50}));
    EXPECT_TRUE(Util::timespecLessEqual({10, 50}, {10, 51}));
    EXPECT_TRUE(Util::timespecLessEqual({10, 50}, {10, 50}));
    EXPECT_FALSE(Util::timespecLessEqual({10, 50}, {10, 30}));
    EXPECT_FALSE(Util::timespecLessEqual({30, 50}, {29, 100}));
}

TEST(UtilTest, timespecAdd) {
    struct timespec result;
    result = Util::timespecAdd({10, 20}, {30, 40});
    EXPECT_EQ(40, result.tv_sec);
    EXPECT_EQ(60, result.tv_nsec);
    result = Util::timespecAdd({10, 1000000020}, {30, 4000000006});
    EXPECT_EQ(45, result.tv_sec);
    EXPECT_EQ(26, result.tv_nsec);
}

TEST(UtilTest, readPmc) {
    Util::mockPmcValue = 1;
    EXPECT_EQ(Util::readPmc(0), 1U);
    Util::mockPmcValue = 0;
}

TEST(UtilTest, pinThreadToCore) {
    cpu_set_t oldSet = Util::getCpuAffinity();
    Util::pinThreadToCore(0);
    EXPECT_EQ(sched_getcpu(), 0);
    Util::setCpuAffinity(oldSet);
}

TEST(UtilTest, getCpuAffinity) {
    cpu_set_t oldSet = Util::getCpuAffinity();
    for (int i = 0; i < CPU_COUNT(&oldSet); i++)
        EXPECT_TRUE(CPU_ISSET(i, &oldSet));
}

TEST(UtilTest, setCpuAffinity) {
    cpu_set_t oldSet = Util::getCpuAffinity();
    Util::pinThreadToCore(0);
    Util::setCpuAffinity(oldSet);
    cpu_set_t newSet = Util::getCpuAffinity();
    EXPECT_TRUE(CPU_EQUAL(&oldSet, &newSet));
}


TEST(UtilTest, serialReadPmc) {
    Util::mockPmcValue = 1;
    EXPECT_EQ(Util::serialReadPmc(0), 1U);
}

}  // namespace RAMCloud
