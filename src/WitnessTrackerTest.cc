/* Copyright (c) 2014-2015 Stanford University
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
#include "WitnessTracker.h"

namespace RAMCloud {

class WitnessTrackerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    WitnessTracker tracker;

    WitnessTrackerTest()
        : logEnabler()
        , tracker()
    {}

    DISALLOW_COPY_AND_ASSIGN(WitnessTrackerTest);
};

TEST_F(WitnessTrackerTest, free) {
    tracker.free(1, 22, 33);
    WitnessTracker::WitnessTableId key = {1, 22};
    EXPECT_EQ(33, tracker.deletable[key].top());
    tracker.free(1, 22, 34);
    EXPECT_EQ(34, tracker.deletable[key].top());
    tracker.free(1, 23, 35);
    EXPECT_EQ(34, tracker.deletable[key].top());
    WitnessTracker::WitnessTableId key2 = {1, 23};
    EXPECT_EQ(35, tracker.deletable[key2].top());
}

TEST_F(WitnessTrackerTest, getDeletable) {
    // Enough deletables to entirely populate deletableIndices array
    tracker.free(1, 22, 33);
    tracker.free(1, 22, 34);
    tracker.free(1, 22, 35);
    int16_t deletableIndices[3];
    tracker.getDeletable(1, 22, deletableIndices);

    WitnessTracker::WitnessTableId key = {1, 22};
    EXPECT_TRUE(tracker.deletable[key].empty());
    EXPECT_EQ(35, deletableIndices[0]);
    EXPECT_EQ(34, deletableIndices[1]);
    EXPECT_EQ(33, deletableIndices[2]);


    // Not enough deletables to entirely populate deletableIndices array.
    tracker.free(1, 22, 33);
    tracker.free(1, 22, 34);
    tracker.getDeletable(1, 22, deletableIndices);

    EXPECT_TRUE(tracker.deletable[key].empty());
    EXPECT_EQ(34, deletableIndices[0]);
    EXPECT_EQ(33, deletableIndices[1]);
    EXPECT_EQ(-1, deletableIndices[2]);

    // No deletables.
    tracker.getDeletable(1, 22, deletableIndices);

    EXPECT_TRUE(tracker.deletable[key].empty());
    EXPECT_EQ(-1, deletableIndices[0]);
    EXPECT_EQ(-1, deletableIndices[1]);
    EXPECT_EQ(-1, deletableIndices[2]);

    tracker.getDeletable(1, 27, deletableIndices);

    WitnessTracker::WitnessTableId key2 = {1, 27};
    EXPECT_TRUE(tracker.deletable[key2].empty());
    EXPECT_EQ(-1, deletableIndices[0]);
    EXPECT_EQ(-1, deletableIndices[1]);
    EXPECT_EQ(-1, deletableIndices[2]);
}

}
