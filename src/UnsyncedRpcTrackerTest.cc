/* Copyright (c) 2014-2016 Stanford University
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
#include "UnsyncedRpcTracker.h"
#include "Memory.h"
#include "Transport.h"
#include "RamCloud.h"

namespace RAMCloud {

class UnsyncedRpcTrackerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    RamCloud ramcloud;
    UnsyncedRpcTracker* tracker;
    Transport::SessionRef session;
    void* request;

    UnsyncedRpcTrackerTest()
        : logEnabler()
        , context()
        , ramcloud(&context, "mock:host=coordinator")
        , tracker(ramcloud.unsyncedRpcTracker)
        , session(new Transport::Session("Test"))
        , request(Memory::xmalloc(HERE, 1000))
    {
    }

    ~UnsyncedRpcTrackerTest()
    {
    }

    DISALLOW_COPY_AND_ASSIGN(UnsyncedRpcTrackerTest);
};

TEST_F(UnsyncedRpcTrackerTest, registerUnsynced) {
    WireFormat::LogPosition logPos = {2, 10, 5};
    auto callback = []() {};
    tracker->registerUnsynced(session, request, 1, 2, 3, logPos, callback);

    EXPECT_EQ(1U, tracker->masters.size());
    EXPECT_EQ(2, session->refCount);
    auto master = tracker->masters[session.get()];
    EXPECT_EQ(session.get(), master->session.get());
    EXPECT_EQ(1U, master->rpcs.size());
    auto unsynced = &master->rpcs.front();
    EXPECT_EQ(request, unsynced->request);
    EXPECT_EQ(1UL, unsynced->tableId);
    EXPECT_EQ(2UL, unsynced->keyHash);
    EXPECT_EQ(3UL, unsynced->objVersion);
    EXPECT_EQ(2UL, unsynced->logPosition.segmentId);
    EXPECT_EQ(10UL, unsynced->logPosition.offset);
    free(request);
}

TEST_F(UnsyncedRpcTrackerTest, flushSession) {
    // TODO(seojin): implement test with fake transport?
}

TEST_F(UnsyncedRpcTrackerTest, UpdateSyncPoint) {
    WireFormat::LogPosition logPos = {2, 10, 5};
    bool callbackInvoked = false;
    auto callback = [&callbackInvoked]() {
        callbackInvoked = true;
    };
    tracker->registerUnsynced(session, request, 1, 2, 3, logPos, callback);

    auto master = tracker->masters[session.get()];
    EXPECT_EQ(1U, master->rpcs.size());

    // Normal case: GC and callback is invoked.
    WireFormat::LogPosition syncPos = {3, 1, 1};
    tracker->updateSyncPoint(session.get(), syncPos);
    EXPECT_TRUE(master->rpcs.empty());
    EXPECT_TRUE(callbackInvoked);

    // No matching session / master.
    tracker->updateSyncPoint(NULL, syncPos);
    EXPECT_TRUE(master->rpcs.empty());
}

}  // namespace RAMCloud
