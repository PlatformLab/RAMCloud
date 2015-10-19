/* Copyright (c) 2012-2015 Stanford University
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

#include "SegmentManager.h"
#include "SegmentIterator.h"
#include "LogDigest.h"
#include "LogMetadata.h"
#include "ServerConfig.h"
#include "ServerRpcPool.h"
#include "MasterTableMetadata.h"
#include "WorkerTimer.h"

namespace RAMCloud {

/**
 * Unit tests for SegmentManager.
 */
class SegmentManagerTest : public ::testing::Test {
  public:
    Context context;
    ServerId serverId;
    ServerList serverList;
    ServerConfig serverConfig;
    ReplicaManager replicaManager;
    MasterTableMetadata masterTableMetadata;
    SegletAllocator allocator;
    SegmentManager segmentManager;

    SegmentManagerTest()
        : context(),
          serverId(ServerId(57, 0)),
          serverList(&context),
          serverConfig(ServerConfig::forTesting()),
          replicaManager(&context, &serverId, 0, false, false),
          masterTableMetadata(),
          allocator(&serverConfig),
          segmentManager(&context, &serverConfig, &serverId,
                         allocator, replicaManager, &masterTableMetadata)
    {
    }

    // The following class is used for testing: it generates a log message
    // identifying this timer whenever it is invoked.
    class DummyWorkerTimer : public WorkerTimer {
      public:
        explicit DummyWorkerTimer(Dispatch* dispatch)
            : WorkerTimer(dispatch)
            , sleepMicroseconds(0)
        { }
        void handleTimerEvent() {
            if (sleepMicroseconds != 0) {
                usleep(sleepMicroseconds);
            }
        }

        // If non-zero, then handler will delayed for this long before
        // returning.
        int sleepMicroseconds;

      private:
        DISALLOW_COPY_AND_ASSIGN(DummyWorkerTimer);
    };

  private:
    DISALLOW_COPY_AND_ASSIGN(SegmentManagerTest);
};

TEST_F(SegmentManagerTest, constructor)
{
    serverConfig.master.diskExpansionFactor = 0.99;
    EXPECT_THROW(SegmentManager(&context,
                                &serverConfig,
                                &serverId,
                                allocator,
                                replicaManager,
                                &masterTableMetadata),
                 SegmentManagerException);

    EXPECT_EQ(1U, segmentManager.nextSegmentId);
    EXPECT_EQ(320U, segmentManager.maxSegments);
    EXPECT_EQ(segmentManager.maxSegments - 2,
              segmentManager.freeSlots.size());
    EXPECT_EQ(2U, allocator.getFreeCount(SegletAllocator::EMERGENCY_HEAD));
}

TEST_F(SegmentManagerTest, destructor) {
    SegletAllocator allocator2(&serverConfig);
    Tub<SegmentManager> mgr;
    mgr.construct(&context, &serverConfig, &serverId, allocator2,
                  replicaManager, &masterTableMetadata);
    EXPECT_EQ(2U, allocator2.getFreeCount(SegletAllocator::EMERGENCY_HEAD));
    EXPECT_EQ(0U, allocator2.getFreeCount(SegletAllocator::CLEANER));
    EXPECT_EQ(318U, allocator2.getFreeCount(SegletAllocator::DEFAULT));
    mgr->allocHeadSegment();
    mgr->allocHeadSegment();
    EXPECT_EQ(316U, allocator2.getFreeCount(SegletAllocator::DEFAULT));

    mgr.destroy();
    EXPECT_EQ(318U, allocator2.getFreeCount(SegletAllocator::DEFAULT));
}

static bool
allocFilter(string s)
{
    return s == "alloc";
}

TEST_F(SegmentManagerTest, allocHead) {
    TestLog::Enable _(allocFilter);

    EXPECT_EQ(static_cast<LogSegment*>(NULL), segmentManager.getHeadSegment());
    LogSegment* head = segmentManager.allocHeadSegment();
    EXPECT_EQ("alloc: purpose: 0", TestLog::get());
    EXPECT_NE(static_cast<LogSegment*>(NULL), head);
    EXPECT_EQ(head, segmentManager.getHeadSegment());

    SegmentIterator it(*head);
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGHEADER, it.getType());

    it.next();
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_LOGDIGEST, it.getType());

    it.next();
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_TABLESTATS, it.getType());

    it.next();
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_SAFEVERSION, it.getType());

    it.next();
    EXPECT_TRUE(it.isDone());

    LogSegment* oldHead = head;
    head = segmentManager.allocHeadSegment();
    EXPECT_NE(static_cast<LogSegment*>(NULL), head);
    EXPECT_NE(head, oldHead);
    EXPECT_EQ(head, segmentManager.getHeadSegment());
    EXPECT_EQ(oldHead,
       &segmentManager.segmentsByState[SegmentManager::NEWLY_CLEANABLE].back());
    EXPECT_TRUE(oldHead->closed);

    int successes = 0;
    while (segmentManager.allocHeadSegment() != NULL)
        successes++;
    EXPECT_EQ(316, successes);
    EXPECT_EQ(317U,
        segmentManager.segmentsByState[SegmentManager::NEWLY_CLEANABLE].size());
}

TEST_F(SegmentManagerTest, allocSideSegment_forRegularSideLog) {
    TestLog::Enable _(allocFilter);

    uint64_t regularSlotCount = segmentManager.freeSlots.size();
    LogSegment* s =
        segmentManager.allocSideSegment(0);
    EXPECT_NE(static_cast<LogSegment*>(NULL), s);
    EXPECT_EQ("alloc: purpose: 2", TestLog::get());
    EXPECT_EQ(regularSlotCount - 1, segmentManager.freeSlots.size());

    SegmentIterator it(*s);
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGHEADER, it.getType());
}

TEST_F(SegmentManagerTest, allocSideSegment_forCleanerSideLog) {
    TestLog::Enable _(allocFilter);

    segmentManager.initializeSurvivorReserve(1);
    uint64_t cleanerSlotCount = segmentManager.freeSurvivorSlots.size();
    LogSegment* s =
        segmentManager.allocSideSegment(SegmentManager::FOR_CLEANING);
    EXPECT_NE(static_cast<LogSegment*>(NULL), s);
    EXPECT_EQ("alloc: purpose: 3", TestLog::get());
    EXPECT_EQ(cleanerSlotCount - 1, segmentManager.freeSurvivorSlots.size());

    SegmentIterator it(*s);
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_SEGHEADER, it.getType());

    EXPECT_EQ(static_cast<LogSegment*>(NULL),
        segmentManager.allocSideSegment(SegmentManager::FOR_CLEANING));
}

static void
initCleanerSegmentPool(SegmentManager* sm)
{
    usleep(1000);
    sm->initializeSurvivorReserve(1);
}

TEST_F(SegmentManagerTest, allocSideSegment_mustNotFail) {
    EXPECT_EQ(0U, segmentManager.freeSurvivorSlots.size());
    EXPECT_EQ(static_cast<LogSegment*>(NULL),
            segmentManager.allocSideSegment(SegmentManager::FOR_CLEANING));

    // spin a thread that will sleep for 1ms and then initialize the cleaner
    // pool.
    std::thread thread(initCleanerSegmentPool, &segmentManager);

    // ensure that we wait around for the thread to fill the pool.
    LogSegment* s =
        segmentManager.allocSideSegment(SegmentManager::FOR_CLEANING |
                                        SegmentManager::MUST_NOT_FAIL);
    EXPECT_NE(static_cast<LogSegment*>(NULL), s);

    thread.join();
}

TEST_F(SegmentManagerTest, cleaningComplete) {
    LogSegment* cleaned = segmentManager.allocHeadSegment();
    EXPECT_NE(static_cast<LogSegment*>(NULL), cleaned);
    segmentManager.allocHeadSegment();
    EXPECT_NE(cleaned, segmentManager.getHeadSegment());

    // cleaned segments must always be in the CLEANABLE state
    segmentManager.changeState(*cleaned, SegmentManager::CLEANABLE);

    segmentManager.initializeSurvivorReserve(1);
    LogSegment* survivor = segmentManager.allocSideSegment();
    EXPECT_NE(static_cast<LogSegment*>(NULL), survivor);

    ServerRpcPoolInternal::currentEpoch = 17530;

    LogSegmentVector survivors;
    LogSegmentVector clean;
    survivors.push_back(survivor);
    clean.push_back(cleaned);
    segmentManager.cleaningComplete(clean, survivors);

    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::CLEANABLE_PENDING_DIGEST].size());
    EXPECT_EQ(survivor, &segmentManager.segmentsByState[
        SegmentManager::CLEANABLE_PENDING_DIGEST].back());

    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_DIGEST_AND_REFERENCES].size());
    EXPECT_EQ(cleaned, &segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_DIGEST_AND_REFERENCES].back());
    EXPECT_EQ(17531U, ServerRpcPoolInternal::currentEpoch);
    EXPECT_EQ(17530U, cleaned->cleanedEpoch);
}

TEST_F(SegmentManagerTest, cleanableSegments) {
    LogSegmentVector cleanable;

    EXPECT_EQ(0U, segmentManager.segmentsByState[
        SegmentManager::NEWLY_CLEANABLE].size());
    segmentManager.cleanableSegments(cleanable);
    EXPECT_EQ(0U, cleanable.size());

    segmentManager.allocHeadSegment();
    EXPECT_EQ(0U, segmentManager.segmentsByState[
        SegmentManager::NEWLY_CLEANABLE].size());
    segmentManager.cleanableSegments(cleanable);
    EXPECT_EQ(0U, cleanable.size());

    segmentManager.allocHeadSegment();
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::NEWLY_CLEANABLE].size());
    segmentManager.cleanableSegments(cleanable);
    EXPECT_EQ(0U, segmentManager.segmentsByState[
        SegmentManager::NEWLY_CLEANABLE].size());
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::CLEANABLE].size());
    EXPECT_EQ(1U, cleanable.size());
    EXPECT_EQ(cleanable[0], &segmentManager.segmentsByState[
        SegmentManager::CLEANABLE].back());

    cleanable.clear();
    EXPECT_EQ(0U, segmentManager.segmentsByState[
        SegmentManager::NEWLY_CLEANABLE].size());
    segmentManager.allocHeadSegment();
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::NEWLY_CLEANABLE].size());
    segmentManager.cleanableSegments(cleanable);
    EXPECT_EQ(0U, segmentManager.segmentsByState[
        SegmentManager::NEWLY_CLEANABLE].size());
    EXPECT_EQ(2U, segmentManager.segmentsByState[
        SegmentManager::CLEANABLE].size());
    EXPECT_EQ(1U, cleanable.size());
}

TEST_F(SegmentManagerTest, getActiveSegments) {
    LogSegmentVector active;

    EXPECT_NO_THROW(segmentManager.getActiveSegments(1, active));
    EXPECT_EQ(0U, active.size());

    LogSegment* newlyCleanable = segmentManager.allocHeadSegment();
    LogSegment* cleanable = segmentManager.allocHeadSegment();
    LogSegment* freeablePendingJunk = segmentManager.allocHeadSegment();
    LogSegment* head = segmentManager.allocHeadSegment();

    // "newlyCleanable" is in the correct state already, as is "head"
    segmentManager.changeState(*cleanable, SegmentManager::NEWLY_CLEANABLE);
    segmentManager.changeState(*freeablePendingJunk,
        SegmentManager::FREEABLE_PENDING_DIGEST_AND_REFERENCES);

    segmentManager.getActiveSegments(1, active);
    EXPECT_EQ(4U, active.size());
    EXPECT_EQ(newlyCleanable, active[0]);
    EXPECT_EQ(cleanable, active[1]);
    EXPECT_EQ(freeablePendingJunk, active[2]);
    EXPECT_EQ(head, active[3]);

    active.clear();
    segmentManager.getActiveSegments(3, active);
    EXPECT_EQ(2U, active.size());
    EXPECT_EQ(freeablePendingJunk, active[0]);
    EXPECT_EQ(head, active[1]);

    active.clear();
    segmentManager.getActiveSegments(head->id + 1, active);
    EXPECT_EQ(0U, active.size());
}

TEST_F(SegmentManagerTest, initializeSurvivorSegmentReserve) {
    LogSegment* nullSeg = NULL;
    EXPECT_EQ(nullSeg,
        segmentManager.allocSideSegment(SegmentManager::FOR_CLEANING));
    EXPECT_TRUE(segmentManager.initializeSurvivorReserve(1));
    LogSegment* s = segmentManager.allocSideSegment();
    EXPECT_NE(nullSeg, s);
}

TEST_F(SegmentManagerTest, indexOperator) {
    for (uint32_t i = 0; i < segmentManager.maxSegments + 5; i++)
        EXPECT_THROW(segmentManager[i], SegmentManagerException);
    LogSegment* head = segmentManager.allocHeadSegment();
    EXPECT_EQ(&*segmentManager.segments[head->slot],
        &segmentManager[head->slot]);
}

TEST_F(SegmentManagerTest, doesIdExist) {
    EXPECT_FALSE(segmentManager.doesIdExist(0));

    LogSegment* oldHead = segmentManager.allocHeadSegment();
    EXPECT_FALSE(segmentManager.doesIdExist(0));
    EXPECT_TRUE(segmentManager.doesIdExist(1));
    EXPECT_FALSE(segmentManager.doesIdExist(2));

    segmentManager.allocHeadSegment();
    EXPECT_FALSE(segmentManager.doesIdExist(0));
    EXPECT_TRUE(segmentManager.doesIdExist(1));
    EXPECT_TRUE(segmentManager.doesIdExist(2));
    EXPECT_FALSE(segmentManager.doesIdExist(3));

    segmentManager.free(oldHead);
    EXPECT_FALSE(segmentManager.doesIdExist(0));
    EXPECT_FALSE(segmentManager.doesIdExist(1));
    EXPECT_TRUE(segmentManager.doesIdExist(2));
    EXPECT_FALSE(segmentManager.doesIdExist(3));
}

// getFreeSegmentCount, getMaximumSegmentCount, getSegletSize, & getSegmentSize
// aren't paricularly interesting

TEST_F(SegmentManagerTest, writeHeader) {
    LogSegment* s = segmentManager.alloc(SegmentManager::ALLOC_HEAD, 42, 8118);
    EXPECT_EQ(8118U, s->creationTimestamp);
    SegmentIterator sanity(*s);
    EXPECT_TRUE(sanity.isDone());

    *const_cast<uint64_t*>(&s->id) = 5723;
    segmentManager.writeHeader(s);
    SegmentIterator it(*s);
    EXPECT_FALSE(it.isDone());
    Buffer buffer;
    it.appendToBuffer(buffer);
    const SegmentHeader* header = buffer.getStart<SegmentHeader>();
    EXPECT_EQ(*serverId, header->logId);
    EXPECT_EQ(5723U, header->segmentId);
    EXPECT_EQ(serverConfig.segmentSize, header->capacity);
}

TEST_F(SegmentManagerTest, writeDigest_basics) {
    // Implicit check via calls via alloc().
    LogSegment* prevHead = segmentManager.alloc(SegmentManager::ALLOC_HEAD,
                                                0,
                                                0);
    LogSegment* newHead = segmentManager.alloc(SegmentManager::ALLOC_HEAD,
                                               1,
                                               0);

    for (SegmentIterator it(*prevHead); !it.isDone(); it.next()) {
        if (it.getType() != LOG_ENTRY_TYPE_LOGDIGEST)
            continue;

        Buffer buffer;
        it.appendToBuffer(buffer);
        LogDigest digest(buffer.getRange(0, buffer.size()),
                         buffer.size());
        EXPECT_EQ(1U, digest.size());
        EXPECT_EQ(0UL, digest[0]);
    }

    for (SegmentIterator it(*newHead); !it.isDone(); it.next()) {
        if (it.getType() != LOG_ENTRY_TYPE_LOGDIGEST)
            continue;

        Buffer buffer;
        it.appendToBuffer(buffer);
        LogDigest digest(buffer.getRange(0, buffer.size()),
                         buffer.size());
        EXPECT_EQ(2U, digest.size());
        EXPECT_EQ(0UL, digest[0]);
        EXPECT_EQ(1UL, digest[1]);
    }
}

TEST_F(SegmentManagerTest, getHeadSegment) {
    EXPECT_EQ(static_cast<LogSegment*>(NULL), segmentManager.getHeadSegment());
    segmentManager.allocHeadSegment();
    EXPECT_NE(static_cast<LogSegment*>(NULL), segmentManager.getHeadSegment());
    EXPECT_EQ(&segmentManager.segmentsByState[SegmentManager::HEAD].back(),
        segmentManager.getHeadSegment());
}

TEST_F(SegmentManagerTest, changeState) {
    LogSegment* s = segmentManager.allocHeadSegment();
    EXPECT_EQ(SegmentManager::HEAD, segmentManager.states[s->slot]);
    EXPECT_EQ(1U, segmentManager.segmentsByState[SegmentManager::HEAD].size());
    EXPECT_EQ(0U, segmentManager.segmentsByState[
        SegmentManager::CLEANABLE].size());
    segmentManager.changeState(*s, SegmentManager::CLEANABLE);
    EXPECT_EQ(SegmentManager::CLEANABLE, segmentManager.states[s->slot]);
    EXPECT_EQ(0U, segmentManager.segmentsByState[SegmentManager::HEAD].size());
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::CLEANABLE].size());
}

TEST_F(SegmentManagerTest, alloc_noSlots) {
    segmentManager.freeSlots.clear();
    EXPECT_EQ(static_cast<LogSegment*>(NULL),
        segmentManager.alloc(SegmentManager::ALLOC_HEAD, 0, 0));
}

TEST_F(SegmentManagerTest, alloc_noSeglets) {
    vector<Seglet*> seglets;
    allocator.alloc(SegletAllocator::DEFAULT,
                    downCast<uint32_t>(
                        allocator.getFreeCount(SegletAllocator::DEFAULT)),
                    seglets);
    EXPECT_EQ(static_cast<LogSegment*>(NULL),
        segmentManager.alloc(SegmentManager::ALLOC_HEAD, 0, 0));

    foreach (Seglet* s, seglets)
        s->free();
}

TEST_F(SegmentManagerTest, alloc_normal) {
    LogSegment* s = segmentManager.alloc(SegmentManager::ALLOC_HEAD, 79, 94305);
    EXPECT_NE(static_cast<LogSegment*>(NULL), s);
    EXPECT_EQ(79LU, s->id);
    EXPECT_EQ(94305U, s->creationTimestamp);
    EXPECT_FALSE(s->isEmergencyHead);
    EXPECT_EQ(SegmentManager::HEAD, segmentManager.states[s->slot]);
    EXPECT_EQ(s->slot, segmentManager.idToSlotMap[s->id]);
    EXPECT_EQ(1U, segmentManager.allSegments.size());
    EXPECT_EQ(1U, segmentManager.segmentsByState[SegmentManager::HEAD].size());
}

TEST_F(SegmentManagerTest, alloc_emergencyHead) {
    SegmentManager* sm = &segmentManager;
    LogSegment* s = sm->alloc(SegmentManager::ALLOC_EMERGENCY_HEAD, 88, 94305);
    EXPECT_NE(static_cast<LogSegment*>(NULL), s);
    EXPECT_EQ(94305U, s->creationTimestamp);
    EXPECT_TRUE(s->isEmergencyHead);
    EXPECT_EQ(SegmentManager::HEAD, sm->states[s->slot]);
}

TEST_F(SegmentManagerTest, alloc_survivor) {
    SegmentManager* sm = &segmentManager;
    segmentManager.initializeSurvivorReserve(1);
    LogSegment* s = sm->alloc(SegmentManager::ALLOC_CLEANER_SIDELOG, 88, 94305);
    EXPECT_NE(static_cast<LogSegment*>(NULL), s);
    EXPECT_EQ(94305U, s->creationTimestamp);
    EXPECT_FALSE(s->isEmergencyHead);
    EXPECT_EQ(SegmentManager::SIDELOG, sm->states[s->slot]);
}

TEST_F(SegmentManagerTest, allocSlot) {
    SegmentManager* sm = &segmentManager;

    // default pool
    sm->freeSlots.clear();
    EXPECT_EQ(-1U, sm->allocSlot(SegmentManager::ALLOC_HEAD));
    sm->freeSlots.push_back(57);
    EXPECT_EQ(57U, sm->allocSlot(SegmentManager::ALLOC_HEAD));
    EXPECT_EQ(-1U, sm->allocSlot(SegmentManager::ALLOC_HEAD));

    // default pool again
    sm->freeSlots.clear();
    EXPECT_EQ(-1U, sm->allocSlot(SegmentManager::ALLOC_REGULAR_SIDELOG));
    sm->freeSlots.push_back(57);
    EXPECT_EQ(57U, sm->allocSlot(SegmentManager::ALLOC_REGULAR_SIDELOG));
    EXPECT_EQ(-1U, sm->allocSlot(SegmentManager::ALLOC_REGULAR_SIDELOG));

    // survivor pool
    EXPECT_EQ(-1U, sm->allocSlot(SegmentManager::ALLOC_CLEANER_SIDELOG));
    sm->freeSurvivorSlots.push_back(86);
    EXPECT_EQ(86U, sm->allocSlot(SegmentManager::ALLOC_CLEANER_SIDELOG));
    EXPECT_EQ(-1U, sm->allocSlot(SegmentManager::ALLOC_CLEANER_SIDELOG));

    // emergency head pool
    sm->freeEmergencyHeadSlots.clear();
    EXPECT_EQ(-1U, sm->allocSlot(SegmentManager::ALLOC_EMERGENCY_HEAD));
    sm->freeEmergencyHeadSlots.push_back(13);
    EXPECT_EQ(13U, sm->allocSlot(SegmentManager::ALLOC_EMERGENCY_HEAD));
    EXPECT_EQ(-1U, sm->allocSlot(SegmentManager::ALLOC_EMERGENCY_HEAD));
}

TEST_F(SegmentManagerTest, freeSlot) {
    // default pool
    segmentManager.freeSlots.clear();
    EXPECT_EQ(0U, segmentManager.freeSlots.size());
    segmentManager.freeSlot(52, false);
    EXPECT_EQ(1U, segmentManager.freeSlots.size());
    EXPECT_EQ(52U, segmentManager.freeSlots[0]);

    // survivor pool
    EXPECT_TRUE(segmentManager.initializeSurvivorReserve(1));
    segmentManager.freeSurvivorSlots.clear();
    EXPECT_EQ(0U, segmentManager.freeSurvivorSlots.size());
    segmentManager.freeSlot(98, false);
    EXPECT_EQ(1U, segmentManager.freeSurvivorSlots.size());
    EXPECT_EQ(98U, segmentManager.freeSurvivorSlots[0]);

    // emergency head pool
    segmentManager.freeEmergencyHeadSlots.clear();
    EXPECT_EQ(0U, segmentManager.freeEmergencyHeadSlots.size());
    segmentManager.freeSlot(62, true);
    EXPECT_EQ(1U, segmentManager.freeEmergencyHeadSlots.size());
    EXPECT_EQ(62U, segmentManager.freeEmergencyHeadSlots[0]);
}

TEST_F(SegmentManagerTest, free) {
    LogSegment* s = segmentManager.allocHeadSegment();

    EXPECT_EQ(317U, segmentManager.freeSlots.size());
    EXPECT_TRUE(contains(segmentManager.idToSlotMap, s->id));
    EXPECT_EQ(1U, segmentManager.segmentsByState[SegmentManager::HEAD].size());
    EXPECT_EQ(1U, segmentManager.allSegments.size());
    EXPECT_NE(SegmentManager::INVALID_STATE, segmentManager.states[s->slot]);
    EXPECT_TRUE(segmentManager.segments[s->slot]);

    // NULL out the ReplicatedSegment since RS::free() may not be called on
    // an open segment. ReplicaManager tracks all RSes, so dropping our
    // pointer is perfectly safe.
    s->replicatedSegment = NULL;

    uint64_t id = s->id;
    SegmentSlot slot = s->slot;

    segmentManager.free(s);
    EXPECT_EQ(318U, segmentManager.freeSlots.size());
    EXPECT_FALSE(contains(segmentManager.idToSlotMap, id));
    EXPECT_EQ(0U, segmentManager.segmentsByState[SegmentManager::HEAD].size());
    EXPECT_EQ(0U, segmentManager.allSegments.size());
    EXPECT_EQ(SegmentManager::INVALID_STATE, segmentManager.states[slot]);
    EXPECT_FALSE(segmentManager.segments[slot]);
}

TEST_F(SegmentManagerTest, addToLists) {
    EXPECT_EQ(0U, segmentManager.segmentsByState[SegmentManager::HEAD].size());
    EXPECT_EQ(0U, segmentManager.allSegments.size());
    LogSegment*s = segmentManager.allocHeadSegment();
    // allocHead implicitly calls addToLists...
    EXPECT_EQ(1U, segmentManager.segmentsByState[SegmentManager::HEAD].size());
    EXPECT_EQ(s, &segmentManager.segmentsByState[SegmentManager::HEAD].back());
    EXPECT_EQ(1U, segmentManager.allSegments.size());
    EXPECT_EQ(s, &segmentManager.allSegments.back());
}

TEST_F(SegmentManagerTest, removeFromLists) {
    LogSegment* s = segmentManager.allocHeadSegment();
    EXPECT_EQ(1U, segmentManager.segmentsByState[SegmentManager::HEAD].size());
    EXPECT_EQ(1U, segmentManager.allSegments.size());
    segmentManager.removeFromLists(*s);
    EXPECT_EQ(0U, segmentManager.segmentsByState[SegmentManager::HEAD].size());
    EXPECT_EQ(0U, segmentManager.allSegments.size());

    segmentManager.addToLists(*s);
}

// Need a do-nothing subclass of the abstract parent type.
class TestServerRpc : public Transport::ServerRpc {
    void sendReply() {}
    string getClientServiceLocator() { return ""; }
};

TEST_F(SegmentManagerTest, freeUnreferencedSegments) {
    LogSegment* freeable = segmentManager.allocHeadSegment();
    segmentManager.allocHeadSegment();

    ServerRpcPool<TestServerRpc> pool;
    TestServerRpc* rpc = pool.construct();
    rpc->epoch = 8;

    segmentManager.changeState(*freeable,
        SegmentManager::FREEABLE_PENDING_REFERENCES);

    freeable->cleanedEpoch = 8;
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());

    freeable->cleanedEpoch = 9;
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());

    freeable->cleanedEpoch = 7;
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(0U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());

    pool.destroy(rpc);
}

// Helper function that invokes Dispatch::poll in a separate thread.
static void testPoll(Dispatch* dispatch) {
    dispatch->poll();
}

TEST_F(SegmentManagerTest, freeUnreferencedSegments_blockByWorkerTimer) {
    LogSegment* freeable = segmentManager.allocHeadSegment();
    segmentManager.allocHeadSegment();

    ServerRpcPool<TestServerRpc> pool;
    TestServerRpc* rpc = pool.construct();
    rpc->epoch = 8;

    segmentManager.changeState(*freeable,
        SegmentManager::FREEABLE_PENDING_REFERENCES);

    Tub<SegmentManagerTest::DummyWorkerTimer> timer;
    Dispatch dispatch(false);
    timer.construct(&dispatch);
    for (int i = 1; i < 1000; i++) {
        if (timer->handlerRunning) {
            break;
        }
        usleep(100);
    }

    timer->start(1000);
    timer->sleepMicroseconds = 10000;
    Cycles::mockTscValue = 2000;
    std::thread thread(testPoll, &dispatch);

    // Wait for the handler to start executing (see "Timing-Dependent Tests"
    // in designNotes).
    for (int i = 1; i < 1000; i++) {
        if (timer->handlerRunning) {
            break;
        }
        usleep(1000);
    }
    EXPECT_TRUE(timer->handlerRunning);
    timer->manager->epoch = 7;
    EXPECT_EQ(7U, WorkerTimer::getEarliestOutstandingEpoch());

    // Ongoing WorkerTimer handler prevents cleaning.
    freeable->cleanedEpoch = 7;
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());

    // WorkerTimer handler is finished.
    timer.destroy();
    EXPECT_EQ(~0UL, WorkerTimer::getEarliestOutstandingEpoch());
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(0U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());

    pool.destroy(rpc);
    thread.join();
    Cycles::mockTscValue = 0;              // We need to measure real time!
}

TEST_F(SegmentManagerTest, freeUnreferencedSegments_logWhenStuck) {
    TestLog::Enable _("freeUnreferencedSegments");
    LogSegment* freeable = segmentManager.allocHeadSegment();
    segmentManager.allocHeadSegment();

    ServerRpcPoolInternal::currentEpoch = 8;
    ServerRpcPool<TestServerRpc> pool;
    TestServerRpc* rpc = pool.construct();
    rpc->epoch = 8;

    segmentManager.changeState(*freeable,
        SegmentManager::FREEABLE_PENDING_REFERENCES);

    freeable->cleanedEpoch = 8;
    Cycles::mockTscValue = 1000;
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());
    EXPECT_EQ("", TestLog::get());
    EXPECT_EQ(1.0, segmentManager.nextMessageSeconds);

    Cycles::mockTscValue += Cycles::fromSeconds(0.9);
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());
    EXPECT_EQ("", TestLog::get());

    Cycles::mockTscValue += Cycles::fromSeconds(0.11);
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());
    EXPECT_EQ("freeUnreferencedSegments: Unfinished RPCs are preventing 1 "
            "segments from being freed (segment epoch 8, RPC epoch 8, "
            "current epoch 8, stuck for 1 seconds)", TestLog::get());
    TestLog::reset();

    Cycles::mockTscValue += Cycles::fromSeconds(0.5);
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());
    EXPECT_EQ("", TestLog::get());

    Cycles::mockTscValue += Cycles::fromSeconds(0.55);
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(1U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());
    EXPECT_EQ("freeUnreferencedSegments: Unfinished RPCs are preventing 1 "
            "segments from being freed (segment epoch 8, RPC epoch 8, "
            "current epoch 8, stuck for 2 seconds)", TestLog::get());

    Cycles::mockTscValue += Cycles::fromSeconds(2.0);
    freeable->cleanedEpoch = 7;
    segmentManager.freeUnreferencedSegments();
    EXPECT_EQ(0U, segmentManager.segmentsByState[
        SegmentManager::FREEABLE_PENDING_REFERENCES].size());
    EXPECT_EQ(0.0, segmentManager.nextMessageSeconds);

    Cycles::mockTscValue = 0;
    pool.destroy(rpc);
}

} // namespace RAMCloud
