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
#include "BackupStorage.h"
#include "Buffer.h"
#include "CoordinatorClient.h"
#include "EnumerationIterator.h"
#include "LogIterator.h"
#include "MockCluster.h"
#include "Memory.h"
#include "MasterClient.h"
#include "MasterService.h"
#include "MultiRead.h"
#include "MultiWrite.h"
#include "RamCloud.h"
#include "ReplicaManager.h"
#include "SegmentManager.h"
#include "ShortMacros.h"
#include "StringUtil.h"
#include "Tablets.pb.h"

namespace RAMCloud {

class ObjectManagerTest : public ::testing::Test {
  public:
    Context context;
    ServerList serverList;
    ServerConfig masterConfig;
    TabletManager tabletManager;
    ObjectManager objectManager;

    ObjectManagerTest()
        : context()
        , serverList(&context)
        , masterConfig(ServerConfig::forTesting())
        , tabletManager()
        , objectManager(&context, ServerId(5), &masterConfig, &tabletManager)
    {
        tabletManager.addTablet(0, 0, ~0UL, TabletManager::NORMAL);
    }

    /**
     * Build a properly formatted segment containing a single object. This
     * segment may be passed directly to the ObjectManager::replaySegment()
     * routine.
     */
    uint32_t
    buildRecoverySegment(char *segmentBuf, uint32_t segmentCapacity,
                         Key& key, uint64_t version, string objContents,
                         Segment::Certificate* outCertificate)
    {
        Segment s;
        uint32_t dataLength = downCast<uint32_t>(objContents.length()) + 1;
        Object newObject(key, objContents.c_str(), dataLength, version, 0);

        Buffer newObjectBuffer;
        newObject.serializeToBuffer(newObjectBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_OBJ, newObjectBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.getTotalLength());
        buffer.copy(0, buffer.getTotalLength(), segmentBuf);
        s.getAppendedLength(outCertificate);

        return buffer.getTotalLength();
    }

    /**
     * Build a properly formatted segment containing a single tombstone. This
     * segment may be passed directly to the ObjectManager::replaySegment()
     * routine.
     */
    uint32_t
    buildRecoverySegment(char *segmentBuf, uint64_t segmentCapacity,
                         ObjectTombstone& tomb,
                         Segment::Certificate* outCertificate)
    {
        Segment s;
        Buffer newTombstoneBuffer;
        tomb.serializeToBuffer(newTombstoneBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_OBJTOMB, newTombstoneBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.getTotalLength());
        buffer.copy(0, buffer.getTotalLength(), segmentBuf);
        s.getAppendedLength(outCertificate);

        return buffer.getTotalLength();
    }

    /**
     * Build a properly formatted segment containing a single safeVersion.
     * This segment may be passed directly to the ObjectManager::replaySegment()
     * routine.
     */
    uint32_t
    buildRecoverySegment(char *segmentBuf, uint64_t segmentCapacity,
                         ObjectSafeVersion &safeVer,
                         Segment::Certificate* outCertificate)
    {
        Segment s;
        Buffer newSafeVerBuffer;
        safeVer.serializeToBuffer(newSafeVerBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_SAFEVERSION,
                                newSafeVerBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.getTotalLength());
        buffer.copy(0, buffer.getTotalLength(), segmentBuf);
        s.getAppendedLength(outCertificate);

        return buffer.getTotalLength();
    }

    /**
     * Store an object in the log and hash table, returning its Log::Reference.
     */
    Log::Reference
    storeObject(Key& key, string value)
    {
        Object o(key, value.c_str(), downCast<uint32_t>(value.size()), 0, 0);
        Buffer buffer;
        o.serializeToBuffer(buffer);
        Log::Reference reference;
        objectManager.log.append(LOG_ENTRY_TYPE_OBJ, 0, buffer, &reference);
        {
            ObjectManager::HashTableBucketLock lock(objectManager, key);
            objectManager.replace(lock, key, reference);
        }
        return reference;
    }

    /**
     * Store a tombstone in the log and hash table, return its Log::Reference.
     */
    Log::Reference
    storeTombstone(Key& key)
    {
        Object o(key, NULL, 0, 1, 0);
        ObjectTombstone t(o, 0, 0);
        Buffer buffer;
        t.serializeToBuffer(buffer);
        Log::Reference reference;
        objectManager.log.append(LOG_ENTRY_TYPE_OBJTOMB, 0, buffer, &reference);
        {
            ObjectManager::HashTableBucketLock lock(objectManager, key);
            objectManager.replace(lock, key, reference);
        }
        return reference;
    }

    /**
     * Verify an object replayed during recovery by looking it up in the hash
     * table by key and comparing contents.
     */
    void
    verifyRecoveryObject(Key& key, string contents)
    {
        Buffer value;
        EXPECT_EQ(STATUS_OK, objectManager.readObject(key, &value, NULL, NULL));
        const char *s = reinterpret_cast<const char *>(
            value.getRange(0, value.getTotalLength()));
        EXPECT_EQ(0, strcmp(s, contents.c_str()));
    }

    /**
     * Verify that the safe version number was correctly recovered.
     * TODO(Satoshi): Documentation.
     */
    int
    verifyCopiedSafeVer(const ObjectSafeVersion *safeVerSrc)
    {
        bool safeVerFound = false;
        int  safeVerScanned = 0;

        for (LogIterator it(objectManager.log); !it.isDone(); it.next()) {
            // Notice that more than two safeVersion objects exist
            // in the head segment:
            // 1st safeVersion is allocated when the segment is opened.
            // 2nd or lator is the one copied by the recovery.
            if (it.getType() == LOG_ENTRY_TYPE_SAFEVERSION) {
                safeVerScanned++;
                Buffer buf;
                it.setBufferTo(buf);
                ObjectSafeVersion safeVerDest(buf);
                if (safeVerSrc->serializedForm.safeVersion
                    == safeVerDest.serializedForm.safeVersion) {
                    safeVerFound = true;
                }
            }
        }
        EXPECT_TRUE(safeVerFound);
        return safeVerScanned;
    }

    /**
     * This is simply a convenience wrapper around ObjectManager::lookup that
     * only extracts the reference (not the value, type, or version).
     */
    bool
    lookup(Key& key, Log::Reference* outReference)
    {
        Buffer unusedBuffer;
        LogEntryType unusedType;
        ObjectManager::HashTableBucketLock fakeLock(objectManager, 0);
        return objectManager.lookup(
            fakeLock, key, unusedType, unusedBuffer, NULL, outReference);
    }

    /**
     * This is simply a convenience wrapper for the ReplaySegment test. Given
     * the individual components of an object's key (tableId, string, length),
     * it returns the status of ObjectManager::readObject.
     */
    Status
    getObjectStatus(uint64_t tableId, const void* stringKey, uint16_t keyLength)
    {
        Buffer unusedBuffer;
        Key key(tableId, stringKey, keyLength);
        return objectManager.readObject(key, &unusedBuffer, 0, 0);
    }

    static bool
    replaySegmentFilter(string s)
    {
        return (s == "replaySegment" || s == "recover" ||
                s == "recoveryMasterFinished");
    }

    DISALLOW_COPY_AND_ASSIGN(ObjectManagerTest);
};

TEST_F(ObjectManagerTest, constructor) {
    // After switching from passing ServerId& to ServerId (we used to not know
    // the ServerId prior to instantiation), a bug was introduced where OM took
    // a ServerId, but passed the stack temporary by reference to replicaManager
    // and SegmentManager. Ensure this doesn't happen anymore.
    EXPECT_EQ(ServerId(5), objectManager.segmentManager.logId);
    EXPECT_EQ(ServerId(5), objectManager.replicaManager.masterId);
}

static bool
writeObjectFilter(string s)
{
    return s == "writeObject";
}

TEST_F(ObjectManagerTest, writeObject) {
    Key key(1, "1", 1);
    Buffer buffer;
    buffer.append("value", 5);

    // no tablet, no dice.
    EXPECT_EQ(STATUS_UNKNOWN_TABLET,
        objectManager.writeObject(key, buffer, 0, 0));

    // non-NORMAL tablet state, no dice.
    tabletManager.addTablet(1, 0, ~0UL, TabletManager::RECOVERING);
    EXPECT_EQ(STATUS_UNKNOWN_TABLET,
        objectManager.writeObject(key, buffer, 0, 0));

    TestLog::Enable _(writeObjectFilter);

    // new object (no tombstone needed)
    tabletManager.changeState(1, 0, ~0UL, TabletManager::RECOVERING,
                                          TabletManager::NORMAL);
    EXPECT_EQ(STATUS_OK, objectManager.writeObject(key, buffer, 0, 0));
    EXPECT_EQ("writeObject: object: 32 bytes, version 1", TestLog::get());

    // object overwrite (tombstone needed)
    TestLog::reset();
    EXPECT_EQ(STATUS_OK, objectManager.writeObject(key, buffer, 0, 0));
    EXPECT_EQ("writeObject: object: 32 bytes, version 2 | "
              "writeObject: tombstone: 35 bytes, version 1", TestLog::get());
}

TEST_F(ObjectManagerTest, readObject) {
    // XXX
}

TEST_F(ObjectManagerTest, removeObject) {
    // XXX
}

TEST_F(ObjectManagerTest, removeOrphanedObjects) {
    tabletManager.addTablet(97, 0, ~0UL, TabletManager::NORMAL);
    Key key(97, "1", 1);

    Buffer value;
    value.append("hi", 2);
    EXPECT_EQ(STATUS_OK, objectManager.writeObject(key, value, NULL, NULL));

    EXPECT_TRUE(tabletManager.getTablet(key, NULL));
    EXPECT_EQ(STATUS_OK, objectManager.readObject(key, &value, NULL, NULL));

    objectManager.removeOrphanedObjects();
    EXPECT_EQ(STATUS_OK, objectManager.readObject(key, &value, NULL, NULL));

    tabletManager.deleteTablet(97, 0, ~0UL);
    EXPECT_FALSE(tabletManager.getTablet(key, NULL));

    TestLog::Enable _;
    objectManager.removeOrphanedObjects();
    EXPECT_EQ(STATUS_UNKNOWN_TABLET,
        objectManager.readObject(key, &value, NULL, NULL));

    tabletManager.addTablet(97, 0, ~0UL, TabletManager::NORMAL);
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST,
        objectManager.readObject(key, &value, NULL, NULL));

    EXPECT_EQ(
        "removeIfOrphanedObject: removing orphaned object at ref 31457334 | "
        "free: free on reference 31457334",
        TestLog::get());
}

TEST_F(ObjectManagerTest, replaySegment) {
    uint32_t segLen = 8192;
    char seg[segLen];
    uint32_t len; // number of bytes in a recovery segment
    Buffer buffer;
    Log::Reference reference;
    Log::Reference logTomb1Ref;
    Log::Reference logTomb2Ref;
    LogEntryType type;
    bool ret;
    SideLog sl(&objectManager.log);

    ////////////////////////////////////////////////////////////////////
    // For Object recovery there are 3 major cases.
    //  1) Object is in the HashTable, but no corresponding
    //     ObjectTombstone.
    //     The recovered obj is only added if the version is newer than
    //     the existing obj.
    //
    //  2) Opposite of 1 above.
    //     The recovered obj is only added if the version is newer than
    //     the tombstone. If so, the tombstone is also discarded.
    //
    //  3) Neither an Object nor ObjectTombstone is present.
    //     The recovered obj is always added.
    ////////////////////////////////////////////////////////////////////

    // Case 1a: Newer object already there; ignore object.
    Key key0(0, "key0", 4);
    Segment::Certificate certificate;
    len = buildRecoverySegment(seg, segLen, key0, 1, "newer guy", &certificate);
    Tub<SegmentIterator> it;
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key0, "newer guy");
    len = buildRecoverySegment(seg, segLen, key0, 0, "older guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key0, "newer guy");

    // Case 1b: Older object already there; replace object.
    Key key1(0, "key1", 4);
    len = buildRecoverySegment(seg, segLen, key1, 0, "older guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key1, "older guy");
    len = buildRecoverySegment(seg, segLen, key1, 1, "newer guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key1, "newer guy");

    // Case 2a: Equal/newer tombstone already there; ignore object.
    Key key2(0, "key2", 4);
    Object o1(key2, NULL, 0, 1, 0);
    ObjectTombstone t1(o1, 0, 0);
    buffer.reset();
    t1.serializeToBuffer(buffer);
    objectManager.log.append(LOG_ENTRY_TYPE_OBJTOMB, 0, buffer, &logTomb1Ref);
    objectManager.log.sync();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key2);
        ret = objectManager.replace(lock, key2, logTomb1Ref);
    }
    EXPECT_FALSE(ret);
    len = buildRecoverySegment(seg, segLen, key2, 1, "equal guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    len = buildRecoverySegment(seg, segLen, key2, 0, "older guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    EXPECT_TRUE(lookup(key2, &reference));
    EXPECT_EQ(reference, logTomb1Ref);
    objectManager.removeTombstones();
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, getObjectStatus(0, "key2", 4));

    // Case 2b: Lesser tombstone already there; add object, remove tomb.
    Key key3(0, "key3", 4);
    Object o2(key3, NULL, 0, 10, 0);
    ObjectTombstone t2(o2, 0, 0);
    buffer.reset();
    t2.serializeToBuffer(buffer);
    ret = objectManager.log.append(
        LOG_ENTRY_TYPE_OBJTOMB, 0, buffer, &logTomb2Ref);
    objectManager.log.sync();
    EXPECT_TRUE(ret);
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key3);
        ret = objectManager.replace(lock, key3, logTomb2Ref);
    }
    EXPECT_FALSE(ret);
    len = buildRecoverySegment(seg, segLen, key3, 11, "newer guy",
                               &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key3, "newer guy");
    EXPECT_TRUE(lookup(key3, &reference));
    EXPECT_NE(reference, logTomb1Ref);
    EXPECT_NE(reference, logTomb2Ref);
    objectManager.removeTombstones();

    // Case 3: No tombstone, no object. Recovered object always added.
    Key key4(0 , "key4", 4);
    EXPECT_FALSE(lookup(key4, &reference));
    len = buildRecoverySegment(seg, segLen, key4, 0, "only guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key4, "only guy");

    ////////////////////////////////////////////////////////////////////
    // For ObjectTombstone recovery there are the same 3 major cases:
    //  1) Object is in  the HashTable, but no corresponding
    //     ObjectTombstone.
    //     The recovered tomb is only added if the version is equal to
    //     or greater than the object. If so, the object is purged.
    //
    //  2) Opposite of 1 above.
    //     The recovered tomb is only added if the version is newer than
    //     the current tombstone. If so, the old tombstone is discarded.
    //
    //  3) Neither an Object nor ObjectTombstone is present.
    //     The recovered tombstone is always added.
    ////////////////////////////////////////////////////////////////////

    // Case 1a: Newer object already there; ignore tombstone.
    Key key5(0, "key5", 4);
    len = buildRecoverySegment(seg, segLen, key5, 1, "newer guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    Object o3(key5, NULL, 0, 0, 0);
    ObjectTombstone t3(o3, 0, 0);
    len = buildRecoverySegment(seg, segLen, t3, &certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key5, "newer guy");

    // Case 1b: Equal/older object already there; discard and add tombstone.
    Key key6(0, "key6", 4);
    len = buildRecoverySegment(seg, segLen, key6, 0, "equal guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key6, "equal guy");
    Object o4(key6, NULL, 0, 0, 0);
    ObjectTombstone t4(o4, 0, 0);
    len = buildRecoverySegment(seg, segLen, t4, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    objectManager.removeTombstones();
    EXPECT_FALSE(lookup(key6, &reference));
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, getObjectStatus(0, "key6", 4));

    Key key7(0, "key7", 4);
    len = buildRecoverySegment(seg, segLen, key7, 0, "older guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key7, "older guy");
    Object o5(key7, NULL, 0, 1, 0);
    ObjectTombstone t5(o5, 0, 0);
    len = buildRecoverySegment(seg, segLen, t5, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    objectManager.removeTombstones();
    EXPECT_FALSE(lookup(key7, &reference));
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, getObjectStatus(0, "key7", 4));

    // Case 2a: Newer tombstone already there; ignore.
    Key key8(0, "key8", 4);
    Object o6(key8, NULL, 0, 1, 0);
    ObjectTombstone t6(o6, 0, 0);
    len = buildRecoverySegment(seg, segLen, t6, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    buffer.reset();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key8);
        ret = objectManager.lookup(lock, key8, type, buffer);
    }
    EXPECT_TRUE(ret);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, type);
    ObjectTombstone t6InLog(buffer);
    EXPECT_EQ(1U, t6InLog.getObjectVersion());
    ObjectTombstone t7(o6, 0, 0);
    t7.serializedForm.objectVersion = 0;
    t7.serializedForm.checksum = t7.computeChecksum();
    len = buildRecoverySegment(seg, segLen, t7, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    const uint8_t* t6LogPtr = buffer.getStart<uint8_t>();
    buffer.reset();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key8);
        ret = objectManager.lookup(lock, key8, type, buffer);
    }
    EXPECT_TRUE(ret);
    EXPECT_EQ(t6LogPtr, buffer.getStart<uint8_t>());

    // Case 2b: Older tombstone already there; replace.
    Key key9(0, "key9", 4);
    Object o8(key9, NULL, 0, 0, 0);
    ObjectTombstone t8(o8, 0, 0);
    len = buildRecoverySegment(seg, segLen, t8, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    buffer.reset();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key9);
        ret = objectManager.lookup(lock, key9, type, buffer);
    }
    EXPECT_TRUE(ret);
    ObjectTombstone t8InLog(buffer);
    EXPECT_EQ(0U, t8InLog.getObjectVersion());

    Object o9(key9, NULL, 0, 1, 0);
    ObjectTombstone t9(o9, 0, 0);
    len = buildRecoverySegment(seg, segLen, t9, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    buffer.reset();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key9);
        ret = objectManager.lookup(lock, key9, type, buffer);
    }
    EXPECT_TRUE(ret);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, type);
    ObjectTombstone t9InLog(buffer);
    EXPECT_EQ(1U, t9InLog.getObjectVersion());

    // Case 3: No tombstone, no object. Recovered tombstone always added.
    Key key10(0, "key10", 5);
    EXPECT_FALSE(lookup(key10, &reference));
    Object o10(key10, NULL, 0, 0, 0);
    ObjectTombstone t10(o10, 0, 0);
    len = buildRecoverySegment(seg, segLen, t10, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    buffer.reset();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key10);
        EXPECT_TRUE(objectManager.lookup(lock, key10, type, buffer));
    }
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, type);
    Buffer t10Buffer;
    t10.serializeToBuffer(t10Buffer);
    EXPECT_EQ(0, memcmp(t10Buffer.getRange(0, t10Buffer.getTotalLength()),
                        buffer.getRange(0, buffer.getTotalLength()),
                        buffer.getTotalLength()));
    ////////////////////////////////////////////////////////////////////
    //
    //  For safeVersion recovery from OBJECT_SAFEVERSION entry
    //
    ////////////////////////////////////////////////////////////////////
    objectManager.segmentManager.safeVersion = 1UL; // reset safeVersion to 1

    ObjectSafeVersion safeVer(10UL);
    len = buildRecoverySegment(seg, segLen, safeVer, &certificate);
    it.construct(&seg[0], len, certificate);
    EXPECT_EQ(14U, len); // 14 = EntryHeader(1B) + ? (1B)
    //                    + safeVersion (8B) + checksum (4B)
    TestLog::Enable _(replaySegmentFilter);
    objectManager.replaySegment(&sl, *it);
    EXPECT_TRUE(TestUtil::matchesPosixRegex(
        "SAFEVERSION 10 recovered",
        TestLog::get()));
    TestLog::reset();
    // The SideLog must be committed before we can iterate the log to look
    // for safe version objects.
    sl.commit();
    // three safeVer should be found (SideLog commit() opened a new head).
    EXPECT_EQ(3, verifyCopiedSafeVer(&safeVer));

    // recovered from safeVer
    EXPECT_EQ(10UL, objectManager.segmentManager.safeVersion);
}

TEST_F(ObjectManagerTest, rejectOperation) {
    RejectRules empty, rules;
    memset(&empty, 0, sizeof(empty));

    // Fail: object doesn't exist.
    rules = empty;
    rules.doesntExist = 1;
    EXPECT_EQ(objectManager.rejectOperation(&rules, VERSION_NONEXISTENT),
              STATUS_OBJECT_DOESNT_EXIST);

    // Succeed: object doesn't exist.
    rules = empty;
    rules.exists = rules.versionLeGiven = rules.versionNeGiven = 1;
    EXPECT_EQ(objectManager.rejectOperation(&rules, VERSION_NONEXISTENT),
              STATUS_OK);

    // Fail: object exists.
    rules = empty;
    rules.exists = 1;
    EXPECT_EQ(objectManager.rejectOperation(&rules, 2),
              STATUS_OBJECT_EXISTS);

    // versionLeGiven.
    rules = empty;
    rules.givenVersion = 0x400000001;
    rules.versionLeGiven = 1;
    EXPECT_EQ(objectManager.rejectOperation(&rules, 0x400000000),
              STATUS_WRONG_VERSION);
    EXPECT_EQ(objectManager.rejectOperation(&rules, 0x400000001),
              STATUS_WRONG_VERSION);
    EXPECT_EQ(objectManager.rejectOperation(&rules, 0x400000002),
              STATUS_OK);

    // versionNeGiven.
    rules = empty;
    rules.givenVersion = 0x400000001;
    rules.versionNeGiven = 1;
    EXPECT_EQ(objectManager.rejectOperation(&rules, 0x400000000),
              STATUS_WRONG_VERSION);
    EXPECT_EQ(objectManager.rejectOperation(&rules, 0x400000001),
              STATUS_OK);
    EXPECT_EQ(objectManager.rejectOperation(&rules, 0x400000002),
              STATUS_WRONG_VERSION);
}

TEST_F(ObjectManagerTest, getTable) {
    // Table exists.
    Key key1(0, "0", 1);
    EXPECT_TRUE(tabletManager.getTablet(key1, 0));

    // Table doesn't exist.
    Key key2(1000, "0", 1);
    EXPECT_FALSE(tabletManager.getTablet(key2, 0));
}

TEST_F(ObjectManagerTest, objectRelocationCallback_objectAlive) {
    Key key(0, "key0", 4);

    Buffer value;
    value.append("item0", 5);
    objectManager.writeObject(key, value, NULL, NULL);

    LogEntryType oldType;
    Buffer oldBuffer;
    bool success = false;
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        success = objectManager.lookup(lock, key, oldType, oldBuffer, 0, 0);
    }
    EXPECT_TRUE(success);

    Log::Reference newReference;
    success = objectManager.log.append(LOG_ENTRY_TYPE_OBJ,
                                       0,
                                       oldBuffer,
                                       &newReference);
    objectManager.log.sync();
    EXPECT_TRUE(success);

    LogEntryType newType;
    Buffer newBuffer;
    newType = objectManager.log.getEntry(newReference, newBuffer);

    LogEntryType oldType2;
    Buffer oldBuffer2;
    Log::Reference oldReference;
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        success = objectManager.lookup(lock, key, oldType2, oldBuffer2, 0,
            &oldReference);
    }
    EXPECT_TRUE(success);
    EXPECT_EQ(oldType, oldType2);
    EXPECT_EQ(oldBuffer.getStart<uint8_t>(), oldBuffer2.getStart<uint8_t>());

    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    objectManager.relocate(LOG_ENTRY_TYPE_OBJ, oldBuffer, relocator);
    EXPECT_TRUE(relocator.didAppend);

    LogEntryType newType2;
    Buffer newBuffer2;
    Log::Reference newReference2;
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        objectManager.lookup(lock, key, newType2, newBuffer2, 0,
            &newReference2);
    }
    EXPECT_TRUE(relocator.didAppend);
    EXPECT_EQ(newType, newType2);
    EXPECT_EQ(newReference.toInteger() + 37, newReference2.toInteger());
    EXPECT_NE(oldReference, newReference);
    EXPECT_NE(newBuffer.getStart<uint8_t>(),
              oldBuffer.getStart<uint8_t>());
    EXPECT_EQ(newBuffer.getStart<uint8_t>() + 37,
              newBuffer2.getStart<uint8_t>());
}

TEST_F(ObjectManagerTest, objectRelocationCallback_objectDeleted) {
    Key key(0, "key0", 4);

    Buffer value;
    value.append("item0", 5);
    objectManager.writeObject(key, value, NULL, NULL);

    LogEntryType type;
    Buffer buffer;
    bool success = false;
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        success = objectManager.lookup(lock, key, type, buffer);
    }
    EXPECT_TRUE(success);

    objectManager.removeObject(key, NULL, NULL);

    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    objectManager.relocate(LOG_ENTRY_TYPE_OBJ, buffer, relocator);
    EXPECT_FALSE(relocator.didAppend);
}

TEST_F(ObjectManagerTest, objectRelocationCallback_objectModified) {
    Key key(0, "key0", 4);

    Buffer value;
    value.append("item0", 5);
    objectManager.writeObject(key, value, NULL, NULL);

    LogEntryType type;
    Buffer buffer;
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        objectManager.lookup(lock, key, type, buffer);
    }

    value.reset();
    value.append("item0-v2", 8);
    objectManager.writeObject(key, value, NULL, NULL);

    Log::Reference dummyReference;

    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    objectManager.relocate(LOG_ENTRY_TYPE_OBJ, buffer, relocator);
    EXPECT_FALSE(relocator.didAppend);
}

static bool
segmentExists(string s)
{
    return s == "segmentExists";
}

TEST_F(ObjectManagerTest, tombstoneRelocationCallback_basics) {
    TestLog::Enable _(&segmentExists);
    Key key(0, "key0", 4);

    Buffer value;
    value.append("item0", 5);
    objectManager.writeObject(key, value, NULL, NULL);

    LogEntryType type;
    Buffer buffer;
    Log::Reference reference;
    bool success = false;
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        success = objectManager.lookup(lock, key, type, buffer, 0, &reference);
    }
    EXPECT_TRUE(success);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, type);

    Object object(buffer);
    ObjectTombstone tombstone(object,
                              objectManager.log.getSegmentId(reference),
                              0);

    Buffer tombstoneBuffer;
    tombstone.serializeToBuffer(tombstoneBuffer);

    Log::Reference oldTombstoneReference;
    success = objectManager.log.append(
        LOG_ENTRY_TYPE_OBJTOMB, 0, tombstoneBuffer, &oldTombstoneReference);
    objectManager.log.sync();
    EXPECT_TRUE(success);

    Log::Reference newTombstoneReference;
    success = objectManager.log.append(LOG_ENTRY_TYPE_OBJTOMB,
        0, tombstoneBuffer, &newTombstoneReference);
    objectManager.log.sync();
    EXPECT_TRUE(success);

    LogEntryType oldTypeInLog;
    Buffer oldBufferInLog;
    oldTypeInLog = objectManager.log.getEntry(oldTombstoneReference,
                                          oldBufferInLog);

    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    objectManager.relocate(LOG_ENTRY_TYPE_OBJTOMB, oldBufferInLog, relocator);
    EXPECT_TRUE(relocator.didAppend);

    // Check that tombstoneRelocationCallback() is checking the liveness
    // of the right segment (in log.segmentExists() function call).
    string comparisonString = "segmentExists: " +
        format("%lu", objectManager.log.getSegmentId(oldTombstoneReference));
    EXPECT_EQ(comparisonString, TestLog::get());
}

TEST_F(ObjectManagerTest, lookup) {
    // XXX
}

TEST_F(ObjectManagerTest, remove) {
    // XXX
}

TEST_F(ObjectManagerTest, replace) {
    // XXX
}

static bool
removeIfTombstoneFilter(string s)
{
    return s == "removeIfTombstone";
}

TEST_F(ObjectManagerTest, removeIfTombstone_nonTombstone) {
    // 1. calling on a non-tombstone does nothing
    TestLog::Enable _(removeIfTombstoneFilter);
    Key key(1, "key!", 4);
    Log::Reference reference = storeObject(key, "value!");
    tabletManager.addTablet(1, 0, ~0UL, TabletManager::RECOVERING);
    ObjectManager::CleanupParameters params = { &objectManager, 0 };
    objectManager.removeIfTombstone(reference.toInteger(), &params);
    EXPECT_EQ("", TestLog::get());
}

TEST_F(ObjectManagerTest, removeIfTombstone_recoveringTablet) {
    // 2. calling on a tombstone with a RECOVERING tablet does nothing
    TestLog::Enable _(removeIfTombstoneFilter);
    Key key(1, "key!", 4);
    Log::Reference reference = storeTombstone(key);
    tabletManager.addTablet(1, 0, ~0UL, TabletManager::RECOVERING);
    ObjectManager::CleanupParameters params = { &objectManager, 0 };
    objectManager.removeIfTombstone(reference.toInteger(), &params);
    EXPECT_EQ("", TestLog::get());
}

TEST_F(ObjectManagerTest, removeIfTombstone_nonRecoveringTablet) {
    // 3. calling on a tombstone with a non-RECOVERING tablet discards
    TestLog::Enable _(removeIfTombstoneFilter);
    Key key(1, "key!", 4);
    Log::Reference reference = storeTombstone(key);
    tabletManager.addTablet(1, 0, ~0UL, TabletManager::NORMAL);
    ObjectManager::CleanupParameters params = { &objectManager, 0 };
    objectManager.removeIfTombstone(reference.toInteger(), &params);
    EXPECT_EQ("removeIfTombstone: discarding", TestLog::get());
}

TEST_F(ObjectManagerTest, removeIfTombstone_noTablet) {
    // 4. calling on a tombstone with no tablet discards
    TestLog::Enable _(removeIfTombstoneFilter);
    Key key(1, "key!", 4);
    Log::Reference reference = storeTombstone(key);
    ObjectManager::CleanupParameters params = { &objectManager, 0 };
    objectManager.removeIfTombstone(reference.toInteger(), &params);
    EXPECT_EQ("removeIfTombstone: discarding", TestLog::get());
}

TEST_F(ObjectManagerTest, removeTombstones) {
    Key key(0, "key!", 4);
    storeTombstone(key);

    objectManager.removeTombstones();

    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        LogEntryType type;
        Buffer buffer;
        EXPECT_FALSE(objectManager.lookup(lock, key, type, buffer, 0, 0));
    }
}

TEST_F(ObjectManagerTest, RemoveTombstonePoller_poll) {
    LogEntryType type;
    Buffer buffer;

    Key key(0, "key!", 4);
    storeTombstone(key);

    ObjectManager::RemoveTombstonePoller* remover =
        &objectManager.tombstoneRemover;

    // poller should do nothing if it doesn't think there's work to do
    EXPECT_EQ(0U, remover->currentBucket);
    objectManager.tombstoneRemover.lastReplaySegmentCount =
        objectManager.replaySegmentReturnCount = 0;

    TestLog::Enable _;

    for (uint64_t i = 0; i < 2 * objectManager.objectMap.getNumBuckets(); i++)
        objectManager.tombstoneRemover.poll();
    EXPECT_EQ("", TestLog::get());
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        EXPECT_TRUE(objectManager.lookup(lock, key, type, buffer, 0, 0));
    }

    // now it should scan the whole thing
    TestLog::reset();
    EXPECT_EQ(0U, remover->currentBucket);
    objectManager.replaySegmentReturnCount++;
    for (uint64_t i = 0; i < objectManager.objectMap.getNumBuckets(); i++)
        objectManager.tombstoneRemover.poll();
    EXPECT_EQ("removeIfTombstone: discarding | "
              "poll: Cleanup of tombstones completed pass 0", TestLog::get());
    EXPECT_EQ(1U, remover->passes);
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        EXPECT_FALSE(objectManager.lookup(lock, key, type, buffer, 0, 0));
    }

    // and it shouldn't run anymore...
    TestLog::reset();
    for (uint64_t i = 0; i < 2 * objectManager.objectMap.getNumBuckets(); i++)
        objectManager.tombstoneRemover.poll();
    EXPECT_EQ("", TestLog::get());
}

}  // namespace RAMCloud
