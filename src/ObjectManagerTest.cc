/* Copyright (c) 2014 Stanford University
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
    ServerId serverId;
    ServerList serverList;
    ServerConfig masterConfig;
    MasterTableMetadata masterTableMetadata;
    TabletManager tabletManager;
    ObjectManager objectManager;

    ObjectManagerTest()
        : context()
        , serverId(5)
        , serverList(&context)
        , masterConfig(ServerConfig::forTesting())
        , masterTableMetadata()
        , tabletManager()
        , objectManager(&context,
                        &serverId,
                        &masterConfig,
                        &tabletManager,
                        &masterTableMetadata)
    {
        objectManager.initOnceEnlisted();
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

        Buffer dataBuffer;
        Object newObject(key, objContents.c_str(), dataLength, version, 0,
                            dataBuffer);

        Buffer newObjectBuffer;
        newObject.assembleForLog(newObjectBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_OBJ, newObjectBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.size());
        buffer.copy(0, buffer.size(), segmentBuf);
        s.getAppendedLength(outCertificate);

        return buffer.size();
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
        tomb.assembleForLog(newTombstoneBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_OBJTOMB, newTombstoneBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.size());
        buffer.copy(0, buffer.size(), segmentBuf);
        s.getAppendedLength(outCertificate);

        return buffer.size();
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
        safeVer.assembleForLog(newSafeVerBuffer);
        bool success = s.append(LOG_ENTRY_TYPE_SAFEVERSION,
                                newSafeVerBuffer);
        EXPECT_TRUE(success);
        s.close();

        Buffer buffer;
        s.appendToBuffer(buffer);
        EXPECT_GE(segmentCapacity, buffer.size());
        buffer.copy(0, buffer.size(), segmentBuf);
        s.getAppendedLength(outCertificate);

        return buffer.size();
    }

    /**
     * Store an object in the log and hash table, returning its Log::Reference.
     */
    Log::Reference
    storeObject(Key& key, string value, uint64_t version = 0)
    {
        Buffer dataBuffer;
        Object o(key, value.c_str(), downCast<uint32_t>(value.size()),
                    version, 0, dataBuffer);

        Buffer buffer;
        o.assembleForLog(buffer);
        Log::Reference reference;
        objectManager.log.append(LOG_ENTRY_TYPE_OBJ, buffer, &reference);
        {
            ObjectManager::HashTableBucketLock lock(objectManager, key);
            objectManager.replace(lock, key, reference);
        }
        TableStats::increment(&masterTableMetadata,
                              key.getTableId(),
                              buffer.size(),
                              1);
        return reference;
    }

    /**
     * Store a tombstone in the log and hash table, return its Log::Reference.
     */
    Log::Reference
    storeTombstone(Key& key, uint64_t version = 0)
    {
        Buffer dataBuffer;
        Object o(key, NULL, 0, version, 0, dataBuffer);
        ObjectTombstone t(o, 0, 0);
        Buffer buffer;
        t.assembleForLog(buffer);
        Log::Reference reference;
        objectManager.log.append(LOG_ENTRY_TYPE_OBJTOMB, buffer, &reference);
        {
            ObjectManager::HashTableBucketLock lock(objectManager, key);
            objectManager.replace(lock, key, reference);
        }
        TableStats::increment(&masterTableMetadata,
                              key.getTableId(),
                              buffer.size(),
                              1);
        return reference;
    }

    /**
     * Verify an object replayed during recovery by looking it up in the hash
     * table by key and comparing contents.
     */
    void
    verifyRecoveryObject(Key& key, string contents)
    {
        ObjectBuffer value;
        EXPECT_EQ(STATUS_OK, objectManager.readObject(key, &value, NULL, NULL));
        const char *s = reinterpret_cast<const char *>(value.getValue());
        EXPECT_EQ(0, strcmp(s, contents.c_str()));
    }

    /**
     * Verify that the safe version number was correctly recovered.
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
                if (safeVerSrc->header.safeVersion
                    == safeVerDest.header.safeVersion) {
                    safeVerFound = true;
                }
            }
        }
        EXPECT_TRUE(safeVerFound);
        return safeVerScanned;
    }

    /**
     * Returns a stringafied format of metadata found for a particular table.
     *
     * \param tableId
     *      TableId of entry to check for.
     * \return
     *      Returns a string that represents the metadata if found.
     */
    std::string
    verifyMetadata(uint64_t tableId)
    {
        MasterTableMetadata::Entry* entry;

        entry = masterTableMetadata.find(tableId);
        if (entry != NULL) {
            return format("found=true tableId=%lu byteCount=%lu recordCount=%lu"
                       , tableId
                       , entry->stats.byteCount
                       , entry->stats.recordCount);

        } else {
            return format("found=false tableId=%lu"
                       , tableId);
        }
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
    EXPECT_EQ(ServerId(5), *objectManager.segmentManager.logId);
    EXPECT_EQ(ServerId(5), *objectManager.replicaManager.masterId);
}

TEST_F(ObjectManagerTest, readHashes) {
    uint64_t tableId = 0;
    uint8_t numKeys = 2;

    KeyInfo keyList0[2];
    keyList0[0].keyLength = 8;
    keyList0[0].key = "obj0key0";
    keyList0[1].keyLength = 8;
    keyList0[1].key = "obj0key1";

    const void* value0 = "obj0value";
    uint32_t valueLength0 = 9;

    Buffer keysAndVal0;
    Object::appendKeysAndValueToBuffer(tableId, numKeys, keyList0, value0,
                                       valueLength0, &keysAndVal0);

    Object obj0(tableId, 0, 0, keysAndVal0);
    Status writeStatus0 = objectManager.writeObject(obj0, NULL, NULL);
    EXPECT_EQ(STATUS_OK, writeStatus0);

    KeyInfo keyList1[2];
    keyList1[0].keyLength = 8;
    keyList1[0].key = "obj1key0";
    keyList1[1].keyLength = 8;
    keyList1[1].key = "obj1key1";

    const void* value1 = "obj1value";
    uint32_t valueLength1 = 9;

    Buffer keysAndVal1;
    Object::appendKeysAndValueToBuffer(tableId, numKeys, keyList1, value1,
                                       valueLength1, &keysAndVal1);

    Object obj1(tableId, 0, 0, keysAndVal1);
    Status writeStatus1 = objectManager.writeObject(obj1, NULL, NULL);
    EXPECT_EQ(STATUS_OK, writeStatus1);

    Buffer pKHashes;
    pKHashes.emplaceAppend<uint64_t>(obj0.getPKHash());
    pKHashes.emplaceAppend<uint64_t>(obj1.getPKHash());

    // readHashes such that both objects are read.
    Buffer responseBuffer;
    uint32_t numHashesResponse;
    uint32_t numObjectsResponse;
    objectManager.readHashes(tableId, 2, &pKHashes, 0, 1000, &responseBuffer,
            &numHashesResponse, &numObjectsResponse);

    EXPECT_EQ(2U, numHashesResponse);
    EXPECT_EQ(2U, numObjectsResponse);

    uint32_t respOffset = 0;
    respOffset += sizeof32(uint64_t); // version
    // value of object0
    uint32_t obj0Length = *responseBuffer.getOffset<uint32_t>(respOffset);
    respOffset += sizeof32(uint32_t); // length
    Object o0(tableId, 1, 0, responseBuffer, respOffset, obj0Length);
    EXPECT_EQ("obj0value", string(reinterpret_cast<const char*>(o0.getValue()),
                                  o0.getValueLength()));
    respOffset += obj0Length;

    respOffset += sizeof32(uint64_t); // version
    // value of object1
    uint32_t obj1Length = *responseBuffer.getOffset<uint32_t>(respOffset);
    respOffset += sizeof32(uint32_t); // length
    Object o1(tableId, 1, 0, responseBuffer, respOffset, obj1Length);
    EXPECT_EQ("obj1value", string(reinterpret_cast<const char*>(o1.getValue()),
                                  o1.getValueLength()));
}

TEST_F(ObjectManagerTest, readObject) {
    Buffer buffer;
    Key key(1, "1", 1);
    storeObject(key, "hi", 93);
    EXPECT_EQ("found=true tableId=1 byteCount=30 recordCount=1"
              , verifyMetadata(1));

    // no tablet, no dice
    EXPECT_EQ(STATUS_UNKNOWN_TABLET,
        objectManager.readObject(key, &buffer, 0, 0));

    // non-normal tablet, no dice
    tabletManager.addTablet(1, 0, ~0UL, TabletManager::RECOVERING);
    EXPECT_EQ(STATUS_UNKNOWN_TABLET,
        objectManager.readObject(key, &buffer, 0, 0));

    // (now make the tablet acceptable for handling reads)
    tabletManager.changeState(1, 0, ~0UL, TabletManager::RECOVERING,
                                          TabletManager::NORMAL);

    // not found, clearly no dice
    Key key2(1, "2", 1);
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST,
        objectManager.readObject(key2, &buffer, 0, 0));

    // non-object, no dice
    storeTombstone(key2);
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST,
        objectManager.readObject(key2, &buffer, 0, 0));

    // ensure reject rules are applied
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.exists = 1;
    EXPECT_EQ(STATUS_OBJECT_EXISTS,
        objectManager.readObject(key, &buffer, &rules, 0));

    // let's finally try a case that should work...
    uint64_t version;
    EXPECT_EQ(STATUS_OK, objectManager.readObject(key, &buffer, 0, &version));
    EXPECT_EQ(93UL, version);
    EXPECT_EQ(
        "{ tableId: 0 startKeyHash: 0 "
            "endKeyHash: 18446744073709551615 state: 0 reads: 0 writes: 0 }\n"
        "{ tableId: 1 startKeyHash: 0 "
            "endKeyHash: 18446744073709551615 state: 0 reads: 4 writes: 0 }",
        tabletManager.toString());
}

static bool
antiGetEntryFilter(string s)
{
    return s != "getEntry";
}

TEST_F(ObjectManagerTest, removeObject) {
    Key key(1, "1", 1);
    storeObject(key, "hi", 93);
    EXPECT_EQ("found=true tableId=1 byteCount=30 recordCount=1"
              , verifyMetadata(1));

    // no tablet, no dice
    EXPECT_EQ(STATUS_UNKNOWN_TABLET, objectManager.removeObject(key, 0, 0));
    EXPECT_EQ("found=true tableId=1 byteCount=30 recordCount=1"
              , verifyMetadata(1));

    // non-normal tablet, no dice
    tabletManager.addTablet(1, 0, ~0UL, TabletManager::RECOVERING);
    EXPECT_EQ(STATUS_UNKNOWN_TABLET, objectManager.removeObject(key, 0, 0));
    EXPECT_EQ("found=true tableId=1 byteCount=30 recordCount=1"
              , verifyMetadata(1));

    // (now make the tablet acceptable for handling removes)
    tabletManager.changeState(1, 0, ~0UL, TabletManager::RECOVERING,
                                          TabletManager::NORMAL);

    // not found, not an error
    Key key2(1, "2", 1);
    EXPECT_EQ(STATUS_OK, objectManager.removeObject(key2, 0, 0));
    EXPECT_EQ("found=true tableId=1 byteCount=30 recordCount=1"
              , verifyMetadata(1));

    // non-object, not an error
    storeTombstone(key2);
    EXPECT_EQ("found=true tableId=1 byteCount=63 recordCount=2"
              , verifyMetadata(1));
    EXPECT_EQ(STATUS_OK, objectManager.removeObject(key2, 0, 0));
    EXPECT_EQ("found=true tableId=1 byteCount=63 recordCount=2"
              , verifyMetadata(1));

    // ensure reject rules are applied
    RejectRules rules;
    memset(&rules, 0, sizeof(rules));
    rules.exists = 1;
    EXPECT_EQ(STATUS_OBJECT_EXISTS,
        objectManager.removeObject(key, &rules, 0));
    EXPECT_EQ("found=true tableId=1 byteCount=63 recordCount=2"
              , verifyMetadata(1));

    // let's finally try a case that should work...
    TestLog::Enable _(antiGetEntryFilter);
    HashTable::Candidates c;
    objectManager.objectMap.lookup(key.getHash(), c);
    uint64_t ref = c.getReference();
    uint64_t version;
    EXPECT_EQ(STATUS_OK, objectManager.removeObject(key, 0, &version));
    EXPECT_EQ("found=true tableId=1 byteCount=96 recordCount=3"
              , verifyMetadata(1));
    EXPECT_EQ(93UL, version);
    EXPECT_EQ(format("free: free on reference %lu", ref), TestLog::get());
    Buffer buffer;
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST,
        objectManager.readObject(key, &buffer, 0, 0));
    EXPECT_EQ(94UL, objectManager.segmentManager.safeVersion);
    LogEntryType type;
    ObjectManager::HashTableBucketLock lock(objectManager, key);
    EXPECT_FALSE(objectManager.lookup(lock, key, type, buffer, 0, 0));
}

TEST_F(ObjectManagerTest, removeObject_returnRemovedObj) {
    Key key(1, "a", 1);
    storeObject(key, "hi", 93);
    EXPECT_EQ("found=true tableId=1 byteCount=30 recordCount=1"
              , verifyMetadata(1));

    tabletManager.addTablet(1, 0, ~0UL, TabletManager::NORMAL);

    TestLog::Enable _(antiGetEntryFilter);
    HashTable::Candidates c;
    objectManager.objectMap.lookup(key.getHash(), c);
    uint64_t ref = c.getReference();
    uint64_t version;
    Buffer removedObjBuffer;
    EXPECT_EQ(STATUS_OK,
              objectManager.removeObject(key, 0, &version, &removedObjBuffer));

    // Check that the remove succeeded and the object doesn't exist.
    EXPECT_EQ("found=true tableId=1 byteCount=63 recordCount=2",
              verifyMetadata(1));
    EXPECT_EQ(93UL, version);

    Buffer buffer;
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST,
        objectManager.readObject(key, &buffer, 0, 0));
    EXPECT_EQ(94UL, objectManager.segmentManager.safeVersion);
    LogEntryType type;
    ObjectManager::HashTableBucketLock lock(objectManager, key);
    EXPECT_FALSE(objectManager.lookup(lock, key, type, buffer, 0, 0));
    EXPECT_EQ(format("free: free on reference %lu", ref), TestLog::get());

    // Check that the buffer returned corresponds to the object removed.
    Object oldObj(removedObjBuffer);
    EXPECT_EQ(1U, oldObj.getKeyCount());
    KeyLength oldKeyLength;
    const void* oldKey = oldObj.getKey(0, &oldKeyLength);
    EXPECT_EQ("a", string(reinterpret_cast<const char*>(oldKey), oldKeyLength));
    uint32_t oldValueLength;
    const void* oldValue = oldObj.getValue(&oldValueLength);
    EXPECT_EQ("hi", string(reinterpret_cast<const char*>(oldValue),
                           oldValueLength));
}

TEST_F(ObjectManagerTest, removeOrphanedObjects) {
    tabletManager.addTablet(97, 0, ~0UL, TabletManager::NORMAL);
    Key key(97, "1", 1);

    Buffer value;
    Object obj(key, "hi", 2, 0, 0, value);

    EXPECT_EQ(STATUS_OK, objectManager.writeObject(obj, NULL, NULL));

    EXPECT_TRUE(tabletManager.getTablet(key, NULL));
    EXPECT_EQ(STATUS_OK, objectManager.readObject(key, &value, NULL, NULL));

    objectManager.removeOrphanedObjects();
    EXPECT_EQ(STATUS_OK, objectManager.readObject(key, &value, NULL, NULL));

    tabletManager.deleteTablet(97, 0, ~0UL);
    EXPECT_FALSE(tabletManager.getTablet(key, NULL));

    value.reset();
    TestLog::Enable _(antiGetEntryFilter);
    HashTable::Candidates c;
    objectManager.objectMap.lookup(key.getHash(), c);
    uint64_t ref = c.getReference();
    objectManager.removeOrphanedObjects();
    EXPECT_EQ(STATUS_UNKNOWN_TABLET,
        objectManager.readObject(key, &value, NULL, NULL));

    tabletManager.addTablet(97, 0, ~0UL, TabletManager::NORMAL);
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST,
        objectManager.readObject(key, &value, NULL, NULL));

    EXPECT_EQ(
        format("removeIfOrphanedObject: removing orphaned object at ref %lu | "
               "free: free on reference %lu", ref, ref),
        TestLog::get());
}

TEST_F(ObjectManagerTest, replaySegment_highestBTreeIdMap) {
    uint32_t segLen = 8192;
    char seg[segLen];
    uint32_t len; // number of bytes in a recovery segment
    Buffer buffer;
    Log::Reference reference;
    Log::Reference logTomb1Ref;
    Log::Reference logTomb2Ref;
    SideLog sl(&objectManager.log);

    char keyStr[8];
    uint64_t *bTreeKey = reinterpret_cast<uint64_t*>(keyStr);
    *bTreeKey = 12345;
    Key key0(0, keyStr, 8);
    Segment::Certificate certificate;
    len = buildRecoverySegment(seg, segLen, key0, 1, "newer guy", &certificate);
    Tub<SegmentIterator> it;
    it.construct(&seg[0], len, certificate);
    std::unordered_map<uint64_t, uint64_t> highestBTreeIdMap;
    highestBTreeIdMap[0] = 0;
    objectManager.replaySegment(&sl, *it, highestBTreeIdMap);
    EXPECT_EQ(12345U, highestBTreeIdMap[0]);
    EXPECT_EQ("found=true tableId=0 byteCount=45 recordCount=1"
              , verifyMetadata(0));
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
    EXPECT_EQ("found=true tableId=0 byteCount=41 recordCount=1"
              , verifyMetadata(0));
    len = buildRecoverySegment(seg, segLen, key0, 0, "older guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key0, "newer guy");
    EXPECT_EQ("found=true tableId=0 byteCount=41 recordCount=1"
              , verifyMetadata(0));

    // Case 1b: Older object already there; replace object.
    Key key1(0, "key1", 4);
    len = buildRecoverySegment(seg, segLen, key1, 0, "older guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key1, "older guy");
    EXPECT_EQ("found=true tableId=0 byteCount=82 recordCount=2"
              , verifyMetadata(0)); // Object added.
    len = buildRecoverySegment(seg, segLen, key1, 1, "newer guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    verifyRecoveryObject(key1, "newer guy");
    EXPECT_EQ("found=true tableId=0 byteCount=123 recordCount=3"
              , verifyMetadata(0));

    // Case 2a: Equal/newer tombstone already there; ignore object.
    Key key2(0, "key2", 4);
    Buffer dataBuffer;
    Object o1(key2, NULL, 0, 1, 0, dataBuffer);

    ObjectTombstone t1(o1, 0, 0);
    buffer.reset();
    t1.assembleForLog(buffer);
    objectManager.log.append(LOG_ENTRY_TYPE_OBJTOMB, buffer, &logTomb1Ref);
    objectManager.log.sync();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key2);
        ret = objectManager.replace(lock, key2, logTomb1Ref);
    }
    EXPECT_FALSE(ret);
    len = buildRecoverySegment(seg, segLen, key2, 1, "equal guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    EXPECT_EQ("found=true tableId=0 byteCount=123 recordCount=3"
              , verifyMetadata(0));
    len = buildRecoverySegment(seg, segLen, key2, 0, "older guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    EXPECT_TRUE(lookup(key2, &reference));
    EXPECT_EQ(reference, logTomb1Ref);
    objectManager.removeTombstones();
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, getObjectStatus(0, "key2", 4));

    // Case 2b: Lesser tombstone already there; add object, remove tomb.
    Key key3(0, "key3", 4);

    dataBuffer.reset();
    Object o2(key3, NULL, 0, 10, 0, dataBuffer);

    ObjectTombstone t2(o2, 0, 0);
    buffer.reset();
    t2.assembleForLog(buffer);
    ret = objectManager.log.append(
        LOG_ENTRY_TYPE_OBJTOMB, buffer, &logTomb2Ref);
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
    EXPECT_EQ("found=true tableId=0 byteCount=164 recordCount=4"
              , verifyMetadata(0));
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
    EXPECT_EQ("found=true tableId=0 byteCount=204 recordCount=5"
              , verifyMetadata(0));
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
    EXPECT_EQ("found=true tableId=0 byteCount=245 recordCount=6"
              , verifyMetadata(0));
    dataBuffer.reset();
    Object o3(key5, NULL, 0, 0, 0, dataBuffer);

    ObjectTombstone t3(o3, 0, 0);
    len = buildRecoverySegment(seg, segLen, t3, &certificate);
    objectManager.replaySegment(&sl, *it);
    EXPECT_EQ("found=true tableId=0 byteCount=245 recordCount=6"
              , verifyMetadata(0));
    verifyRecoveryObject(key5, "newer guy");

    // Case 1b: Equal/older object already there; discard and add tombstone.
    Key key6(0, "key6", 4);
    len = buildRecoverySegment(seg, segLen, key6, 0, "equal guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    EXPECT_EQ("found=true tableId=0 byteCount=286 recordCount=7"
              , verifyMetadata(0));
    verifyRecoveryObject(key6, "equal guy");

    dataBuffer.reset();
    Object o4(key6, NULL, 0, 0, 0, dataBuffer);

    ObjectTombstone t4(o4, 0, 0);
    len = buildRecoverySegment(seg, segLen, t4, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    objectManager.removeTombstones();
    EXPECT_EQ("found=true tableId=0 byteCount=322 recordCount=8"
              , verifyMetadata(0));
    EXPECT_FALSE(lookup(key6, &reference));
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, getObjectStatus(0, "key6", 4));

    Key key7(0, "key7", 4);
    len = buildRecoverySegment(seg, segLen, key7, 0, "older guy", &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    EXPECT_EQ("found=true tableId=0 byteCount=363 recordCount=9"
              , verifyMetadata(0));
    verifyRecoveryObject(key7, "older guy");
    dataBuffer.reset();
    Object o5(key7, NULL, 0, 1, 0, dataBuffer);

    ObjectTombstone t5(o5, 0, 0);
    len = buildRecoverySegment(seg, segLen, t5, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    objectManager.removeTombstones();
    EXPECT_EQ("found=true tableId=0 byteCount=399 recordCount=10"
              , verifyMetadata(0));
    EXPECT_FALSE(lookup(key7, &reference));
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST, getObjectStatus(0, "key7", 4));

    // Case 2a: Newer tombstone already there; ignore.
    Key key8(0, "key8", 4);
    dataBuffer.reset();
    Object o6(key8, NULL, 0, 1, 0, dataBuffer);

    ObjectTombstone t6(o6, 0, 0);
    len = buildRecoverySegment(seg, segLen, t6, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    buffer.reset();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key8);
        ret = objectManager.lookup(lock, key8, type, buffer);
    }
    EXPECT_EQ("found=true tableId=0 byteCount=435 recordCount=11"
              , verifyMetadata(0));
    EXPECT_TRUE(ret);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, type);
    ObjectTombstone t6InLog(buffer);
    EXPECT_EQ(1U, t6InLog.getObjectVersion());
    ObjectTombstone t7(o6, 0, 0);
    t7.header.objectVersion = 0;
    t7.header.checksum = t7.computeChecksum();
    len = buildRecoverySegment(seg, segLen, t7, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    const uint8_t* t6LogPtr = buffer.getStart<uint8_t>();
    buffer.reset();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key8);
        ret = objectManager.lookup(lock, key8, type, buffer);
    }
    EXPECT_EQ("found=true tableId=0 byteCount=435 recordCount=11"
              , verifyMetadata(0));
    EXPECT_TRUE(ret);
    EXPECT_EQ(t6LogPtr, buffer.getStart<uint8_t>());

    // Case 2b: Older tombstone already there; replace.
    Key key9(0, "key9", 4);
    dataBuffer.reset();
    Object o8(key9, NULL, 0, 0, 0, dataBuffer);

    ObjectTombstone t8(o8, 0, 0);
    len = buildRecoverySegment(seg, segLen, t8, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    buffer.reset();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key9);
        ret = objectManager.lookup(lock, key9, type, buffer);
    }
    EXPECT_EQ("found=true tableId=0 byteCount=471 recordCount=12"
              , verifyMetadata(0));
    EXPECT_TRUE(ret);
    ObjectTombstone t8InLog(buffer);
    EXPECT_EQ(0U, t8InLog.getObjectVersion());

    dataBuffer.reset();
    Object o9(key9, NULL, 0, 1, 0, dataBuffer);

    ObjectTombstone t9(o9, 0, 0);
    len = buildRecoverySegment(seg, segLen, t9, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    buffer.reset();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key9);
        ret = objectManager.lookup(lock, key9, type, buffer);
    }
    EXPECT_EQ("found=true tableId=0 byteCount=507 recordCount=13"
              , verifyMetadata(0));
    EXPECT_TRUE(ret);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, type);
    ObjectTombstone t9InLog(buffer);
    EXPECT_EQ(1U, t9InLog.getObjectVersion());

    // Case 3: No tombstone, no object. Recovered tombstone always added.
    Key key10(0, "key10", 5);
    EXPECT_FALSE(lookup(key10, &reference));
    dataBuffer.reset();
    Object o10(key10, NULL, 0, 0, 0, dataBuffer);

    ObjectTombstone t10(o10, 0, 0);
    len = buildRecoverySegment(seg, segLen, t10, &certificate);
    it.construct(&seg[0], len, certificate);
    objectManager.replaySegment(&sl, *it);
    buffer.reset();
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key10);
        EXPECT_TRUE(objectManager.lookup(lock, key10, type, buffer));
    }
    EXPECT_EQ("found=true tableId=0 byteCount=544 recordCount=14"
              , verifyMetadata(0));
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, type);
    Buffer t10Buffer;
    t10.assembleForLog(t10Buffer);
    EXPECT_EQ(string(reinterpret_cast<const char*>(
                     t10Buffer.getRange(0, t10Buffer.size())),
                     buffer.size()),
              string(reinterpret_cast<const char*>(
                     buffer.getRange(0, buffer.size())),
                     buffer.size()));
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

static bool
writeObjectFilter(string s)
{
    return s == "writeObject";
}

TEST_F(ObjectManagerTest, writeObject) {
    Key key(1, "1", 1);
    Buffer buffer;
    // create an object just so that buffer will be populated with the key
    // and the value. This keeps the abstractions intact
    Object obj(key, "value", 5, 0, 0, buffer);

    // no tablet, no dice.
    EXPECT_EQ(STATUS_UNKNOWN_TABLET,
        objectManager.writeObject(obj, 0, 0));
    EXPECT_EQ("found=false tableId=1", verifyMetadata(1));

    // non-NORMAL tablet state, no dice.
    tabletManager.addTablet(1, 0, ~0UL, TabletManager::RECOVERING);
    EXPECT_EQ(STATUS_UNKNOWN_TABLET,
        objectManager.writeObject(obj, 0, 0));
    EXPECT_EQ("found=false tableId=1", verifyMetadata(1));

    TestLog::Enable _(writeObjectFilter);

    // new object (no tombstone needed)
    tabletManager.changeState(1, 0, ~0UL, TabletManager::RECOVERING,
                                          TabletManager::NORMAL);
    EXPECT_EQ(STATUS_OK, objectManager.writeObject(obj, 0, 0));
    EXPECT_EQ("writeObject: object: 33 bytes, version 1", TestLog::get());
    EXPECT_EQ("found=true tableId=1 byteCount=33 recordCount=1"
              , verifyMetadata(1));

    // object overwrite (tombstone needed)
    TestLog::reset();
    EXPECT_EQ(STATUS_OK, objectManager.writeObject(obj, 0, 0));
    EXPECT_EQ("writeObject: object: 33 bytes, version 2 | "
              "writeObject: tombstone: 33 bytes, version 1", TestLog::get());
    EXPECT_EQ("found=true tableId=1 byteCount=99 recordCount=3"
              , verifyMetadata(1));

    // object overwrite (hashtable contains tombstone)
    Log::Reference reference = storeTombstone(key, 0);
    EXPECT_EQ(STATUS_OK, objectManager.writeObject(obj, 0, 0));

    // Verify RetryException  when overwriting with no space
    uint64_t original = objectManager.getLog()->totalBytesRemaining;
    objectManager.getLog()->totalBytesRemaining = 0;
    EXPECT_THROW(objectManager.writeObject(obj, 0, 0), RetryException);
    objectManager.getLog()->totalBytesRemaining = original;
}

TEST_F(ObjectManagerTest, writeObject_returnRemovedObj) {
    tabletManager.addTablet(1, 0, ~0UL, TabletManager::NORMAL);
    Key key(1, "a", 1);

    // New object.
    Buffer writeBuffer;
    Object obj1(key, "originalValue", 13, 0, 0, writeBuffer);
    EXPECT_EQ(STATUS_OK, objectManager.writeObject(obj1, 0, 0));

    // Object overwrite. This is what we're testing in this unit test.
    writeBuffer.reset();
    Object obj2(key, "newValue", 8, 0, 0, writeBuffer);

    TestLog::Enable _(writeObjectFilter);
    Buffer removedObjBuffer;
    EXPECT_EQ(STATUS_OK,
              objectManager.writeObject(obj2, 0, 0, &removedObjBuffer));

    // Check that the object got overwritten correctly.
    EXPECT_EQ("writeObject: object: 36 bytes, version 2 | "
              "writeObject: tombstone: 33 bytes, version 1", TestLog::get());
    EXPECT_EQ("found=true tableId=1 byteCount=110 recordCount=3",
              verifyMetadata(1));

    // Check that the buffer returned corresponds to the object overwritten.
    Object oldObj(removedObjBuffer);
    EXPECT_EQ(1U, oldObj.getKeyCount());
    KeyLength oldKeyLength;
    const void* oldKey = oldObj.getKey(0, &oldKeyLength);
    EXPECT_EQ("a", string(reinterpret_cast<const char*>(oldKey), oldKeyLength));
    uint32_t oldValueLength;
    const void* oldValue = oldObj.getValue(&oldValueLength);
    EXPECT_EQ("originalValue", string(reinterpret_cast<const char*>(oldValue),
                                      oldValueLength));
}

TEST_F(ObjectManagerTest, flushEntriesToLog) {

    tabletManager.addTablet(1, 0, ~0UL, TabletManager::NORMAL);

    Buffer logBuffer;
    uint32_t numEntries = 0;

    Key key1(1, "1", 1);
    Key key2(1, "2", 1);
    Key key3(1, "3", 1);
    Key key4(1, "4", 1);
    Buffer buffer;
    // create an object just so that buffer will be populated with the key
    // and the value. This keeps the abstractions intact

    Object obj1(key1, "value", 5, 1, 0, buffer);

    // Case i) new object (no tombstone needed)
    bool tombstoneAdded = false;
    EXPECT_EQ(STATUS_OK,
              objectManager.prepareForLog(obj1, &logBuffer, NULL,
                                          &tombstoneAdded));
    EXPECT_FALSE(tombstoneAdded);
    if (tombstoneAdded)
        numEntries+= 2;
    else
        numEntries+= 1;

    // Case ii) store an object in the log.
    // Now create a tombstone in logBuffer for that object
    storeObject(key2, "value", 1);
    EXPECT_EQ(STATUS_OK,
              objectManager.writeTombstone(key2, &logBuffer));
    numEntries++;

    // Case iii) store an object and its tombstone in the log.
    // Now create a newer version of the object in logBuffer
    // with the same key
    storeObject(key3, "value", 1);
    //storeTombstone(key3, 1);
    objectManager.removeObject(key3, NULL, NULL);

    tombstoneAdded = false;
    buffer.reset();
    // newer object has version = 2
    Object obj3(key3, "value", 5, 2, 0, buffer);
    EXPECT_EQ(STATUS_OK,
              objectManager.prepareForLog(obj3, &logBuffer, NULL,
                                          &tombstoneAdded));
    EXPECT_FALSE(tombstoneAdded);
    if (tombstoneAdded)
        numEntries+= 2;
    else
        numEntries+= 1;

    // Case iv) store an object in the log
    // Write a newer version of the object in logBuffer
    storeObject(key4, "value", 1);

    tombstoneAdded = false;
    buffer.reset();
    // newer object has version = 2
    Object obj4(key4, "value", 5, 2, 0, buffer);
    EXPECT_EQ(STATUS_OK,
              objectManager.prepareForLog(obj4, &logBuffer, NULL,
                                          &tombstoneAdded));
    EXPECT_TRUE(tombstoneAdded);
    if (tombstoneAdded)
        numEntries+= 2;
    else
        numEntries+= 1;

    EXPECT_EQ(5U, numEntries);

    //TestLog::Enable _(writeObjectFilter);
    TestLog::Enable _(antiGetEntryFilter);

    // Verify that the flush is rejected when the log's remaining size is small
    uint64_t original = objectManager.getLog()->totalBytesRemaining;
    objectManager.getLog()->totalBytesRemaining = 0;
    EXPECT_FALSE(objectManager.flushEntriesToLog(&logBuffer, numEntries));
    objectManager.getLog()->totalBytesRemaining = original;

    // flush all the entries in logBuffer to the log atomically
    EXPECT_TRUE(objectManager.flushEntriesToLog(&logBuffer, numEntries));
    EXPECT_EQ(0U, logBuffer.size());

    // now check the hashtable and the log for correctness

    // for key1
    Buffer readBuffer;
    uint64_t version;
    EXPECT_EQ(STATUS_OK, objectManager.readObject(key1, &readBuffer, 0,
              &version));
    EXPECT_EQ(1U, version);

    // for key3
    readBuffer.reset();
    EXPECT_EQ(STATUS_OK, objectManager.readObject(key3, &readBuffer, 0,
              &version));
    EXPECT_EQ(2U, version);

    // for key4
    readBuffer.reset();
    EXPECT_EQ(STATUS_OK, objectManager.readObject(key4, &readBuffer, 0,
              &version));
    EXPECT_EQ(2U, version);

    // for key2
    // key2 is checked in the end because the HashTable lock is acquired
    // and relies on the function going out of scope to be destroyed
    readBuffer.reset();
    EXPECT_EQ(STATUS_OBJECT_DOESNT_EXIST,
        objectManager.readObject(key2, &readBuffer, 0, 0));
    LogEntryType type;
    ObjectManager::HashTableBucketLock lock(objectManager, key2);
    EXPECT_FALSE(objectManager.lookup(lock, key2, type, buffer, 0, 0));
}

TEST_F(ObjectManagerTest, prepareForLog) {
    // nothing worth doing here. The functionality required for
    // this is anyway tested and has to be tested in the
    // flushEntriesToLog() test
}

TEST_F(ObjectManagerTest, writeTombstone) {
    Buffer logBuffer;

    Key key(1, "1", 1);
    storeObject(key, "hi", 93);
    EXPECT_EQ("found=true tableId=1 byteCount=30 recordCount=1"
              , verifyMetadata(1));

    // no tablet, no dice
    EXPECT_EQ(STATUS_UNKNOWN_TABLET,
              objectManager.writeTombstone(key, &logBuffer));
    EXPECT_EQ("found=true tableId=1 byteCount=30 recordCount=1"
              , verifyMetadata(1));

    // non-normal tablet, no dice
    tabletManager.addTablet(1, 0, ~0UL, TabletManager::RECOVERING);
    EXPECT_EQ(STATUS_UNKNOWN_TABLET,
              objectManager.writeTombstone(key, &logBuffer));
    EXPECT_EQ("found=true tableId=1 byteCount=30 recordCount=1"
              , verifyMetadata(1));

    // (now make the tablet acceptable for handling removes)
    tabletManager.changeState(1, 0, ~0UL, TabletManager::RECOVERING,
                                          TabletManager::NORMAL);

    // not found, not an error
    Key key2(1, "2", 1);
    EXPECT_EQ(STATUS_OK,
              objectManager.writeTombstone(key2, &logBuffer));
    EXPECT_EQ("found=true tableId=1 byteCount=30 recordCount=1"
              , verifyMetadata(1));

    // non-object, not an error
    storeTombstone(key2);
    EXPECT_EQ("found=true tableId=1 byteCount=63 recordCount=2"
              , verifyMetadata(1));
    EXPECT_EQ(STATUS_OK,
              objectManager.writeTombstone(key2, &logBuffer));
    EXPECT_EQ("found=true tableId=1 byteCount=63 recordCount=2"
              , verifyMetadata(1));

    // let's finally try a case that should work...
    // the tombstone is only written to the buffer
    TestLog::Enable _(antiGetEntryFilter);
    EXPECT_EQ(STATUS_OK,
              objectManager.writeTombstone(key, &logBuffer));
    EXPECT_EQ("found=true tableId=1 byteCount=63 recordCount=2"
              , verifyMetadata(1));
}

TEST_F(ObjectManagerTest, RemoveTombstonePoller_poll) {
    LogEntryType type;
    Buffer buffer;

    Key key(0, "key!", 4);
    storeTombstone(key);

    ObjectManager::RemoveTombstonePoller* remover =
        objectManager.tombstoneRemover.get();

    // poller should do nothing if it doesn't think there's work to do
    EXPECT_EQ(0U, remover->currentBucket);
    objectManager.tombstoneRemover->lastReplaySegmentCount =
        objectManager.replaySegmentReturnCount = 0;

    TestLog::Enable _(antiGetEntryFilter);

    for (uint64_t i = 0; i < 2 * objectManager.objectMap.getNumBuckets(); i++)
        objectManager.tombstoneRemover->poll();
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
        objectManager.tombstoneRemover->poll();
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
        objectManager.tombstoneRemover->poll();
    EXPECT_EQ("", TestLog::get());
}

TEST_F(ObjectManagerTest, lookup_object) {
    Key key(1, "1", 1);
    Buffer buffer;
    LogEntryType type;

    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        EXPECT_FALSE(objectManager.lookup(lock, key, type, buffer, 0, 0));
    }

    Log::Reference reference = storeObject(key, "value", 15);

    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        EXPECT_TRUE(objectManager.lookup(lock, key, type, buffer, 0, 0));
    }
    Object o(buffer);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, type);
    EXPECT_EQ("1", string(reinterpret_cast<const char*>(
                   o.getKey()), 1));
    const void *dataBlob = reinterpret_cast<const void *>(
                            reinterpret_cast<const uint8_t*>(o.getValue()));
    EXPECT_EQ("value", string(reinterpret_cast<const char*>(
                   dataBlob), 5));

    uint64_t v;
    Log::Reference r;
    ObjectManager::HashTableBucketLock lock(objectManager, key);
    EXPECT_TRUE(objectManager.lookup(lock, key, type, buffer, &v, &r));
    EXPECT_EQ(15U, v);
    EXPECT_EQ(reference, r);
}

TEST_F(ObjectManagerTest, lookup_tombstone) {
    Key key(1, "1", 1);
    Buffer buffer;
    LogEntryType type;

    Log::Reference reference = storeTombstone(key, 15);

    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        EXPECT_TRUE(objectManager.lookup(lock, key, type, buffer, 0, 0));
    }
    ObjectTombstone t(buffer);
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJTOMB, type);
    EXPECT_EQ("1", string(reinterpret_cast<const char*>(
                   t.getKey()), 1));
    EXPECT_EQ(15U, t.getObjectVersion());

    uint64_t v;
    Log::Reference r;
    ObjectManager::HashTableBucketLock lock(objectManager, key);
    EXPECT_TRUE(objectManager.lookup(lock, key, type, buffer, &v, &r));
    EXPECT_EQ(15U, v);
    EXPECT_EQ(reference, r);
}

TEST_F(ObjectManagerTest, remove) {
    Key key(1, "1", 1);
    Key key2(2, "2", 2);

    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        EXPECT_FALSE(objectManager.remove(lock, key));
    }

    storeObject(key, "value1", 15);
    storeTombstone(key2, 827);

    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        EXPECT_TRUE(objectManager.remove(lock, key));
        EXPECT_FALSE(objectManager.remove(lock, key));

        EXPECT_TRUE(objectManager.remove(lock, key2));
        EXPECT_FALSE(objectManager.remove(lock, key2));
    }
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
    Object obj(key, "item0", 5, 0, 0, value);
    objectManager.writeObject(obj, NULL, NULL);
    EXPECT_EQ("found=true tableId=0 byteCount=36 recordCount=1"
              , verifyMetadata(0));

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

    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    objectManager.relocate(LOG_ENTRY_TYPE_OBJ, oldBuffer,
                           oldReference, relocator);
    EXPECT_TRUE(relocator.didAppend);
    EXPECT_EQ("found=true tableId=0 byteCount=36 recordCount=1"
              , verifyMetadata(0));

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
    EXPECT_EQ(newReference.toInteger() + 38, newReference2.toInteger());
    EXPECT_NE(oldReference, newReference);
    EXPECT_NE(newBuffer.getStart<uint8_t>(),
              oldBuffer.getStart<uint8_t>());
}

TEST_F(ObjectManagerTest, objectRelocationCallback_objectDeleted) {
    Key key(0, "key0", 4);

    Buffer value;
    Object obj(key, "item0", 5, 0, 0, value);
    objectManager.writeObject(obj, NULL, NULL);
    EXPECT_EQ("found=true tableId=0 byteCount=36 recordCount=1"
              , verifyMetadata(0));

    LogEntryType type;
    Buffer buffer;
    bool success = false;
    Log::Reference reference;
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        success = objectManager.lookup(lock, key, type, buffer, 0, &reference);
    }
    EXPECT_TRUE(success);

    objectManager.removeObject(key, NULL, NULL);
    EXPECT_EQ("found=true tableId=0 byteCount=72 recordCount=2"
              , verifyMetadata(0));

    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    objectManager.relocate(LOG_ENTRY_TYPE_OBJ, buffer, reference, relocator);
    EXPECT_FALSE(relocator.didAppend);
    // Only the object was relocated so the stats should only reflect the
    // contents of the tombstone.
    EXPECT_EQ("found=true tableId=0 byteCount=36 recordCount=1"
              , verifyMetadata(0));
}

TEST_F(ObjectManagerTest, objectRelocationCallback_objectModified) {
    Key key(0, "key0", 4);

    Buffer value;
    Object obj(key, "item0", 5, 0, 0, value);
    objectManager.writeObject(obj, NULL, NULL);
    EXPECT_EQ("found=true tableId=0 byteCount=36 recordCount=1"
              , verifyMetadata(0));

    LogEntryType type;
    Buffer buffer;
    Log::Reference reference;
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        objectManager.lookup(lock, key, type, buffer, 0, &reference);
    }

    value.reset();

    Object object(key, "item0-v2", 8, 0, 0, value);
    objectManager.writeObject(object, NULL, NULL);
    EXPECT_EQ("found=true tableId=0 byteCount=111 recordCount=3"
              , verifyMetadata(0));

    Log::Reference dummyReference;

    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    objectManager.relocate(LOG_ENTRY_TYPE_OBJ, buffer, reference, relocator);
    EXPECT_FALSE(relocator.didAppend);
    // Only the object was relocated so the stats should only reflect the
    // contents of the tombstone and the new object.
    EXPECT_EQ("found=true tableId=0 byteCount=75 recordCount=2"
              , verifyMetadata(0));
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
    Object obj(key, "item0", 5, 0, 0, value);
    objectManager.writeObject(obj, NULL, NULL);
    EXPECT_EQ("found=true tableId=0 byteCount=36 recordCount=1"
              , verifyMetadata(0));

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
    tombstone.assembleForLog(tombstoneBuffer);

    Log::Reference oldTombstoneReference;
    success = objectManager.log.append(
        LOG_ENTRY_TYPE_OBJTOMB, tombstoneBuffer, &oldTombstoneReference);
    objectManager.log.sync();
    EXPECT_TRUE(success);
    // Update metadata manually due to manual log append.
    TableStats::increment(&masterTableMetadata,
                          tombstone.getTableId(),
                          tombstoneBuffer.size(),
                          1);
    EXPECT_EQ("found=true tableId=0 byteCount=72 recordCount=2"
              , verifyMetadata(0));

    Log::Reference newTombstoneReference;
    success = objectManager.log.append(LOG_ENTRY_TYPE_OBJTOMB,
        tombstoneBuffer, &newTombstoneReference);
    objectManager.log.sync();
    EXPECT_TRUE(success);
    // Update metadata manually due to manual log append.
    TableStats::increment(&masterTableMetadata,
                          tombstone.getTableId(),
                          tombstoneBuffer.size(),
                          1);
    EXPECT_EQ("found=true tableId=0 byteCount=108 recordCount=3"
              , verifyMetadata(0));


    LogEntryType oldTypeInLog;
    Buffer oldBufferInLog;
    oldTypeInLog = objectManager.log.getEntry(oldTombstoneReference,
                                          oldBufferInLog);

    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    objectManager.relocate(LOG_ENTRY_TYPE_OBJTOMB,
                           oldBufferInLog,
                           oldTombstoneReference,
                           relocator);
    EXPECT_TRUE(relocator.didAppend);
    // Relocator should not drop the old tombstone.  The stats should still
    // reflect the existence of the object and both tombstones.
    EXPECT_EQ("found=true tableId=0 byteCount=108 recordCount=3"
              , verifyMetadata(0));

    // Check that tombstoneRelocationCallback() is checking the liveness
    // of the right segment (in log.segmentExists() function call).
    string comparisonString = "segmentExists: " +
        format("%lu", objectManager.log.getSegmentId(oldTombstoneReference));
    EXPECT_EQ(comparisonString, TestLog::get());
}

TEST_F(ObjectManagerTest, tombstoneRelocationCallback_cleanTombstone) {
//    TestLog::Enable _(&segmentExists);
    // Testing that cleaning a tombstone will update table metadata.
    Key key(0, "key0", 4);
    Buffer dataBuffer;
    Object o(key, "hi", 2, 0, 0, dataBuffer);

    // Create tombstone will "bad" segmentId so it will be cleaned.
    ObjectTombstone tombstone(o, 0xBAD, 0);

    Buffer tombstoneBuffer;
    tombstone.assembleForLog(tombstoneBuffer);

    Log::Reference oldTombstoneReference;
    bool success = objectManager.log.append(
        LOG_ENTRY_TYPE_OBJTOMB, tombstoneBuffer, &oldTombstoneReference);
    objectManager.log.sync();
    EXPECT_TRUE(success);
    // Update metadata manually due to manual log append.
    TableStats::increment(&masterTableMetadata,
                          tombstone.getTableId(),
                          tombstoneBuffer.size(),
                          1);
    EXPECT_EQ("found=true tableId=0 byteCount=36 recordCount=1"
              , verifyMetadata(0));

    LogEntryType oldTypeInLog;
    Buffer oldBufferInLog;
    oldTypeInLog = objectManager.log.getEntry(oldTombstoneReference,
                                          oldBufferInLog);

    LogEntryRelocator relocator(
        objectManager.segmentManager.getHeadSegment(), 1000);
    objectManager.relocate(LOG_ENTRY_TYPE_OBJTOMB,
                           oldBufferInLog,
                           oldTombstoneReference,
                           relocator);
    EXPECT_FALSE(relocator.didAppend);
    // Tombstone should have been cleaned leaving no bytes and no objects.
    EXPECT_EQ("found=true tableId=0 byteCount=0 recordCount=0"
              , verifyMetadata(0));
}

TEST_F(ObjectManagerTest, replace_noPriorVersion) {
    Key key(1, "1", 1);

    ObjectManager::HashTableBucketLock lock(objectManager, key);
    HashTable::Candidates c;
    objectManager.objectMap.lookup(key.getHash(), c);
    EXPECT_TRUE(c.isDone());

    Log::Reference reference(0xdeadbeef);
    EXPECT_FALSE(objectManager.replace(lock, key, reference));
    objectManager.objectMap.lookup(key.getHash(), c);
    EXPECT_FALSE(c.isDone());
    EXPECT_EQ(reference.toInteger(), c.getReference());
}

TEST_F(ObjectManagerTest, replace_priorVersion) {
    Key key(1, "1", 1);

    Log::Reference firstRef = storeObject(key, "old", 0);
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        EXPECT_TRUE(objectManager.remove(lock, key));
        EXPECT_FALSE(objectManager.replace(lock, key, firstRef));
    }
    Log::Reference secondRef = storeObject(key, "new", 1);
    {
        ObjectManager::HashTableBucketLock lock(objectManager, key);
        EXPECT_TRUE(objectManager.replace(lock, key, secondRef));
    }

    HashTable::Candidates c;
    objectManager.objectMap.lookup(key.getHash(), c);
    EXPECT_FALSE(c.isDone());
    EXPECT_EQ(secondRef.toInteger(), c.getReference());
}

}  // namespace RAMCloud
