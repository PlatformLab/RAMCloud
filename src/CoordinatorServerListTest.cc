/* Copyright (c) 2011 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
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
#include "CoordinatorServerList.h"

namespace RAMCloud {

class CoordinatorServerListTest : public ::testing::Test {
  public:
    CoordinatorServerListTest()
        : sl()
    {
    }

    CoordinatorServerList sl;

    DISALLOW_COPY_AND_ASSIGN(CoordinatorServerListTest);
};

TEST_F(CoordinatorServerListTest, add) {
    EXPECT_EQ(0U, sl.serverList.size());
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);
    EXPECT_EQ(ServerId(1, 0), sl.add("hi", true));
    EXPECT_TRUE(sl.serverList[1].entry);
    EXPECT_FALSE(sl.serverList[0].entry);
    EXPECT_EQ(1U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);
    EXPECT_EQ(ServerId(1, 0), sl.serverList[1].entry->serverId);
    EXPECT_EQ("hi", sl.serverList[1].entry->serviceLocator);
    EXPECT_TRUE(sl.serverList[1].entry->isMaster);
    EXPECT_FALSE(sl.serverList[1].entry->isBackup);
    EXPECT_EQ(1U, sl.serverList[1].nextGenerationNumber);

    EXPECT_EQ(ServerId(2, 0), sl.add("hi again", false));
    EXPECT_TRUE(sl.serverList[2].entry);
    EXPECT_EQ(ServerId(2, 0), sl.serverList[2].entry->serverId);
    EXPECT_EQ("hi again", sl.serverList[2].entry->serviceLocator);
    EXPECT_FALSE(sl.serverList[2].entry->isMaster);
    EXPECT_TRUE(sl.serverList[2].entry->isBackup);
    EXPECT_EQ(1U, sl.serverList[2].nextGenerationNumber);
    EXPECT_EQ(1U, sl.numberOfMasters);
    EXPECT_EQ(1U, sl.numberOfBackups);
}

TEST_F(CoordinatorServerListTest, remove) {
    EXPECT_THROW(sl.remove(ServerId(0, 0)), Exception);

    sl.add("hi!", true);
    EXPECT_NO_THROW(sl.remove(ServerId(1, 0)));
    EXPECT_FALSE(sl.serverList[1].entry);
    EXPECT_THROW(sl.remove(ServerId(1, 0)), Exception);
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);

    sl.add("hi, again", false);
    EXPECT_TRUE(sl.serverList[1].entry);
    EXPECT_THROW(sl.remove(ServerId(1, 2)), Exception);
    EXPECT_NO_THROW(sl.remove(ServerId(1, 1)));
    EXPECT_EQ(0U, sl.numberOfMasters);
    EXPECT_EQ(0U, sl.numberOfBackups);
}

TEST_F(CoordinatorServerListTest, indexOperator) {
    EXPECT_THROW(sl[ServerId(0, 0)], Exception);
    sl.add("yo!", true);
    EXPECT_EQ(ServerId(1, 0), sl[ServerId(1, 0)].serverId);
    EXPECT_EQ("yo!", sl[ServerId(1, 0)].serviceLocator);
    sl.remove(ServerId(1, 0));
    EXPECT_THROW(sl[ServerId(1, 0)], Exception);
}

TEST_F(CoordinatorServerListTest, contains) {
    EXPECT_FALSE(sl.contains(ServerId(0, 0)));
    EXPECT_FALSE(sl.contains(ServerId(1, 0)));

    sl.add("I love it when a plan comes together", false);
    EXPECT_TRUE(sl.contains(ServerId(1, 0)));

    sl.add("Come with me if you want to live", true);
    EXPECT_TRUE(sl.contains(ServerId(2, 0)));

    sl.remove(ServerId(1, 0));
    EXPECT_FALSE(sl.contains(ServerId(1, 0)));

    sl.remove(ServerId(2, 0));
    EXPECT_FALSE(sl.contains(ServerId(2, 0)));

    sl.add("I'm running out 80s shows and action movie quotes", false);
    EXPECT_TRUE(sl.contains(ServerId(1, 1)));
}

TEST_F(CoordinatorServerListTest, nextMasterIndex) {
    EXPECT_EQ(-1U, sl.nextMasterIndex(0));
    sl.add("", false);
    sl.add("", true);
    sl.add("", false);
    sl.add("", false);
    sl.add("", true);
    sl.add("", false);

    EXPECT_EQ(2U, sl.nextMasterIndex(0));
    EXPECT_EQ(2U, sl.nextMasterIndex(2));
    EXPECT_EQ(5U, sl.nextMasterIndex(3));
    EXPECT_EQ(-1U, sl.nextMasterIndex(6));
}

TEST_F(CoordinatorServerListTest, nextBackupIndex) {
    EXPECT_EQ(-1U, sl.nextMasterIndex(0));
    sl.add("", true);
    sl.add("", false);
    sl.add("", true);

    EXPECT_EQ(2U, sl.nextBackupIndex(0));
    EXPECT_EQ(2U, sl.nextBackupIndex(2));
    EXPECT_EQ(-1U, sl.nextBackupIndex(3));
}

TEST_F(CoordinatorServerListTest, serialise) {
    {
        ProtoBuf::ServerList serverList;
        sl.serialise(serverList, false, false);
        EXPECT_EQ(0, serverList.server_size());
        sl.serialise(serverList, true, true);
        EXPECT_EQ(0, serverList.server_size());
    }

    ServerId first = sl.add("", true);
    sl.add("", true);
    sl.add("", true);
    sl.add("", false);
    sl.remove(first);       // ensure removed entries are skipped

    {
        ProtoBuf::ServerList serverList;
        sl.serialise(serverList, false, false);
        EXPECT_EQ(0, serverList.server_size());
        sl.serialise(serverList, true, false);
        EXPECT_EQ(2, serverList.server_size());
        EXPECT_TRUE(serverList.server(0).is_master());
        EXPECT_FALSE(serverList.server(0).is_backup());
        EXPECT_TRUE(serverList.server(1).is_master());
        EXPECT_FALSE(serverList.server(1).is_backup());
    }

    {
        ProtoBuf::ServerList serverList;
        sl.serialise(serverList, false, true);
        EXPECT_EQ(1, serverList.server_size());
        EXPECT_FALSE(serverList.server(0).is_master());
        EXPECT_TRUE(serverList.server(0).is_backup());
    }

    {
        ProtoBuf::ServerList serverList;
        sl.serialise(serverList, true, true);
        EXPECT_EQ(3, serverList.server_size());
        EXPECT_TRUE(serverList.server(0).is_master());
        EXPECT_FALSE(serverList.server(0).is_backup());
        EXPECT_TRUE(serverList.server(1).is_master());
        EXPECT_FALSE(serverList.server(1).is_backup());
        EXPECT_FALSE(serverList.server(2).is_master());
        EXPECT_TRUE(serverList.server(2).is_backup());
    }
}

TEST_F(CoordinatorServerListTest, firstFreeIndex) {
    EXPECT_EQ(0U, sl.serverList.size());
    EXPECT_EQ(1U, sl.firstFreeIndex());
    EXPECT_EQ(2U, sl.serverList.size());
    sl.add("hi", true);
    EXPECT_EQ(2U, sl.firstFreeIndex());
    sl.add("hi again", true);
    EXPECT_EQ(3U, sl.firstFreeIndex());
    sl.remove(ServerId(2, 0));
    EXPECT_EQ(2U, sl.firstFreeIndex());
    sl.remove(ServerId(1, 0));
    EXPECT_EQ(1U, sl.firstFreeIndex());
}

TEST_F(CoordinatorServerListTest, getReferenceFromServerId) {
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(0, 0)), Exception);
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(1, 0)), Exception);

    sl.add("", true);
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(0, 0)), Exception);
    EXPECT_NO_THROW(sl.getReferenceFromServerId(ServerId(1, 0)));
    EXPECT_THROW(sl.getReferenceFromServerId(ServerId(2, 0)), Exception);
}

TEST_F(CoordinatorServerListTest, getPointerFromIndex) {
    EXPECT_THROW(sl.getPointerFromIndex(0), Exception);
    EXPECT_THROW(sl.getPointerFromIndex(1), Exception);

    sl.add("", true);
    EXPECT_EQ(static_cast<const CoordinatorServerList::Entry*>(NULL),
        sl.getPointerFromIndex(0));
    EXPECT_EQ(static_cast<const CoordinatorServerList::Entry*>(
        sl.serverList[1].entry.get()), sl.getPointerFromIndex(1));
    EXPECT_THROW(sl.getPointerFromIndex(2), Exception);

    sl.remove(ServerId(1, 0));
    EXPECT_EQ(static_cast<const CoordinatorServerList::Entry*>(NULL),
        sl.getPointerFromIndex(1));
}

TEST_F(CoordinatorServerListTest, Entry_constructor) {
    CoordinatorServerList::Entry a(ServerId(52, 374),
        "You forgot your boarding pass", true);
    EXPECT_EQ(ServerId(52, 374), a.serverId);
    EXPECT_EQ("You forgot your boarding pass", a.serviceLocator);
    EXPECT_TRUE(a.isMaster);
    EXPECT_FALSE(a.isBackup);
    EXPECT_EQ(static_cast<ProtoBuf::Tablets*>(NULL), a.will);
    EXPECT_EQ(0U, a.backupReadMegsPerSecond);

    CoordinatorServerList::Entry b(ServerId(27, 72),
        "I ain't got time to bleed", false);
    EXPECT_EQ(ServerId(27, 72), b.serverId);
    EXPECT_EQ("I ain't got time to bleed", b.serviceLocator);
    EXPECT_FALSE(b.isMaster);
    EXPECT_TRUE(b.isBackup);
    EXPECT_EQ(static_cast<ProtoBuf::Tablets*>(NULL), b.will);
    EXPECT_EQ(0U, b.backupReadMegsPerSecond);
}

static bool
compareEntries(CoordinatorServerList::Entry& a,
               CoordinatorServerList::Entry& b)
{
    // If this trips, you need to update some checks below.
    EXPECT_EQ(40U, sizeof(a));

    if (a.serverId != b.serverId)
        return false;
    if (a.serviceLocator != b.serviceLocator)
        return false;
    if (a.isMaster != b.isMaster)
        return false;
    if (a.isBackup != b.isBackup)
        return false;
    if (a.will != b.will)
        return false;
    if (a.backupReadMegsPerSecond != b.backupReadMegsPerSecond)
        return false;

    return true;
}

TEST_F(CoordinatorServerListTest, Entry_copyConstructor) {
    CoordinatorServerList::Entry source(ServerId(234, 273), "hi!", false);
    source.backupReadMegsPerSecond = 57;
    source.will = reinterpret_cast<ProtoBuf::Tablets*>(0x11deadbeef22UL);
    CoordinatorServerList::Entry dest(source);
    EXPECT_TRUE(compareEntries(source, dest));
}

TEST_F(CoordinatorServerListTest, Entry_assignmentOperator) {
    CoordinatorServerList::Entry source(ServerId(73, 72), "hi", false);
    source.backupReadMegsPerSecond = 785;
    source.will = reinterpret_cast<ProtoBuf::Tablets*>(0x11beefcafe22UL);
    CoordinatorServerList::Entry dest(ServerId(0, 0), "bye", true);
    dest = source;
    EXPECT_TRUE(compareEntries(source, dest));
}

TEST_F(CoordinatorServerListTest, Entry_serialise) {
    CoordinatorServerList::Entry entry(ServerId(0, 0), "", false);
    entry.isMaster = false;
    entry.isBackup = true;
    entry.serverId = ServerId(5234, 23482);
    entry.serviceLocator = "giggity";
    entry.backupReadMegsPerSecond = 723;

    ProtoBuf::ServerList_Entry serialEntry;
    entry.serialise(serialEntry);
    EXPECT_FALSE(serialEntry.is_master());
    EXPECT_TRUE(serialEntry.is_backup());
    EXPECT_EQ(ServerId(5234, 23482).getId(), serialEntry.server_id());
    EXPECT_EQ("giggity", serialEntry.service_locator());
    EXPECT_EQ(723U, serialEntry.user_data());

    entry.isMaster = true;
    entry.isBackup = false;
    ProtoBuf::ServerList_Entry serialEntry2;
    entry.serialise(serialEntry2);
    EXPECT_TRUE(serialEntry2.is_master());
    EXPECT_FALSE(serialEntry2.is_backup());
    EXPECT_EQ(0U, serialEntry2.user_data());
}

}  // namespace RAMCloud
