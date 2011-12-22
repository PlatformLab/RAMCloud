/* Copyright (c) 2011 Stanford University
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
#include "BindTransport.h"
#include "MembershipClient.h"
#include "MembershipService.h"
#include "ServerList.h"
#include "ServerList.pb.h"
#include "ServerListBuilder.h"
#include "ShortMacros.h"
#include "TransportManager.h"

namespace RAMCloud {

class MembershipServiceTest : public ::testing::Test {
  public:
    MembershipService* service;
    BindTransport* transport;
    MembershipClient* client;

    MembershipServiceTest()
        : service(NULL)
        , transport(NULL)
        , client(NULL)
    {
        transport = new BindTransport();
        Context::get().transportManager->registerMock(transport);

        Context::get().serverList = new ServerList();
        service = new MembershipService();
        transport->addService(*service, "mock:host=member", MEMBERSHIP_SERVICE);
        client = new MembershipClient();
    }

    ~MembershipServiceTest() {
        delete client;
        Context::get().transportManager->unregisterMock();
        delete transport;
        delete service;
        delete Context::get().serverList;
        Context::get().serverList = NULL;
    }

    DISALLOW_COPY_AND_ASSIGN(MembershipServiceTest);
};

static bool
setServerListFilter(string s)
{
    return s == "setServerList";
}

TEST_F(MembershipServiceTest, setServerList_fromEmpty) {
    TestLog::Enable _(setServerListFilter);

    EXPECT_EQ(0U, Context::get().serverList->size());
    EXPECT_EQ(0U, Context::get().serverList->getVersion());

    ProtoBuf::ServerList wholeList;
    ServerListBuilder{wholeList}
        (true, false, *ServerId(1, 0), 0, "mock:host=one")
        (false, true, *ServerId(2, 0), 0, "mock:host=two");
    wholeList.set_version_number(0);
    client->setServerList("mock:host=member", wholeList);

    EXPECT_EQ(3U, Context::get().serverList->size());       // [0] is reserved
    EXPECT_EQ(ServerId(1, 0), (*Context::get().serverList)[1]);
    EXPECT_EQ(ServerId(2, 0), (*Context::get().serverList)[2]);
    EXPECT_EQ("mock:host=one",
        Context::get().serverList->getLocator(ServerId(1, 0)));
    EXPECT_EQ("mock:host=two",
        Context::get().serverList->getLocator(ServerId(2, 0)));
    EXPECT_EQ("setServerList: Got complete list of servers containing 2 "
        "entries (version number 0) | setServerList:   Adding server "
        "id 1 (locator \"mock:host=one\") | setServerList:   Adding "
        "server id 2 (locator \"mock:host=two\")", TestLog::get());
}

TEST_F(MembershipServiceTest, setServerList_overlap) {
    EXPECT_EQ(0U, Context::get().serverList->size());
    EXPECT_EQ(0U, Context::get().serverList->getVersion());

    // Set the initial list.
    ProtoBuf::ServerList initialList;
    ServerListBuilder{initialList}
        (true, false, *ServerId(1, 0), 0, "mock:host=one")
        (false, true, *ServerId(2, 0), 0, "mock:host=two");
    initialList.set_version_number(0);
    client->setServerList("mock:host=member", initialList);

    TestLog::Enable _(setServerListFilter);

    // Now issue a new list that partially overlaps.
    ProtoBuf::ServerList newerList;
    ServerListBuilder{newerList}
        (true, false, *ServerId(1, 5), 0, "mock:host=oneBeta")
        (false, true, *ServerId(2, 0), 0, "mock:host=two")
        (false, true, *ServerId(3, 0), 0, "mock:host=three");
    newerList.set_version_number(1);
    client->setServerList("mock:host=member", newerList);

    // We should now have (1, 5), (2, 0), and (3, 0) in our list.
    // (1, 0) was removed.
    EXPECT_EQ(4U, Context::get().serverList->size());       // [0] is reserved
    EXPECT_EQ(ServerId(1, 5), (*Context::get().serverList)[1]);
    EXPECT_EQ(ServerId(2, 0), (*Context::get().serverList)[2]);
    EXPECT_EQ(ServerId(3, 0), (*Context::get().serverList)[3]);
    EXPECT_EQ("mock:host=oneBeta",
        Context::get().serverList->getLocator(ServerId(1, 5)));
    EXPECT_EQ("mock:host=two",
        Context::get().serverList->getLocator(ServerId(2, 0)));
    EXPECT_EQ("mock:host=three",
        Context::get().serverList->getLocator(ServerId(3, 0)));
    EXPECT_EQ("setServerList: Got complete list of servers containing 3 "
        "entries (version number 1) | setServerList:   Removing server "
        "id 1 (locator \"mock:host=one\") | setServerList:   Adding "
        "server id 21474836481 (locator \"mock:host=oneBeta\") | "
        "setServerList:   Adding server id 3 (locator "
        "\"mock:host=three\")", TestLog::get());
}

static bool
updateServerListFilter(string s)
{
    return s == "updateServerList";
}

TEST_F(MembershipServiceTest, updateServerList_normal) {
    EXPECT_EQ(0U, Context::get().serverList->size());
    EXPECT_EQ(0U, Context::get().serverList->getVersion());

    // Set the initial list.
    ProtoBuf::ServerList initialList;
    ServerListBuilder{initialList}
        (true, false, *ServerId(1, 0), 0, "mock:host=one");
    initialList.set_version_number(0);
    client->setServerList("mock:host=member", initialList);

    TestLog::Enable _(updateServerListFilter);

    // Now issue an update.
    ProtoBuf::ServerList updateList;
    ServerListBuilder{updateList}
        (true, false, *ServerId(1, 0), 0, "mock:host=one", 0, false)
        (false, true, *ServerId(2, 0), 0, "mock:host=two");
    updateList.set_version_number(1);
    bool ret = client->updateServerList("mock:host=member", updateList);
    EXPECT_TRUE(ret);

    EXPECT_FALSE(Context::get().serverList->contains(ServerId(1, 0)));
    EXPECT_EQ("mock:host=two",
        Context::get().serverList->getLocator(ServerId(2, 0)));
    EXPECT_EQ("updateServerList: Got server list update (version number 1) "
        "| updateServerList:   Removing server id 1 (locator "
        "\"mock:host=one\") | updateServerList:   Adding server id 2 "
        "(locator \"mock:host=two\")", TestLog::get());
}

TEST_F(MembershipServiceTest, updateServerList_missedUpdate) {
    TestLog::Enable _(updateServerListFilter);

    ProtoBuf::ServerList updateList;
    ServerListBuilder{updateList}
        (true, false, *ServerId(1, 0), 0, "mock:host=one");
    updateList.set_version_number(57234);
    bool ret = client->updateServerList("mock:host=member", updateList);
    EXPECT_FALSE(ret);
    EXPECT_EQ("updateServerList: Update generation number is 57234, but last "
        "seen was 0. Something was lost! Grabbing complete list again!",
        TestLog::get());
}

// Paranoia: What happens if versions check out, but the udpate tells us to
// remove a server that isn't in our list. That's funky behaviour, but is it
// something worth crashing over?
TEST_F(MembershipServiceTest, updateServerList_versionOkButSomethingAmiss) {
    TestLog::Enable _(updateServerListFilter);

    ProtoBuf::ServerList updateList;
    ServerListBuilder{updateList}
        (true, false, *ServerId(1, 0), 0, "mock:host=one", 0, false);
    updateList.set_version_number(1);
    bool ret = client->updateServerList("mock:host=member", updateList);
    EXPECT_FALSE(ret);
    EXPECT_EQ("updateServerList: Got server list update (version number 1) | "
        "updateServerList:   Cannot remove server id 1: The server is not in "
        "our list, despite list version numbers matching (1). Something is "
        "screwed up! Requesting the entire list again.", TestLog::get());
}

}  // namespace RAMCloud
