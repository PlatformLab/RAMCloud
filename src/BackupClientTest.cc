/* Copyright (c) 2010 Stanford University
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

#include "BackupClient.h"
#include "MockTransport.h"
#include "TransportManager.h"

namespace RAMCloud {

class BackupHostTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(BackupHostTest);
    CPPUNIT_TEST(test_commitSegment);
    CPPUNIT_TEST(test_freeSegment);
    CPPUNIT_TEST(test_getRecoveryData);
    CPPUNIT_TEST(test_openSegment);
    CPPUNIT_TEST(test_startReadingData);
    CPPUNIT_TEST(test_writeSegment);
    CPPUNIT_TEST_SUITE_END();

    BackupHost* backup;
    MockTransport* transport;

  public:
    BackupHostTest()
        : backup(NULL)
        , transport(NULL)
    {
    }

    void
    setUp()
    {
        transport = new MockTransport();
        transportManager.registerMock(transport);
        backup = new BackupHost(transportManager.getSession("mock:"));
    }

    void
    tearDown()
    {
        delete backup;
        transportManager.unregisterMock();
        delete transport;
    }

    void
    test_commitSegment()
    {
        transport->setInput("0 0");
        backup->commitSegment(99, 88);
        CPPUNIT_ASSERT_EQUAL("clientSend: 128 0 99 0 88 0",
                             transport->outputLog);
    }

    void
    test_freeSegment()
    {
        transport->setInput("0 0");
        backup->freeSegment(99, 88);
        CPPUNIT_ASSERT_EQUAL("clientSend: 129 0 99 0 88 0",
                             transport->outputLog);
    }

    void
    test_getRecoveryData()
    {
        transport->setInput("0 0 0");
        backup->getRecoveryData(99, TabletMap());
        CPPUNIT_ASSERT_EQUAL("clientSend: 130 0 99 0",
                             transport->outputLog);
    }

    void
    test_openSegment()
    {
        transport->setInput("0 0");
        backup->openSegment(99, 88);
        CPPUNIT_ASSERT_EQUAL("clientSend: 131 0 99 0 88 0",
                             transport->outputLog);
    }

    void
    test_startReadingData()
    {
        transport->setInput("0 0 3 99 0 88 0 77 0");
        vector<uint64_t> segmentIds = backup->startReadingData(99);
        CPPUNIT_ASSERT_EQUAL("clientSend: 132 0 99 0",
                             transport->outputLog);
        CPPUNIT_ASSERT_EQUAL(3, segmentIds.size());
        CPPUNIT_ASSERT_EQUAL(99, segmentIds[0]);
        CPPUNIT_ASSERT_EQUAL(88, segmentIds[1]);
        CPPUNIT_ASSERT_EQUAL(77, segmentIds[2]);
    }

    void
    test_writeSegment()
    {
        transport->setInput("0 0");
        backup->writeSegment(99, 88, 77, "test", 4);
        CPPUNIT_ASSERT_EQUAL("clientSend: 133 0 99 0 88 0 77 4 test",
                             transport->outputLog);
    }

    DISALLOW_COPY_AND_ASSIGN(BackupHostTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BackupHostTest);

} // namespace RAMCloud
