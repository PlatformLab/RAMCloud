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

/**
 * \file
 * Unit tests for BackupClient.
 */

#include <BackupClient.h>
#include <MockTransport.h>

#include <cppunit/extensions/HelperMacros.h>

namespace RAMCloud {

/**
 * Unit tests for BackupHost
 */
class BackupHostTest : public CppUnit::TestFixture {
    Service *service;
    MockTransport *transport;

    const std::string testMessage;
    uint32_t testMessageLen;
    DISALLOW_COPY_AND_ASSIGN(BackupHostTest); // NOLINT

    CPPUNIT_TEST_SUITE(BackupHostTest);
    CPPUNIT_TEST(test_writeSegment_normal);
    // TODO(aravindn): Find a way to fix this test.
    // CPPUNIT_TEST(test_getSegmentMetadata_normal);
    CPPUNIT_TEST_SUITE_END();

  public:
    BackupHostTest()
            : service(0), transport(0), testMessage("God hates ponies."),
              testMessageLen(0)
    {
        testMessageLen = static_cast<uint32_t>(testMessage.length());
    }

    /**
     * Mimick the RPC requests/responses of the BackupServer to sanity
     * check that the backup clients are obeying the RPC protocol
     *
     * This class really does very little except assert that the
     * arguments look good and return something plausible as a
     * response for the client to sanity check.
     */
    /*
    class BackupHostMockNet : public MockNet {
      public:
        virtual void Handle(const char *reqData, size_t len) {
            char respBuffer[1024];
            backup_rpc *resp = static_cast<backup_rpc *>(&respBuffer[0]);

            const backup_rpc *req =
                static_cast<const backup_rpc *>(reqData);
            switch ((enum backup_rpc_type) req->hdr.type) {
            case BACKUP_RPC_WRITE_REQ:
                assert(10 == req->write_req.seg_num);
                assert(20 == req->write_req.off);
                assert(5 == req->write_req.len);
                resp->hdr.type = BACKUP_RPC_WRITE_RESP;
                resp->hdr.len = (uint32_t) BACKUP_RPC_WRITE_RESP_LEN;
                break;
            case BACKUP_RPC_HEARTBEAT_REQ:
            case BACKUP_RPC_COMMIT_REQ:
            case BACKUP_RPC_FREE_REQ:
            case BACKUP_RPC_GETSEGMENTLIST_REQ:
            case BACKUP_RPC_GETSEGMENTMETADATA_REQ:
                assert(10 == req->getsegmentmetadata_req.seg_num);
                resp->getsegmentmetadata_resp.list_count = 1;
                RecoveryObjectMetadata meta;
                meta.key = 1;
                meta.table = 2;
                meta.version = 3;
                meta.offset = 4;
                meta.length = 5;
                resp->getsegmentmetadata_resp.list[0] = meta;
                resp->hdr.type = BACKUP_RPC_GETSEGMENTMETADATA_RESP;
                resp->hdr.len = static_cast<uint32_t>(
                    BACKUP_RPC_GETSEGMENTMETADATA_RESP_LEN_WODATA +
                    sizeof(RecoveryObjectMetadata) *
                    resp->getsegmentmetadata_resp.list_count);
                break;
            case BACKUP_RPC_RETRIEVE_REQ:
            case BACKUP_RPC_HEARTBEAT_RESP:
            case BACKUP_RPC_WRITE_RESP:
            case BACKUP_RPC_COMMIT_RESP:
            case BACKUP_RPC_FREE_RESP:
            case BACKUP_RPC_GETSEGMENTLIST_RESP:
            case BACKUP_RPC_GETSEGMENTMETADATA_RESP:
            case BACKUP_RPC_RETRIEVE_RESP:
            case BACKUP_RPC_ERROR_RESP:
            default:
                break;
            }

            respBuf = &respBuffer[0];
            respLen = resp->hdr.len;
        }
    };
    */
    void
    setUp()
    {
        // The Service object will be deallocated by BackupHost.
        service = new Service();
        transport = new MockTransport();
    }

    void
    tearDown()
    {
        delete transport;
    }

    /**
     * Sanity check that the RPC request/response for writeSegment is
     * well formed
     */
    void
    test_writeSegment_normal()
    {
        BackupHost host(service, transport);

        const char *data = "junk";
        const int32_t len = static_cast<uint32_t>(strlen(data)) + 1;
        host.writeSegment(10, 20, data, len);
    }

    /**
     * Sanity check that the RPC request/response for
     * getSegmentMetadata well formed
     */
    void
    test_getSegmentMetadata_normal()
    {
        BackupHost host(service, transport);

        uint32_t metasSize = 4;
        RecoveryObjectMetadata metas[4];
        metasSize = host.getSegmentMetadata(10, &metas[0], metasSize);
        CPPUNIT_ASSERT_EQUAL(1u, metasSize);
        for (uint32_t i = 0; i < metasSize; i++) {
            CPPUNIT_ASSERT_EQUAL(1ul, metas[i].key);
            CPPUNIT_ASSERT_EQUAL(2ul, metas[i].table);
            CPPUNIT_ASSERT_EQUAL(3ul, metas[i].version);
            CPPUNIT_ASSERT_EQUAL(4ul, metas[i].offset);
            CPPUNIT_ASSERT_EQUAL(5ul, metas[i].length);
        }
    }

};
CPPUNIT_TEST_SUITE_REGISTRATION(BackupHostTest);

} // namespace RAMCloud
