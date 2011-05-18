
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

#ifndef RAMCLOUD_MASTERCLIENT_H
#define RAMCLOUD_MASTERCLIENT_H

#include "Client.h"
#include "Common.h"
#include "CoordinatorClient.h"
#include "Transport.h"
#include "Buffer.h"
#include "Tub.h"

namespace RAMCloud {

/**
 * TODO
 */
class MasterClient : public Client {
  public:

    /*
    * Format for requesting a read of an object as a part of multiRead 
    */
    struct ReadObject {
        /*
        * The table containing the desired object (return value from
        * a previous call to openTable).
        */
        uint32_t tableId;
        /*
        * Identifier within tableId of the object to be read.
        */
        uint64_t id;
        /*
        * If the read for this object was successful, the Tub<Buffer>
        * will hold the contents of the desired object. If not, it will
        * not be initialized, giving "false" when the buffer is tested.
        */
        Tub<Buffer>* value;
        /*
        * The version number of the object is returned here
        */
        uint64_t* version;
        /*
        * The status of read (either that the read succeeded, or the 
        * error in case it didn't) is returned here.
        */
        Status* status;

        ReadObject(uint32_t tableId, uint64_t id, Tub<Buffer>* value,
                   uint64_t* version, Status* status)
            : tableId(tableId)
            , id(id)
            , value(value)
            , version(version)
            , status(status)
        {
        };

        ReadObject()
            : tableId()
            , id()
            , value()
            , version()
            , status()
        {
        };
    };

    /// An asynchronous version of #create().
    class Create {
      public:
        Create(MasterClient& client,
               uint32_t tableId, const void* buf, uint32_t length,
               uint64_t* version = NULL, bool async = false);
        bool isReady() { return client.isReady(state); }
        uint64_t operator()();
      private:
        MasterClient& client;
        uint64_t* version;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(Create);
    };

    class Recover {
      public:
        Recover(MasterClient& client,
                uint64_t masterId, uint64_t partitionId,
                const ProtoBuf::Tablets& tablets,
                const char* backups, uint32_t backupsLen);
        bool isReady() { return client.isReady(state); }
        void operator()();
      private:
        MasterClient& client;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(Recover);
    };

    /// An asynchronous version of #write().
    class Write {
      public:
        Write(MasterClient& client,
              uint32_t tableId, uint64_t id, const void* buf,
              uint32_t length, const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL, bool async = false);
        bool isReady() { return client.isReady(state); }
        void operator()();
      private:
        MasterClient& client;
        uint64_t* version;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(Write);
    };

    /**
     * Report to a master that a particular backup has failed so that it
     * can rereplicate any segments that might have been stored there.
     *
     * See MasterServer::rereplicateSegments().
     */
    class RereplicateSegments {
      public:
        RereplicateSegments(MasterClient& client,
                            uint64_t backupId);
        bool isReady() { return client.isReady(state); }
        void operator()();
      private:
        MasterClient& client;
        uint64_t backupId;
        Buffer requestBuffer;
        Buffer responseBuffer;
        AsyncState state;
        DISALLOW_COPY_AND_ASSIGN(RereplicateSegments);
    };

    explicit MasterClient(Transport::SessionRef session) : session(session) {}
    uint64_t create(uint32_t tableId, const void* buf, uint32_t length,
                    uint64_t* version = NULL, bool async = false);
    void fillWithTestData(uint32_t numObjects, uint32_t objectSize);
    void ping();
    void read(uint32_t tableId, uint64_t id, Buffer* value,
              const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL);
    void multiRead(std::vector<ReadObject> requests);
    void recover(uint64_t masterId, uint64_t partitionId,
                 const ProtoBuf::Tablets& tablets,
                 const ProtoBuf::ServerList& backups);
    void recover(uint64_t masterId, uint64_t partitionId,
                 const ProtoBuf::Tablets& tablets,
                 const char* backups, uint32_t backupsLen);
    void remove(uint32_t tableId, uint64_t id,
                const RejectRules* rejectRules = NULL,
                uint64_t* version = NULL);
    void setTablets(const ProtoBuf::Tablets& tablets);
    void write(uint32_t tableId, uint64_t id, const void* buf,
               uint32_t length, const RejectRules* rejectRules = NULL,
               uint64_t* version = NULL, bool async = false);

  protected:
    Transport::SessionRef session;
    DISALLOW_COPY_AND_ASSIGN(MasterClient);
};
} // namespace RAMCloud

#endif // RAMCLOUD_MASTERCLIENT_H
