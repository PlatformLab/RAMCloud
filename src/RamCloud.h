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

#ifndef RAMCLOUD_RAMCLOUD_H
#define RAMCLOUD_RAMCLOUD_H

#include "Common.h"
#include "CoordinatorClient.h"
#include "MasterClient.h"
#include "ObjectFinder.h"
#include "ServerMetrics.h"

namespace RAMCloud {

/**
 * The RamCloud class provides the primary interface used by applications to
 * access a RAMCloud cluster.
 *
 * Each RamCloud object provides access to a particular RAMCloud cluster;
 * all of the RAMCloud RPC requests appear as methods on this object.
 */
class RamCloud {
  public:
    /// An asynchronous version of #create().
    class Create {
      public:
        /// Start a create RPC. See RamCloud::create.
        Create(RamCloud& ramCloud,
               uint32_t tableId, const void* buf, uint32_t length,
               uint64_t* version = NULL, bool async = false)
            : constructorContext(ramCloud.clientContext)
            , ramCloud(ramCloud)
            , master(ramCloud.objectFinder.lookupHead(tableId))
            , masterCreate(master, tableId, buf, length, version, async)
        {
            // This should be the last line on all return paths of this
            // constructor.
            constructorContext.leave();
        }
        bool isReady() {
            Context::Guard _(ramCloud.clientContext);
            return masterCreate.isReady();
        }
        /// Wait for the create RPC to complete.
        uint64_t operator()() {
            Context::Guard _(ramCloud.clientContext);
            return masterCreate();
        }
      private:
        /// Analogous to RamCloud::constructorContext.
        Context::Guard constructorContext;
        RamCloud& ramCloud;
        MasterClient master;
        MasterClient::Create masterCreate;
        DISALLOW_COPY_AND_ASSIGN(Create);
    };

    /// An asynchronous version of #read().
    class Read {
      public:
        /// Start a read RPC. See RamCloud::read.
        Read(RamCloud& ramCloud,
             uint32_t tableId, uint64_t id, Buffer* value,
             const RejectRules* rejectRules = NULL,
             uint64_t* version = NULL)
            : constructorContext(ramCloud.clientContext)
            , ramCloud(ramCloud)
            , master(ramCloud.objectFinder.lookup(tableId, id))
            , masterRead(master, tableId, id, value, rejectRules, version)
        {
            // This should be the last line on all return paths of this
            // constructor.
            constructorContext.leave();
        }
        void cancel() {
            Context::Guard _(ramCloud.clientContext);
            masterRead.cancel();
        }
        bool isReady() {
            Context::Guard _(ramCloud.clientContext);
            return masterRead.isReady();
        }
        /// Wait for the read RPC to complete.
        void operator()() {
            Context::Guard _(ramCloud.clientContext);
            masterRead();
        }
      private:
        /// Analogous to RamCloud::constructorContext.
        Context::Guard constructorContext;
        RamCloud& ramCloud;
        MasterClient master;
        MasterClient::Read masterRead;
        DISALLOW_COPY_AND_ASSIGN(Read);
    };

    /// An asynchronous version of #write().
    class Write {
      public:
        /// Start a write RPC. See RamCloud::write.
        Write(RamCloud& ramCloud,
              uint32_t tableId, uint64_t id, Buffer& buffer,
              const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL, bool async = false)
            : constructorContext(ramCloud.clientContext)
            , ramCloud(ramCloud)
            , master(ramCloud.objectFinder.lookup(tableId, id))
            , masterWrite(master, tableId, id, buffer,
                          rejectRules, version, async)
        {
            // This should be the last line on all return paths of this
            // constructor.
            constructorContext.leave();
        }
        /// Start a write RPC. See RamCloud::write.
        Write(RamCloud& ramCloud,
              uint32_t tableId, uint64_t id, const void* buf,
              uint32_t length, const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL, bool async = false)
            : constructorContext(ramCloud.clientContext)
            , ramCloud(ramCloud)
            , master(ramCloud.objectFinder.lookup(tableId, id))
            , masterWrite(master, tableId, id, buf, length,
                          rejectRules, version, async)
        {
            // This should be the last line on all return paths of this
            // constructor.
            constructorContext.leave();
        }
        bool isReady() {
            Context::Guard _(ramCloud.clientContext);
            return masterWrite.isReady();
        }
        /// Wait for the write RPC to complete.
        void operator()() {
            Context::Guard _(ramCloud.clientContext);
            masterWrite();
        }
      private:
        /// Analogous to RamCloud::constructorContext.
        Context::Guard constructorContext;
        RamCloud& ramCloud;
        MasterClient master;
        MasterClient::Write masterWrite;
        DISALLOW_COPY_AND_ASSIGN(Write);
    };

    explicit RamCloud(const char* serviceLocator);
    RamCloud(Context& context, const char* serviceLocator);
    void createTable(const char* name);
    void dropTable(const char* name);
    uint32_t openTable(const char* name);
    uint64_t create(uint32_t tableId, const void* buf, uint32_t length,
                    uint64_t* version = NULL, bool async = false);
    string* getServiceLocator();
    ServerMetrics getMetrics(const char* serviceLocator);
    ServerMetrics getMetrics(uint32_t table, uint64_t objectId);
    uint64_t ping(const char* serviceLocator, uint64_t nonce,
                  uint64_t timeoutNanoseconds);
    uint64_t ping(uint32_t table, uint64_t objectId, uint64_t nonce,
                  uint64_t timeoutNanoseconds);
    uint64_t proxyPing(const char* serviceLocator1,
                       const char* serviceLocator2,
                       uint64_t timeoutNanoseconds1,
                       uint64_t timeoutNanoseconds2);
    void read(uint32_t tableId, uint64_t id, Buffer* value,
              const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL);
    void multiRead(MasterClient::ReadObject* requests[], uint32_t numRequests);
    void remove(uint32_t tableId, uint64_t id,
                const RejectRules* rejectRules = NULL,
                uint64_t* version = NULL);
    void write(uint32_t tableId, uint64_t id, const void* buf,
               uint32_t length, const RejectRules* rejectRules = NULL,
               uint64_t* version = NULL, bool async = false);
    void write(uint32_t tableId, uint64_t id, const char* s);

  PRIVATE:
    /**
     * Service locator for the cluster coordinator.
     */
    string coordinatorLocator;

    /**
     * Usually, RamCloud objects create a new context in which to run. This is
     * the location where that context is stored.
     */
    Tub<Context> realClientContext;

    /**
     * This usually refers to realClientContext. For testing purposes and
     * clients that want to provide their own context that they've mucked with,
     * this refers to an externally defined context.
     */
    Context& clientContext;

    /**
     * This should only be used in the constructor. This guard sets the context
     * within the constructor, both during the initializer list and during the
     * code in the constructor.
     */
    Context::Guard constructorContext;
  public:

    /// \copydoc Client::status
    Status status;

  public: // public for now to make administrative calls from clients
    CoordinatorClient coordinator;
    ObjectFinder objectFinder;

  private:
    DISALLOW_COPY_AND_ASSIGN(RamCloud);
};
} // namespace RAMCloud

#endif // RAMCLOUD_RAMCLOUD_H
