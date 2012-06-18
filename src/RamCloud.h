/* Copyright (c) 2010-2012 Stanford University
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
 *
 * In multi-threaded clients there must be a separate RamCloud object for
 * each thread; as of 5/2012 these objects are not thread-safe.
 */
class RamCloud {
  public:
    /// An asynchronous version of #read().
    class Read {
      public:
        /// Start a read RPC. See RamCloud::read.
        Read(RamCloud& ramCloud,
             uint64_t tableId, const char* key, uint16_t keyLength,
             Buffer* value, const RejectRules* rejectRules = NULL,
             uint64_t* version = NULL)
             : ramCloud(ramCloud)
             , master(ramCloud.objectFinder.lookup(tableId,
                                                   key, keyLength))
             , masterRead(master, tableId, key, keyLength, value,
                          rejectRules, version)
        {
        }
        void cancel() {
            masterRead.cancel();
        }
        bool isReady() {
            return masterRead.isReady();
        }
        /// Wait for the read RPC to complete.
        void operator()() {
            masterRead();
        }
      private:
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
              uint64_t tableId, const char* key, uint16_t keyLength,
              Buffer& buffer,
              const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL, bool async = false)
              : ramCloud(ramCloud)
              , master(ramCloud.objectFinder.lookup(tableId,
                       key, keyLength))
              , masterWrite(master, tableId, key, keyLength, buffer,
                            rejectRules, version, async)
        {
        }
        /// Start a write RPC. See RamCloud::write.
        Write(RamCloud& ramCloud,
              uint64_t tableId, const char* key, uint16_t keyLength,
              const void* buf, uint32_t length,
              const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL, bool async = false)
              : ramCloud(ramCloud)
              , master(ramCloud.objectFinder.lookup(tableId,
                       key, keyLength))
              , masterWrite(master, tableId, key, keyLength, buf, length,
                            rejectRules, version, async)
        {
        }
        bool isReady() {
            return masterWrite.isReady();
        }
        /// Wait for the write RPC to complete.
        void operator()() {
            masterWrite();
        }
      private:
        RamCloud& ramCloud;
        MasterClient master;
        MasterClient::Write masterWrite;
        DISALLOW_COPY_AND_ASSIGN(Write);
    };

    explicit RamCloud(const char* serviceLocator);
    RamCloud(Context& context, const char* serviceLocator);
    void createTable(const char* name, uint32_t serverSpan = 1);
    void dropTable(const char* name);
    void splitTablet(const char* name, uint64_t startKeyHash,
                   uint64_t endKeyHash, uint64_t splitKeyHash);
    uint64_t getTableId(const char* name);
    string* getServiceLocator();
    ServerMetrics getMetrics(const char* serviceLocator);
    ServerMetrics getMetrics(uint64_t table, const char* key,
                             uint16_t keyLength);
    uint64_t ping(const char* serviceLocator, uint64_t nonce,
                  uint64_t timeoutNanoseconds);
    uint64_t ping(uint64_t table, const char* key, uint16_t keyLength,
                  uint64_t nonce, uint64_t timeoutNanoseconds);
    uint64_t proxyPing(const char* serviceLocator1,
                       const char* serviceLocator2,
                       uint64_t timeoutNanoseconds1,
                       uint64_t timeoutNanoseconds2);
    void read(uint64_t tableId, const char* key, uint16_t keyLength,
              Buffer* value, const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL);
    void increment(uint64_t tableId, const char* key, uint16_t keyLength,
              int64_t incrementValue, const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL, int64_t* newValue = NULL);
    void multiRead(MasterClient::ReadObject* requests[], uint32_t numRequests);
    void remove(uint64_t tableId, const char* key, uint16_t keyLength,
                const RejectRules* rejectRules = NULL,
                uint64_t* version = NULL);
    void write(uint64_t tableId, const char* key, uint16_t keyLength,
               const void* buf, uint32_t length,
               const RejectRules* rejectRules = NULL,
               uint64_t* version = NULL, bool async = false);
    void write(uint64_t tableId, const char* key, uint16_t keyLength,
               const char* s);

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
