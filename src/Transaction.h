/* Copyright (c) 2015 Stanford University
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

#ifndef RAMCLOUD_TRANSACTION_H
#define RAMCLOUD_TRANSACTION_H

#include <list>
#include <map>
#include <memory>

#include "Common.h"
#include "MultiRead.h"
#include "RamCloud.h"

namespace RAMCloud {

class ClientTransactionTask;

/**
 * The Transaction class provides a client level interface for transactionally
 * performing a set of read, write, and remove operations.  A client would:
 *   1.  construct a Transaction object (*t*),
 *   2.  optimistically perform operations on *t*, and then
 *   3.  attempt to commit the transaction by calling commit on *t*.
 *
 * Transaction commit calls will either:
 *   -   **commit** in which case all performed operations are committed in an
 *       atomic isolated manner; or
 *   -   **abort** in which case effectively no operations are performed.
 * It is the client's responsibility to check the return value of the commit
 * call and retry the transaction upon abort.
 *
 * Each Transaction object represents a single transaction attempt.  Transaction
 * objects should be discarded after the transaction either commits or aborts;
 * a single Transaction object is not intended to be reused to represent
 * multiple transaction attempts.
 */
class Transaction {
  PRIVATE:
    /// Forward declaration
    struct ReadBatch;

  PUBLIC:
    explicit Transaction(RamCloud* ramcloud);

    bool commit();
    void sync();
    bool commitAndSync();

    void read(uint64_t tableId, const void* key, uint16_t keyLength,
            Buffer* value);

    void remove(uint64_t tableId, const void* key, uint16_t keyLength);

    void write(uint64_t tableId, const void* key, uint16_t keyLength,
            const void* buf, uint32_t length);

    /**
     * Encapsulates the state of a Transaction::read operation,
     * allowing it to execute asynchronously.
     */
    class ReadOp {
      public:
        ReadOp(Transaction* transaction, uint64_t tableId, const void* key,
               uint16_t keyLength, Buffer* value, bool batch = false);
        bool isReady();
        void wait();

      PRIVATE:
        Transaction* transaction;       /// Pointer to associated transaction.
        uint64_t tableId;               /// TableId of object to be read.
        Buffer keyBuf;                  /// Contains key of object to be read.
        uint16_t keyLength;             /// Length of key of object to be read.
        Buffer* value;                  /// Return value of the read.
        Tub<ObjectBuffer> buf;          /// Scratch buffer for read rpc.

        bool requestBatched;            /// True if operation should be batched.

        /// Request data for non-batched request.
        struct SingleRequest{
            SingleRequest()
                : readRpc()
            {}

            /// If the value is already cached this rpc is unused.
            Tub<ReadKeysAndValueRpc> readRpc;
        };
        Tub<SingleRequest> singleRequest;   // Use Tub to prevent misuse.

        /// Request data for batched request.
        struct BatchedRequest{
            BatchedRequest()
                : readBatchPtr()
                , request()
            {}

            /// If the value is already cached this batch is unused.
            std::shared_ptr<ReadBatch> readBatchPtr;
            /// Index into the batch where this request is stored.
            MultiReadObject request;
        };
        Tub<BatchedRequest> batchedRequest; // Use Tub to prevent misuse.

        DISALLOW_COPY_AND_ASSIGN(ReadOp);
    };

  PRIVATE:
    /// Overall client state information.
    RamCloud* ramcloud;

    /// Pointer to the dynamically allocated transaction task that represents
    /// that transaction.
    std::shared_ptr<ClientTransactionTask> taskPtr;

    /// Keeps track of whether commit has already been called to preclude
    /// subsequent read, remove, write, and commit calls.
    bool commitStarted;

    /// Keeps a batch of batch allowed ReadOps organized with its supporting
    /// MultiRead rpc.
    struct ReadBatch {
        ReadBatch()
            : rpc()
            , requests()
        {}

        Tub<MultiRead> rpc;
        std::vector<MultiReadObject*> requests;
    };

    /// Points to the next batch of ReadOps to be sent.
    std::shared_ptr<ReadBatch> nextReadBatchPtr;

    DISALLOW_COPY_AND_ASSIGN(Transaction);
};

} // end RAMCloud

#endif  /* RAMCLOUD_TRANSACTION_H */

