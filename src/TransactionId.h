/* Copyright (c) 2015-2016 Stanford University
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

#ifndef RAMCLOUD_TRANSACTIONID_H
#define RAMCLOUD_TRANSACTIONID_H

namespace RAMCloud {

/**
 * Encapsulates the unique identifier for a specific transaction.
 */
struct TransactionId {
    /// Constructor for a TransactionId object.
    TransactionId(uint64_t clientLeaseId, uint64_t clientTransactionId)
        : clientLeaseId(clientLeaseId)
        , clientTransactionId(clientTransactionId)
    {}

    /// Equality operator; implemented to support use of TransactionId objects
    /// as a key in an std::unordered_map.
    bool operator==(const TransactionId &other) const {
        return (clientLeaseId == other.clientLeaseId
                && clientTransactionId == other.clientTransactionId);
    }

    /// Hash operator; implemented to support use of TransactionId objects
    /// as a key in an std::unordered_map.
    struct Hasher {
        std::size_t operator()(const TransactionId& txId) const {
            std::size_t h1 = std::hash<uint64_t>()(txId.clientLeaseId);
            std::size_t h2 = std::hash<uint64_t>()(txId.clientTransactionId);
            return h1 ^ (h2 << 1);
        }
    };

    /// Id of the client lease that issued this transaction.
    uint64_t clientLeaseId;
    /// Transaction Id given to this transaction by the client.  This value
    /// uniquely identifies this transaction among the transactions from the
    /// same client.  Combining this value with the clientLeaseId will provide
    /// a system wide unique transaction identifier.
    uint64_t clientTransactionId;
};

} // namespace RAMCloud

#endif // RAMCLOUD_TRANSACTIONID_H
