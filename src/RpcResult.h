/* Copyright (c) 2014-2015 Stanford University
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

#ifndef RAMCLOUD_RPCRESULT_H
#define RAMCLOUD_RPCRESULT_H

#include "Common.h"
#include "Buffer.h"
#include "Key.h"

namespace RAMCloud {

/**
 * This class defines the format of a linearizable RPC result stored in the log
 * and provides methods to easily construct new ones to be appended
 * and interpret ones that have already been written.
 *
 * In other words, this code centralizes the format and parsing of RpcResults
 * (essentially serialization and deserialization).
 *
 * During RC recovery of RpcResult log, a master inserts the log entry to
 * UnackedRpcResults.
 *
 * Each RpcResult contains a header and a variable length RPC response.
 * When stored in the log, a RpcResult has the following layout:
 *
 * +------------------+----------+
 * | RpcResult Header | Response |
 * +------------------+----------+
 */
class RpcResult {
  public:
    RpcResult(uint64_t tableId, KeyHash keyHash,
              uint64_t leaseId, uint64_t rpcId, uint64_t ackId,
              Buffer& respBuffer, uint32_t respOff = 0, uint32_t respLen = 0);
    RpcResult(uint64_t tableId, KeyHash keyHash,
              uint64_t leaseId, uint64_t rpcId, uint64_t ackId,
              const void* response, uint32_t respLen);
    explicit RpcResult(Buffer& buffer, uint32_t offset = 0,
                       uint32_t length = 0);

    void assembleForLog(Buffer& buffer);
    void assembleForLog(void* buffer);
    void appendRespToBuffer(Buffer& buffer);

    uint64_t getTableId();
    KeyHash getKeyHash();
    uint64_t getLeaseId();
    uint64_t getRpcId();
    uint64_t getAckId();

    const void* getResp(uint32_t *length = NULL);
    uint32_t getRespLength();

    bool checkIntegrity();
    uint32_t getSerializedLength();
    uint32_t computeChecksum();

    /**
     * This data structure defines the format of a RpcResult header stored in a
     * master server's log.
     */
    class Header {
      public:
        /**
         * Construct a serialized RpcResult header.
         *
         * \param tableId
         *      TableId for the object modified by this RPC.
         * \param keyHash
         *      KeyHash for the object modified by this RPC.
         * \param leaseId
         *      leaseId given for this linearizable RPC.
         * \param rpcId
         *      rpcId given for this linearizable RPC.
         * \param ackId
         *      ackId given for this linearizable RPC.
         */
        Header(uint64_t tableId, KeyHash keyHash,
               uint64_t leaseId, uint64_t rpcId, uint64_t ackId)
            : tableId(tableId),
              keyHash(keyHash),
              leaseId(leaseId),
              rpcId(rpcId),
              ackId(ackId),
              checksum(0)
        {
        }

        /// Table to which the object Modified by current RPC.
        /// A (TableId, KeyHash) tuple uniquely identifies the master that
        /// should execute this RPC.
        uint64_t tableId;

        /// Key hash value of the object modified by current RPC.
        KeyHash keyHash;

        /// leaseId given by the client that dispatched this RPC.
        uint64_t leaseId;

        /// RPC ID assigned by the client that dispatched this RPC.
        uint64_t rpcId;

        /// Acknowledgement given by the client that dispatched this RPC.
        uint64_t ackId;

        /// CRC32C checksum covering everything but this field, including the
        /// response.
        uint32_t checksum;

        /// Following this class will be the response.
        /// This member is only here to denote this.
        char response[0];
    } __attribute__((__packed__));
    static_assert(sizeof(Header) == 44,
        "Unexpected serialized RpcResult size");

    /// Copy of the RpcResult header that is in, or will be written to, the log.
    Header header;

    /// Length of the response corresponding to this RPC. This isn't stored in
    /// Header since it can be trivially computed as needed.
    uint32_t respLength;

    /// If the response for the completed rpc all lies in a single
    /// contiguous region of memory, this will point there, otherwise this
    /// will point to NULL.
    const void* response;

    /// If the response for the completed rpc is stored in a Buffer,
    /// this will point to that buffer, otherwise this points to NULL.
    Buffer* respBuffer;

    /// The byte offset in the respBuffer where response start
    uint32_t respOffset;

    DISALLOW_COPY_AND_ASSIGN(RpcResult);
};
} // namespace RAMCloud

#endif
