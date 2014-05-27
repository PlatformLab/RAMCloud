/* Copyright (c) 2014 Stanford University
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

#ifndef RAMCLOUD_INDEXLOOKUP_H
#define RAMCLOUD_INDEXLOOKUP_H

#include "RamCloud.h"
#define NUM_READ_RPC 10

namespace RAMCloud {

class IndexLookup {
  public:
    IndexLookup (RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
                 const void* firstKey, uint16_t firstKeyLength,
                 uint64_t firstAllowedKeyHash,
                 const void* lastKey, uint16_t lastKeyLength);
    ~IndexLookup();
    bool isReady();
    bool getNext();
    const void* getKey(KeyIndex keyIndex = 0, KeyLength *keyLength = NULL);
    KeyLength getKeyLength(KeyIndex keyIndex = 0);
    const void* getValue(uint32_t *valueLength = NULL);
    uint32_t getValueLength();

  private:
    enum ReadRpcStatus{
      AVAILABLE,
      INPROCESS,
      FINISHED
    };
    void issueNextLookup();
    RamCloud* ramcloud;
    Tub<LookupIndexKeysRpc> lookupRpc;
    Tub<IndexedReadRpc> readRpc[NUM_READ_RPC];
    ReadRpcStatus readRpcStatus[NUM_READ_RPC];
    Buffer readRpcPKHashes[NUM_READ_RPC];
    uint32_t readRpcNumHashes[NUM_READ_RPC];
    uint32_t unreadNumObjects[NUM_READ_RPC];
    uint32_t readRpcOffset[NUM_READ_RPC];
    uint64_t tableId;
    uint8_t indexId;
    uint16_t firstKeyLength, lastKeyLength;
    uint64_t nextKeyHash;
    void *firstKey, *lastKey;
    Buffer lookupResp;
    Tub<Object> curObj;
    Buffer readResp[NUM_READ_RPC];
    bool hasIssuedLookupRpc, finishedLookup, isFirstObj;
};

} // end RAMCloud
#endif // RAMCLOUD_INDEXLOOKUP_H
