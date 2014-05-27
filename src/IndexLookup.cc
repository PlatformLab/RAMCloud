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

#include "IndexLookup.h"

namespace RAMCloud {
IndexLookup::IndexLookup(RamCloud* ramcloud, uint64_t tableId, uint8_t indexId,
                         const void* in_firstKey, uint16_t firstKeyLength,
                         uint64_t firstAllowedKeyHash,
                         const void* in_lastKey, uint16_t lastKeyLength)
    : ramcloud(ramcloud)
    , lookupRpc()
    , tableId(tableId)
    , indexId(indexId)
    , firstKeyLength(firstKeyLength)
    , lastKeyLength(lastKeyLength)
    , nextKeyHash (firstAllowedKeyHash)
    , firstKey(NULL)
    , lastKey(NULL)
    , lookupResp()
    , curObj()
    , hasIssuedLookupRpc(false)
    , finishedLookup(false)
    , isFirstObj(true)
{
  for (int i = 0; i < NUM_READ_RPC; i++) {
    readRpcStatus[i] = AVAILABLE;
  }
  firstKey = malloc(firstKeyLength);
  memcpy(firstKey, in_firstKey, firstKeyLength);
  lastKey = malloc(lastKeyLength);
  memcpy(lastKey, in_lastKey, lastKeyLength);
  issueNextLookup();
}

IndexLookup::~IndexLookup()
{
  free(firstKey);
  free(lastKey);
  if (hasIssuedLookupRpc)
    lookupRpc.destroy();
  if (!isFirstObj)
    curObj.destroy();
}

bool
IndexLookup::getNext()
{
  if (!finishedLookup) {
    if (lookupRpc->isReady()) {
      size_t readRpcId = 0;
      for (readRpcId = 0; readRpcId < NUM_READ_RPC; readRpcId++)
        if (readRpcStatus[readRpcId] == AVAILABLE)
          break;
      if (readRpcId < NUM_READ_RPC) {
        uint32_t numHashes;
        uint16_t nextKeyLength;
        uint64_t nextKeyHash;
        lookupRpc->wait(&numHashes, &nextKeyLength, &nextKeyHash);
        readRpcPKHashes[readRpcId].reset();
        uint32_t lookupOffset =
          sizeof32(WireFormat::LookupIndexKeys::Response);
        firstKey = malloc(nextKeyLength);
        memcpy(firstKey, lookupResp.getOffset<char>(lookupOffset),
               nextKeyLength);
        lookupOffset += nextKeyLength;
        for (size_t i = 0; i < numHashes; i++) {
          new(&readRpcPKHashes[readRpcId], APPEND) uint64_t(
              *lookupResp.getOffset<uint64_t>(lookupOffset));
          lookupOffset += sizeof32(uint64_t);
        }
        readResp[readRpcId].reset();
        readRpc[readRpcId].construct(ramcloud, tableId, numHashes,
                                     &readRpcPKHashes[readRpcId], indexId,
                                     firstKey, firstKeyLength,
                                     lastKey, lastKeyLength,
                                     &readResp[readRpcId]);
        readRpcStatus[readRpcId] = INPROCESS;
        if (nextKeyLength > 0)
          issueNextLookup();
        else
          finishedLookup = true;
      }
    }
  }

  for (size_t i = 0; i < NUM_READ_RPC; i++) {
    if (readRpc[i]->isReady() && readRpcStatus[i] == INPROCESS) {
      readRpc[i]->wait(&unreadNumObjects[i]);
      readRpcOffset[i] = sizeof32(WireFormat::IndexedRead::Response);
      readRpcStatus[i] = FINISHED;
    }
  }

  for (size_t i = 0; i < NUM_READ_RPC; i++) {
    if (readRpcStatus[i] == FINISHED) {
      unreadNumObjects[i] --;
      uint64_t version = *readResp[i].getOffset<uint64_t>(readRpcOffset[i]);
      readRpcOffset[i] += sizeof32(uint64_t);
      uint32_t length = *readResp[i].getOffset<uint32_t>(readRpcOffset[i]);
      readRpcOffset[i] += sizeof32(uint32_t);
      if (isFirstObj)
        isFirstObj = false;
      else
        curObj.destroy();
      curObj.construct(tableId, version, 0, readResp[i], readRpcOffset[i], length);
      readRpcOffset[i] += length;
      if (unreadNumObjects[i] == 0) {
        readRpcStatus[i] = AVAILABLE;
      }
      return true;
    }
  }
  return false;
}

const void*
IndexLookup::getKey(KeyIndex keyIndex, KeyLength *keyLength)
{
  return curObj->getKey(keyIndex, keyLength);
}

KeyLength
IndexLookup::getKeyLength(KeyIndex keyIndex)
{
  return curObj->getKeyLength(keyIndex);
}

const void*
IndexLookup::getValue(uint32_t *valueLength)
{
  return curObj->getValue(valueLength);
}

uint32_t
IndexLookup::getValueLength()
{
  return curObj->getValueLength();
}

void
IndexLookup::issueNextLookup()
{
  if (hasIssuedLookupRpc)
    lookupRpc.destroy();
  hasIssuedLookupRpc = true;
  lookupResp.reset();
  lookupRpc.construct(ramcloud, tableId, indexId, firstKey, firstKeyLength,
                      nextKeyHash, lastKey, lastKeyLength, &lookupResp);
}

} // end RAMCloud
