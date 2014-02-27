/* Copyright (c) 2010-2014 Stanford University
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

#include "Common.h"
#include "IndexletManager.h"

namespace RAMCloud {

IndexletManager::IndexletManager()
    : indexletMap()
    , lock("IndexletManager::lock")
{
}

bool
IndexletManager::addIndexlet(uint64_t tableId, uint8_t indexId,
                             uint16_t firstKeyLength, const void *firstKey,
                             uint16_t lastKeyLength, const void *lastKey)
{
    Lock guard(lock);

    // If an existing tablet overlaps this range at all, fail.
    if (lookup(tableId, indexId, firstKeyLength, firstKey, guard)
            != indexletMap.end() ||
        lookup(tableId, indexId, lastKeyLength, lastKey, guard)
            != indexletMap.end()) {
        return false;
    }

    indexletMap.emplace(std::make_pair(tableId, indexId),
                    Indexlet(firstKeyLength, firstKey, lastKeyLength, lastKey));
    return true;
}

bool
IndexletManager::deleteIndexlet(uint64_t tableId, uint8_t indexId,
                                uint16_t firstKeyLength, const void *firstKey,
                                    uint16_t lastKeyLength, const void *lastKey)
{
    Lock guard(lock);

    IndexletMap::iterator it =
            lookup(tableId, indexId, firstKeyLength, firstKey, guard);
    if (it == indexletMap.end())
        return false;

    Indexlet* t = &it->second;
    //TODO(ashgup): compare the two keys
    if (t->firstKeyLength != firstKeyLength || t->lastKeyLength!= lastKeyLength)
        return false;

    indexletMap.erase(it);
    return true;
}

bool
IndexletManager::getIndexlet(uint64_t tableId, uint8_t indexId,
                    uint16_t firstKeyLength, const void *firstKey,
                    uint16_t lastKeyLength, const void *lastKey,
                    Indexlet* outIndexlet)
{
    Lock guard(lock);

    IndexletMap::iterator it =
        lookup(tableId, indexId, firstKeyLength, firstKey, guard);
    if (it == indexletMap.end())
        return false;

    Indexlet* t = &it->second;
    //TODO(ashgup): compare the two keys
    if (t->firstKeyLength != firstKeyLength || t->lastKeyLength!= lastKeyLength)
        return false;

    if (outIndexlet != NULL)
        *outIndexlet = *t;
    return true;
}

IndexletManager::IndexletMap::iterator
IndexletManager::lookup(uint64_t tableId, uint8_t indexId,
                               uint16_t keyLength, const void *key, Lock& lock)
{
    auto range = indexletMap.equal_range(std::make_pair(tableId, indexId));
    IndexletMap::iterator end = range.second;
    for (IndexletMap::iterator it = range.first; it != end; it++) {
        Indexlet* t = &it->second;
        //TODO(ashgup): compare the key
        if (keyLength >= t->firstKeyLength && keyLength <= t->lastKeyLength)
            return it;
    }
    return indexletMap.end();
}

} //namespace
