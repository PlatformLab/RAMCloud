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

#ifndef RAMCLOUD_INDEXLET_H
#define RAMCLOUD_INDEXLET_H

#include "Common.h"
#include "ServerId.h"

namespace RAMCloud {

/**
 * Each indexlet owned by a master is described by fields in this structure.
 * Indexlets describe contiguous ranges of secondary key space for a
 * particular index for a given table.
 */
struct Indexlet {

    //TODO(ashgup): move allocation to indexlet.cc
    Indexlet(void *firstKey, uint16_t firstKeyLength,
             void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
             ServerId serverId, string serviceLocator)
        : firstKey(NULL)
        , firstKeyLength(firstKeyLength)
        , firstNotOwnedKey(NULL)
        , firstNotOwnedKeyLength(firstNotOwnedKeyLength)
        , serverId(serverId)
        , serviceLocator(serviceLocator)
    {
        if (firstKeyLength != 0){
            this->firstKey = malloc(firstKeyLength);
            memcpy(this->firstKey, firstKey, firstKeyLength);
        }
        if (firstNotOwnedKeyLength != 0){
            this->firstNotOwnedKey = malloc(firstNotOwnedKeyLength);
            memcpy(this->firstNotOwnedKey, firstNotOwnedKey,
                                                    firstNotOwnedKeyLength);
        }
    }

    Indexlet(void *firstKey, uint16_t firstKeyLength,
             void *firstNotOwnedKey, uint16_t firstNotOwnedKeyLength,
             ServerId serverId)
        : firstKey(NULL)
        , firstKeyLength(firstKeyLength)
        , firstNotOwnedKey(NULL)
        , firstNotOwnedKeyLength(firstNotOwnedKeyLength)
        , serverId(serverId)
        , serviceLocator()
    {
        if (firstKeyLength != 0){
            this->firstKey = malloc(firstKeyLength);
            memcpy(this->firstKey, firstKey, firstKeyLength);
        }
        if (firstNotOwnedKeyLength != 0){
            this->firstNotOwnedKey = malloc(firstNotOwnedKeyLength);
            memcpy(this->firstNotOwnedKey, firstNotOwnedKey,
                                                firstNotOwnedKeyLength);
        }
    }

    Indexlet(const Indexlet& indexlet)
        : firstKey(NULL)
        , firstKeyLength(indexlet.firstKeyLength)
        , firstNotOwnedKey(NULL)
        , firstNotOwnedKeyLength(indexlet.firstNotOwnedKeyLength)
        , serverId(indexlet.serverId)
        , serviceLocator(indexlet.serviceLocator)
    {
        if (firstKeyLength != 0){
            this->firstKey = malloc(firstKeyLength);
            memcpy(this->firstKey, indexlet.firstKey, firstKeyLength);
        }
        if (firstNotOwnedKeyLength != 0){
            this->firstNotOwnedKey = malloc(firstNotOwnedKeyLength);
            memcpy(this->firstNotOwnedKey, indexlet.firstNotOwnedKey,
                                                    firstNotOwnedKeyLength);
        }
    }

    Indexlet& operator =(const Indexlet& indexlet)
    {
        this->firstKey = NULL;
        this->firstKeyLength = indexlet.firstKeyLength;
        this->firstNotOwnedKey = NULL;
        this->firstNotOwnedKeyLength = indexlet.firstNotOwnedKeyLength;
        this->serverId = indexlet.serverId;
        this->serviceLocator = indexlet.serviceLocator;

        if (firstKeyLength != 0){
            this->firstKey = malloc(firstKeyLength);
            memcpy(this->firstKey, indexlet.firstKey, firstKeyLength);
        }
        if (firstNotOwnedKeyLength != 0){
            this->firstNotOwnedKey = malloc(firstNotOwnedKeyLength);
            memcpy(this->firstNotOwnedKey, indexlet.firstNotOwnedKey,
                                                        firstNotOwnedKeyLength);
        }
        return *this;
    }

    ~Indexlet()
    {
        if (firstKeyLength != 0)
            free(firstKey);
        if (firstNotOwnedKeyLength != 0)
            free(firstNotOwnedKey);
    }

    /// Blob for the smallest key that is in this indexlet.
    //TODO(ashgup): first key can be NULL. add logic in index let manager
    ///TODO(ashgup): add null test case in coordinator service and tablemanager
    void *firstKey;

    /// Length of the firstKey
    uint16_t firstKeyLength;

    /// Blob for the smallest key greater than all the keys belonging to
    /// this indexlet.
    void *firstNotOwnedKey;

    /// Length of the firstNotOwnedKey
    uint16_t firstNotOwnedKeyLength;

    /// The server id of the master owning this indexlet.
    ServerId serverId;

    /// Service locator for the indexlet to be used by the coordinator.
    string serviceLocator;
};

} // namespace RAMCloud

#endif
