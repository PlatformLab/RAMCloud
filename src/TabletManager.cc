/* Copyright (c) 2012-2014 Stanford University
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
#include "TabletManager.h"
#include "TimeTrace.h"

namespace RAMCloud {

TabletManager::TabletManager()
    : tabletMap()
    , lock("TabletManager::lock")
{
}

/**
 * Add a new tablet to this TabletManager's list of tablets. If the tablet
 * already exists or overlaps with any other tablets, the call will fail.
 *
 * \param tableId
 *      Identifier of the table the new tablet belongs to.
 * \param startKeyHash
 *      The first key hash value that this tablet owns.
 * \param endKeyHash
 *      The last key hash value that this tablet owns.
 * \param state
 *      The initial state of the tablet (see the TabletState enum for more
 *      details).
 * \return
 *      Returns true if successfully added, false if the tablet cannot be
 *      added because it overlaps with one or more existing tablets.
 */
bool
TabletManager::addTablet(uint64_t tableId,
                         uint64_t startKeyHash,
                         uint64_t endKeyHash,
                         TabletState state)
{
    Lock guard(lock);

    // If an existing tablet overlaps this range at all, fail.
    if (lookup(tableId, startKeyHash, guard) != tabletMap.end() ||
      lookup(tableId, endKeyHash, guard) != tabletMap.end()) {
        return false;
    }

    tabletMap.insert(std::make_pair(tableId,
                     Tablet(tableId, startKeyHash, endKeyHash, state)));
    return true;
}

/**
 * Given a key, determine whether a tablet exists for this key and has status
 * NORMAL.  We simultaneously increments the read count on the tablet. This is
 * called by ObjectManger::readObject to avoid looking up the Tablet twice, for
 * verification of state and incrementing the read count.
 *
 * \param key
 *      The Key whose tablet we're looking up.
 * \return
 *      True if a tablet was found, otherwise false.
 */
bool
TabletManager::checkAndIncrementReadCount(Key& key) {
    Lock guard(lock);
    TabletMap::iterator it = lookup(key.getTableId(), key.getHash(), guard);

    if (it == tabletMap.end() || it->second.state != NORMAL)
        return false;

    it->second.readCount++;
    return true;
}

/**
 * Given a key, obtain the data of the tablet associated with that key, if one
 * exists. Note that the data returned is a snapshot. The TabletManager's data
 * may be modified at any time by other threads.
 *
 * \param key
 *      The Key whose tablet we're looking up.
 * \param outTablet
 *      Optional pointer to a Tablet object that will be filled with the current
 *      appopriate tablet data, if such a tablet exists. May be NULL if the
 *      caller only wants to check for existence.
 * \return
 *      True if a tablet was found, otherwise false.
 */
bool
TabletManager::getTablet(Key& key, Tablet* outTablet)
{
    return getTablet(key.getTableId(), key.getHash(), outTablet);
}

/**
 * Given a tableId and hash value, obtain the data of the tablet associated
 * with them, if one exists. Note that the data returned is a snapshot. The
 * TabletManager's data may be modified at any time by other threads.
 *
 * \param tableId
 *      The table identifier of the tablet we're looking up.
 * \param keyHash
 *      Key hash value whose tablet we're looking up.
 * \param outTablet
 *      Optional pointer to a Tablet object that will be filled with the current
 *      appopriate tablet data, if such a tablet exists. May be NULL if the caller
 *      only wants to check for existence.
 * \return
 *      True if a tablet was found, otherwise false.
 */
bool
TabletManager::getTablet(uint64_t tableId, uint64_t keyHash, Tablet* outTablet)
{
    Lock guard(lock);

    TabletMap::iterator it = lookup(tableId, keyHash, guard);
    if (it == tabletMap.end())
        return false;

    if (outTablet != NULL)
        *outTablet = it->second;
    return true;
}

/**
 * Given the exact specification of a tablet's range (table identifier and start
 * and end hash values), obtain the current data associated with that tablet, if
 * it exists. Note that the data returned is a snapshot. The TabletManager's data
 * may be modified at any time by other threads.
 *
 * \param tableId
 *      The table identifier of the tablet we're looking up.
 * \param startKeyHash
 *      Key hash value at which the desired tablet begins.
 * \param endKeyHash
 *      Key hash value at which the desired tablet ends.
 * \param outTablet
 *      Optional pointer to a Tablet object that will be filled with the current
 *      appopriate tablet data, if such a tablet exists. May be NULL if the caller
 *      only wants to check for existence.
 * \return
 *      True if a tablet was found, otherwise false.
 */
bool
TabletManager::getTablet(uint64_t tableId,
                         uint64_t startKeyHash,
                         uint64_t endKeyHash,
                         Tablet* outTablet)
{
    Lock guard(lock);

    TabletMap::iterator it = lookup(tableId, startKeyHash, guard);
    if (it == tabletMap.end())
        return false;

    Tablet* t = &it->second;
    if (t->startKeyHash != startKeyHash || t->endKeyHash != endKeyHash)
        return false;

    if (outTablet != NULL)
        *outTablet = *t;
    return true;
}

/**
 * Fill in the given vector with data from all of the tablets this TabletManager
 * is currently keeping track of. The results are unsorted.
 *
 * \param outTablets
 *      Pointer to the vector to append tablet data to.
 */
void
TabletManager::getTablets(vector<Tablet>* outTablets)
{
    Lock guard(lock);

    TabletMap::iterator it = tabletMap.begin();
    for (size_t i = 0; it != tabletMap.end(); i++) {
        outTablets->push_back(it->second);
        ++it;
    }
}

/**
 * Remove a tablet previously created by addTablet() or splitTablet() and delete
 * all data that tracks its existence.
 *
 * \param tableId
 *      The table identifier of the tablet we're deleting.
 * \param startKeyHash
 *      Key hash value at which the to-be-deleted tablet begins.
 * \param endKeyHash
 *      Key hash value at which the to-be-deleted tablet ends.
 * \return
 *      True if the tablet was deleted. False if no such tablet existed.
 */
bool
TabletManager::deleteTablet(uint64_t tableId,
                            uint64_t startKeyHash,
                            uint64_t endKeyHash)
{
    Lock guard(lock);

    TabletMap::iterator it = lookup(tableId, startKeyHash, guard);
    if (it == tabletMap.end())
        return false;

    Tablet* t = &it->second;
    if (t->startKeyHash != startKeyHash || t->endKeyHash != endKeyHash)
        return false;

    tabletMap.erase(it);
    return true;
}

/**
 * Split an existing tablet into two new, contiguous tablets. This may be used
 * prior to migrating objects to another server if only a portion of a tablet
 * needs to be moved.
 *
 * \param tableId
 *      Table identifier corresponding to the tablet to be split.
 * \param splitKeyHash
 *      The point at which to split the tablet. This value must be strictly
 *      between (but not equal to) startKeyHash and endKeyHash. The tablet
 *      will be split into two pieces: [startKeyHash, splitKeyHash - 1] and
 *      [splitKeyHash, endKeyHash].
 * \return
 *      True if the tablet was found and split or if the split already exists.
 *      False if no tablet corresponding to the given (tableId, splitKeyHash)
 *      tuple was found. This operation is idempotent.
 */
bool
TabletManager::splitTablet(uint64_t tableId,
                           uint64_t splitKeyHash)
{
    Lock guard(lock);

    TabletMap::iterator it = lookup(tableId, splitKeyHash, guard);
    if (it == tabletMap.end())
        return false;

    Tablet* t = &it->second;

    // If a split already exists in the master's tablet map, lookup
    // will return the tablet whose startKeyHash matches splitKeyHash.
    // So to make it idempotent, check for this condition before you
    // decide to do the split
    if (splitKeyHash != t->startKeyHash) {
        tabletMap.insert(std::make_pair(tableId, Tablet
                         (tableId, splitKeyHash, t->endKeyHash, t->state)));
        t->endKeyHash = splitKeyHash - 1;

        // It's unclear what to do with the counts when splitting. The old
        // behavior was to simply zero them, so for the time being we'll
        // stick with that. At the very least it's what Christian expects.
        t->readCount = t->writeCount = 0;
    }

    return true;
}

/**
 * Transition the state field associated with a given tablet from a specific
 * old state to a given new state. This is typically used when recovery has
 * completed and a tablet is changed from the RECOVERING to NORMAL state.
 *
 * \param tableId
 *      Table identifier corresponding to the tablet to update.
 * \param startKeyHash
 *      First key hash value corresponding to the tablet to update.
 * \param endKeyHash
 *      Last key hash value corresponding to the tablet to update.
 * \param oldState
 *      The state the tablet is expected to be in prior to changing to the new
 *      value. This helps ensure that the proper transition is made, since
 *      another thread could modify the tablet's state between getTablet() and
 *      changeState() calls.
 * \param newState
 *      The state to transition the tablet to.
 * \return
 *      Returns true if the state was updated, otherwise false.
 */
bool
TabletManager::changeState(uint64_t tableId,
                           uint64_t startKeyHash,
                           uint64_t endKeyHash,
                           TabletState oldState,
                           TabletState newState)
{
    Lock guard(lock);

    TabletMap::iterator it = lookup(tableId, startKeyHash, guard);
    if (it == tabletMap.end())
        return false;

    Tablet* t = &it->second;
    if (t->startKeyHash != startKeyHash || t->endKeyHash != endKeyHash)
        return false;

    if (t->state != oldState)
        return false;

    t->state = newState;
    return true;
}

/**
 * Increment the object read counter on the tablet associated with the given
 * key.
 */
void
TabletManager::incrementReadCount(Key& key)
{
    Lock guard(lock);
    TabletMap::iterator it = lookup(key.getTableId(), key.getHash(), guard);
    if (it != tabletMap.end())
        it->second.readCount++;
}

/**
 * Increment the object write counter on the tablet associated with the given
 * key.
 */
void
TabletManager::incrementWriteCount(Key& key)
{
    Lock guard(lock);
    TabletMap::iterator it = lookup(key.getTableId(), key.getHash(), guard);
    if (it != tabletMap.end())
        it->second.writeCount++;
}

/**
 * Populate a ServerStatistics protocol buffer with read and write statistics
 * gathered for our tablets.
 */
void
TabletManager::getStatistics(ProtoBuf::ServerStatistics* serverStatistics)
{
    Lock guard(lock);

    TabletMap::iterator it = tabletMap.begin();
    while (it != tabletMap.end()) {
        Tablet* t = &it->second;
        ProtoBuf::ServerStatistics_TabletEntry* entry =
            serverStatistics->add_tabletentry();
        entry->set_table_id(t->tableId);
        entry->set_start_key_hash(t->startKeyHash);
        entry->set_end_key_hash(t->endKeyHash);
        uint64_t totalOperations = t->readCount + t->writeCount;
        if (totalOperations > 0)
            entry->set_number_read_and_writes(totalOperations);
        ++it;
    }
}

/**
 * Obtain the total number of tablets this object is managing.
 */
size_t
TabletManager::getCount()
{
    Lock guard(lock);
    return tabletMap.size();
}

/**
 * Obtain a string representation of the tablets this object is managing.
 * Tablets are unordered in the output. This is typically used in unit tests.
 */
string
TabletManager::toString()
{
    Lock guard(lock);

    string output;
    TabletMap::iterator it = tabletMap.begin();
    while (it != tabletMap.end()) {
        if (output.length() != 0)
            output += "\n";
        Tablet* t = &it->second;
        output += format("{ tableId: %lu startKeyHash: %lu endKeyHash: %lu "
            "state: %d reads: %lu writes: %lu }", t->tableId, t->startKeyHash,
            t->endKeyHash, t->state, t->readCount, t->writeCount);
        ++it;
    }

    return output;
}

/**
 * Helper for the public methods that need to look up a tablet. This method
 * iterates over all candidates in the multimap.
 *
 * \param tableId
 *      Identifier of the table to look up.
 * \param keyHash
 *      Key hash value corresponding to the desired tablet.
 * \param lock
 *      This internal method is not thread-safe, so the caller must hold the
 *      monitor lock while calling. This parameter ensures they don't forget
 *      to.
 * \return
 *      A TabletMap::iterator is returned. If no tablet was found, it will be
 *      equal to tabletMap.end(). Otherwise, it will refer to the desired
 *      tablet.
 *
 *      An iterator, rather than a Tablet pointer is returned to facilitate
 *      efficient deletion.
 */
TabletManager::TabletMap::iterator
TabletManager::lookup(uint64_t tableId, uint64_t keyHash, Lock& lock)
{
    auto range = tabletMap.equal_range(tableId);
    TabletMap::iterator end = range.second;
    for (TabletMap::iterator it = range.first; it != end; it++) {
        Tablet* t = &it->second;
        if (keyHash >= t->startKeyHash && keyHash <= t->endKeyHash)
            return it;
    }

    return tabletMap.end();
}

} // namespace
