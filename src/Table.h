/* Copyright (c) 2009 Stanford University
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

#ifndef RAMCLOUD_TABLE_H
#define RAMCLOUD_TABLE_H

#include "Common.h"
#include "HashTable.h"

namespace RAMCloud {

/**
 * Each object of this class represents one table (a collection of objects
 * named with 64-bit integer identifiers).
 */
class Table {
  public:

    /**
     * The maximum length of a table name, including the null terminator.
     */
    static const int TABLE_NAME_MAX_LEN = 64;

    explicit Table(uint64_t tableId)
        : tableId(tableId),
          nextKey(0),
          nextVersion(1)
    {
        name[0] = '\0';
    }

    ~Table()
    {
        numDeletes++;
    }

    const char *GetName() { return &name[0]; }

    /**
     * \param new_name
     *      A string with a length within #TABLE_NAME_MAX_LEN, including the
     *      null terminator.
     */
    void SetName(const char *new_name) {
        strncpy(&name[0], new_name, TABLE_NAME_MAX_LEN);
        name[TABLE_NAME_MAX_LEN - 1] = '\0';
    }

    /**
     * Increment and return the next table-assigned object ID.
     * \return
     *      The next available object ID in the table.
     * \warning
     *      A client could have already placed an object here by fabricating the
     *      object ID.
     */
    uint64_t AllocateKey(HashTable *hashTable) {
        while (hashTable->lookup(tableId, nextKey))
            ++nextKey;
        return nextKey;
    }

    /**
     * Increment and return the master vector clock.
     * \return
     *      The next version available from the master vector clock.
     * \see #nextVersion
     */
    uint64_t AllocateVersion() {
        return nextVersion++;
    }

    /**
     * Ensure the master master vector clock is at least a certain version.
     * \param minimum
     *      The minimum version the master vector clock can be set to after this
     *      operation.
     * \see #nextVersion
     */
    void RaiseVersion(uint64_t minimum) {
        if (minimum > nextVersion)
            nextVersion = minimum;
    }

    /**
     * Object the Table's identifier.
     */
    uint64_t getId() {
        return tableId;
    }

    /// Number of Table object destructors called.
    static int numDeletes;

  private:

    /**
     * The name of the table. If this is an empty string it means the table
     * isn't currently in use.
     * \see #SetName().
     */
    char name[64];

    /**
     * The unique numerical identifier for this table.
     */
    uint64_t tableId;

    /**
     * The next available object ID in the table.
     * \see #AllocateKey().
     */
    uint64_t nextKey;

    /**
     * The master vector clock for the table.
     *
     * \li We guarantee that every distinct blob ever at a particular object ID
     * will have a distinct version number, even across generations, so that
     * they can be uniquely identified across all time with a version number.
     *
     * \li We guarantee that version numbers for a particular object ID
     * monotonically increase over time, so that comparing two version numbers
     * tells which one is more recent.
     *
     * \li We guarantee that the version number of an object increases by
     * exactly one when it is updated, so that clients can accurately predict
     * the version numbers that they will write before the write completes.
     *
     * These guarantees are implemented as follows:
     *
     * \li #nextVersion, the master vector clock, contains the next available
     * version number for the table on the master. It is initialized to a small
     * integer when the table is created and is recoverable after crashes.
     *
     * \li When an object is created, its new version number is set to the value
     * of the master vector clock, and the master vector clock is incremented.
     * See #AllocateVersion.
     *
     * \li When an object is updated, its new version number is set the old
     * blob's version number plus one.
     *
     * \li When an object is deleted, set the master vector clock to the higher
     * of the master vector clock and the deleted blob's version number plus
     * one. See #RaiseVersion.
     */
    uint64_t nextVersion;

    DISALLOW_COPY_AND_ASSIGN(Table);
};

} // namespace RAMCloud

#endif // RAMCLOUD_TABLE_H
