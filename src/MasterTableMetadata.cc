/* Copyright (c) 2013-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "MasterTableMetadata.h"

namespace RAMCloud {

/**
 * Construct MasterTableMetadata object.
 */
MasterTableMetadata::MasterTableMetadata()
    : tableMetadataMap()
    , lock("MasterTableMetadata::lock")
{
}

/**
 * Destruct MasterTableMetadata object.
 */
MasterTableMetadata::~MasterTableMetadata()
{
    SpinLock::Guard _(lock);

    TableMetadataMap::iterator it;
    for (it = tableMetadataMap.begin(); it != tableMetadataMap.end(); ++it) {
        if (it->second != NULL) {
            delete it->second;
            it->second = NULL;
        }
    }
}

/**
 * Finds and returns a pointer to a TableMetadata entry corresponding to the
 * provided tableId.
 *
 * \param tableId
 *      tableId of the entry requested.
 * \return
 *      If found, pointer to the metadata entry corresponding to the provided
 *      tableId.  If no entry is found, NULL is returned.  The pointer returned
 *      will not "expire" in that they can be stored and used later without fear
 *      that the data will be invalidated.
 */
MasterTableMetadata::Entry*
MasterTableMetadata::find(uint64_t tableId) const
{
    SpinLock::Guard _(lock);

    Entry* entry = NULL;
    TableMetadataMap::const_iterator it = tableMetadataMap.find(tableId);
    if (expect_true(it != tableMetadataMap.end())) {
        entry = it->second;
    }
    return entry;
}

/**
 * Tries to find and return a pointer to a TableMetadata entry corresponding t
 * the provided tableId.  If no entry is found, a new entry is created and a
 * pointer to this new entry is returned.
 *
 * \param tableId
 *      tableId of the entry to be found or created.
 * \return
 *      Pointer to the entry for tableId (should never be NULL).  The pointer
 *      returned will not "expire" in that they can be stored and used later
 *      without fear that the data will be invalidated.
 */
MasterTableMetadata::Entry*
MasterTableMetadata::findOrCreate(uint64_t tableId)
{
    SpinLock::Guard _(lock);

    Entry* entry = NULL;
    TableMetadataMap::iterator it = tableMetadataMap.find(tableId);
    if (expect_false(it == tableMetadataMap.end())) {
        entry = new Entry(tableId);
        tableMetadataMap.insert({tableId, entry});
    } else {
        entry = it->second;
    }
    return entry;
}

/**
 * Convenience method for getting a scanner for this MasterTableMetadata object.
 *
 * \return
 *      Returns a scanner constructed with this as its MasterTableMetadata
 *      pointer.  The following two statements are equivalent:
 *
 *          MasterTableMetadata::scanner sc(mtm);
 *          MasterTableMetadata::scanner sc = mtm.getScanner();
 */
MasterTableMetadata::scanner
MasterTableMetadata::getScanner()
{
    return scanner(this);
}


/**
 * Constructs a MasterTableMetadata::scanner object.  A critical section of mtm
 * begins with the construction of this object.  All other methods of mtm will
 * block until this object is destructed.
 *
 * Note: If mtm is NULL (as in the default constructor), no critical section
 * begins and accesses to this object are illegal.  Object can be made legal by
 * moving a legal object into this object.  This mimics standard iterator use
 * and behavior as it allows scanners to be first declared containing some
 * default unusable state (default constructor) and then later have a valid
 * scanner assigned to it (move assignment).
 *
 * \param mtm
 *      Pointer to the MasterTableMetadata whose entries will be accessed.
 *      Default is NULL.
 */
MasterTableMetadata::scanner::scanner(MasterTableMetadata* mtm /* = NULL */)
    : mtm(mtm)
    , it()
{
    if (mtm != NULL) {
        mtm->lock.lock();
        it = mtm->tableMetadataMap.begin();
    }
}

/**
 * Implements MoveConstruction creating a new scanner with the valid contents of
 * other and then invalidates other's contents.  The critical section is passed
 * from other to this object and continues through the destruction of this new
 * object.
 *
 * \param other
 *      Object whose contents will be taken and moved into the new object and
 *      subsequently invalidated.
 */
MasterTableMetadata::scanner::scanner(MasterTableMetadata::scanner&& other)
    : mtm(other.mtm)
    , it(other.it)
{
    other.it = mtm->tableMetadataMap.end();
    other.mtm = NULL;
}

/**
 * Destroys the scanner.  If the scanner is still valid at the time of its
 * destruction, this destructor also ends the critical section.
 */
MasterTableMetadata::scanner::~scanner()
{
    if (mtm != NULL) {
        mtm->lock.unlock();
    }
}

/**
 * Implements MoveAssignment moving the valid contents of other into this object
 * and then invalidating other's contents.  The critical section is passed from
 * other to this object and continues through the destruction of this object.
 *
 * \param other
 *      Object whose contents will be taken and moved into the this object and
 *      subsequently invalidated.
 */
MasterTableMetadata::scanner&
MasterTableMetadata::scanner::operator=(MasterTableMetadata::scanner&& other)
{
    mtm = other.mtm;
    it = other.it;
    other.it = mtm->tableMetadataMap.end();
    other.mtm = NULL;
    return *this;
}

/**
 * Returns true if the scanner has at least one more element that can be
 * accessed with a scanner.next() method call.  This is used as the loop
 * condition during a scan on the MasterTableMetadata container.
 *
 * \return
 *      Return true if container has at least one more element available for
 *      access.  Returns false otherwise.
 */
bool
MasterTableMetadata::scanner::hasNext() const
{
    return (it != mtm->tableMetadataMap.end());
}

/**
 * Returns a pointer to the "next" TableMetadataEntry.  It is used to access
 * entries in the MasterTableMetadata container during a scan.
 *
 * \return
 *      Returns a pointer to the "next" TableMetadataEntry.  Returns NULL if
 *      there is no "next" entry to return.
 */
MasterTableMetadata::Entry*
MasterTableMetadata::scanner::next()
{
    Entry* entry = NULL;
    if (expect_true(it != mtm->tableMetadataMap.end())) {
        entry = it->second;
        ++it;
    }
    return entry;
}

} // namespace RAMCloud
