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


#ifndef RAMCLOUD_MASTERTABLEMETADATA_H
#define RAMCLOUD_MASTERTABLEMETADATA_H

#include <unordered_map>
#include "Common.h"
#include "SpinLock.h"
#include "TableStats.h"

#include "ShortMacros.h"

namespace RAMCloud {

/**
 * Represents a collection of metadata entries; one for each table that resides
 * fully or partially on a given master.  An instance of this class can be made
 * per instance of MasterService in order to store this per table metadata.
 * MasterTableMetadata allows other "client" classes to store per table metadata
 * in this container as "blocks" in a TableMetadataEntry.  For instance, the
 * TabletStatsEstimator collects per table statistics information and stores
 * them in this container for later use.
 *
 * This class provides thread-safe lookups (find), inserts (insert), and
 * iterations (scanner) into the container.  However, this class does NOT
 * provide thread-safe access to the entries returned from lookups, inserts, or
 * iterations (see MasterTableMetadata::TableMetadataEntry).  This is flexibly
 * left to the discretion of those "client" classes who store information in
 * the entries to minimize resource contention when thread-safely is not
 * necessary.  Because this class does not provide thread-safe access to
 * entries, this class also does not provide a thread-safe "remove" method (or
 * any "remove" method for that matter).
 *
 * Note: While we do not believe we need the "remove" functionality at this
 * time, should it be necessary to implement in the future, thread-safe access
 * to entries would also need to be implemented keeping performance (increased
 * possibility of resource contention) in mind.  Important to our choice not
 * to provide "remove" is our assumption that tableIds will not be reused.
 */
class MasterTableMetadata {
  PUBLIC:
    /**
     * Holds the metadata for a single table.  The entry should consist of a
     * tableId and "blocks" of data that the entry holds on behalf of "clients".
     * For instance, this structure holds a TableStatsBlock for TableStats.
     *
     * A table metadata Entry does not itself enforce a locking mechanism for
     * thread-safety.  Instead, it allows "clients" that store blocks in the
     * entry to provide their own mechanism for thread-safety at their own
     * discretion.
     */
    struct Entry {
        uint64_t tableId;
        TableStats::Block stats;

        explicit Entry(uint64_t tableId)
            : tableId(tableId)
            , stats()
        {}
    };

    /// Forward declaration of scanner class allowing use as return type.
    class scanner;

    MasterTableMetadata();
    ~MasterTableMetadata();
    Entry* find(uint64_t tableId) const;
    Entry* findOrCreate(uint64_t tableId);
    scanner getScanner();

  PRIVATE:
    /// Maps tableIds to TableMetadata entries.
    typedef std::unordered_map<uint64_t, Entry*> TableMetadataMap;

    /// This unordered_map is used to store and access all table metadata.
    TableMetadataMap tableMetadataMap;

    /// Monitor spinlock used to protect the tabletMap from concurrent access.
    mutable SpinLock lock;

  PUBLIC:
    /**
     * This class provides "Java-iterator-like" semantics as a means of walking
     * over the contents of the MasterTableMetadata container.  Access through
     * this "scanner" is thread-safe with the critical section beginning at the
     * object's construction and ending at the object's destruction (in standard
     * RAII style). This critical section defines a period of "ownership" where
     * ownership guarantees that no other thread can perform operations on the
     * container. This includes lookups, inserts, and other "scans."  In this
     * way, a valid instance of this class is said to "own" the container until
     * it is destroyed.  Note, however, that since the MasterTableMetadata
     * container does not provide thread-safe access to the entries themselves,
     * other threads can still access entries for which they have a reference
     * even while a thread owns" the container.
     *
     * This class is MoveConstructible and MoveAssignable, but not
     * CopyConstructible or CopyAssignable.  If an object of this type is moved,
     * "ownership" of the container is passed to the new object and the critical
     * section continues until the new object is destroyed.
     *
     * Typical Usage:
     *
     *  MasterTableMetadata mtm;
     *  ...
     *  // Create inner scope to limit critical section.
     *  {
     *      MasterTableMetadata::scanner sc = mtm.getScanner();
     *      while (sc.hasNext()) {
     *          MasterTableMetadata::TableMetadataEntry* entry = sc.next();
     *          // Do something with entry.
     *      }
     *  } // End of critical section.
     *
     * Note: While C++ style iterator would have been preferable, C++ style
     * iterators use CopyConstructible/CopyAssignable concepts.  A copyable
     * object would not allow for RAII-style "locking."  The unconventional
     * style of this class is meant to make clear to the caller that the
     * "scanner" is not a C++ style iterator and should be treated as such.
     * (Plus, rolling your own C++ iterator is annoying.)
     */
    class scanner {
      PUBLIC:
        explicit scanner(MasterTableMetadata* mtm = NULL);
        scanner(scanner&& other);
        ~scanner();
        scanner& operator=(scanner&& other);
        bool hasNext() const;
        Entry* next();
      PRIVATE:
        MasterTableMetadata* mtm;
        TableMetadataMap::iterator it;

        DISALLOW_COPY_AND_ASSIGN(scanner);
    };
};

} // namespace RAMCloud

#endif /* RAMCLOUD_MASTERTABLEMETADATA_H */
