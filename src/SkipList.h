/* Copyright (c) 2016 Stanford University
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

#ifndef RAMCLOUD_SKIPLIST_H
#define RAMCLOUD_SKIPLIST_H

#include "Common.h"
#include "Minimal.h"
#include "ObjectManager.h"

namespace RAMCloud {
/**
 * SkipList is a probabilistic indexing structure that comprises of multiple,
 * hierarchical singly linked lists where the bottom-most level (index 0 in this
 * implementation) contains all the index entries and successive levels contain
 * subsets of the level below. The number of levels an index entry participates
 * in is determined at insert time by flipping a weighted coin (controlled via
 * the constant PROBABILITY) and observing the number of successive 'heads'.
 * For example, if the probability is 0.5, then we expect each successive level
 * to have, on average, half the number of index entries as the one before it.
 *
 * Skiplist's probabilistic nature gives it an advantage over other index data
 * structures like B+ Tree's which require complicated logic to merge/split
 * nodes to maintain perfect balance. Skiplists may become unbalanced, but it
 * is unlikely given a well chosen probability. One down side of SkipLists is
 * that it uses linked lists which tend to trash a CPU's cache as it traverses
 * the data structure.
 *
 * ======== WARNING ======
 *
 * This implementation is built as an experiment and alternative to the B+ tree
 * within RAMCloud. Although the SkipList itself is well tested, it is not
 * perfectly integrated with other code (such as IndexletManager). For example,
 * SkipList is not properly integrated with splitAndMigrateIndexlet and thus
 * will crash the server. However, it is sufficiently integrated such that
 * index create/insert/lookups/deletes should work. With that in mind, this
 * class should only be used for testing and not production.
 */


// API level data structure that stores fields relevant index entries
struct IndexEntry {
    const void *key;
    uint16_t keyLen;
    uint64_t pkHash;
};
class SkipList {
  PUBLIC:
    /// Controls the probability that a successive level in the skip list
    /// will have an entry in the previous level. The number 0.5 was empirically
    /// chosen after trying a few values in the indexBasic ClusterPerf test.
    /// John also retroactively determined this was the best value since
    /// SkipList Node reads are expensive so we want to read as few as possible
    /// on each level, which makes 0.5 ideal.
    static CONSTEXPR_VAR double PROBABILITY = 0.5;

#if (TESTING == false)
    /// Controls the maximum number of linked list levels the SkipList will have
    /// The number 50 was empirically determined via the indexBasic ClusterPerf
    /// test, although ideally the number of MAX_FORWARD_POINTERS should be
    /// unbounded to keep up with an ever growing index structure. That being
    /// said, 50 seems okay for at least 1M objects.
    static const uint8_t MAX_FORWARD_POINTERS = 50;
#else
    static const uint8_t MAX_FORWARD_POINTERS = 10;
#endif
    typedef uint64_t NodeId;

    /**
     * Compares two IndexEntry's first by their keys and then their
     * pkhashes.
     *
     * \param a - first entry to compare
     * \param b - second entry to compare against
     *
     * \return - 0 if equal, -1 if a < b, and 1 if a > b
     */
    static int compareIndexEntries(IndexEntry a, IndexEntry b) {
        const char* strA = static_cast<const char*>(a.key);
        const char* strB = static_cast<const char*>(b.key);
        uint16_t minLength = std::min(a.keyLen, b.keyLen);

        int cmp = strncmp(strA, strB, minLength);
        if (cmp != 0)
            return cmp;

        if (a.keyLen != b.keyLen)
            return a.keyLen - b.keyLen;

        // Got caught by this twice.
        if (a.pkHash == b.pkHash) {
            return 0;
        } else if (a.pkHash > b.pkHash) {
            return 1;
        } else {
            return -1;
        }
    }

  PRIVATE:
    // Special NodeID for demarking that there is no next node within the
    // SkipList. It is appended with a _1 because it clashes with the B+ Tree's
    // namespace.
    static const NodeId INVALID_NODEID_1 = 0;

    // Special NodeId for the first node in the skip list structure
    static const NodeId HEAD_NODEID = 100;

    /**
     * Keeps track of an index entry within the hierarchical SkipList. The Node
     * size is varied since it can participate in a variable number of linked
     * list levels and thus need a variable number of forward pointers.
     */
    struct Node {
        // The number of linked lists this node participates in
        uint8_t levels;

        // The length of the secondary key of the object that's being indexed
        uint16_t keyLen;

        // The primary key hash belonging of the object that's being indexed
        uint64_t pkHash;

        // Stores the variable number of forward pointers
        // followed by the secondary key of the object that's being indexed
        char buffer[];

        // Returns an array of NodeId's representing the pointers to the next
        // Node's in the hierarchical linked list. The index 0 in the array
        // represents the lowest level (i.e. the one that contains all entries)
        NodeId* next() {
            return static_cast<NodeId*>(reinterpret_cast<void*>(&buffer));
        }

        // Returns the secondary key being indexed by the node
        char* key() {
            return static_cast<char*>(&buffer[levels * sizeof(NodeId)]);
        }

        // Returns the full size of the node with the variable components
        uint32_t size() {
            return sizeof32(Node) + keyLen + levels * sizeof32(NodeId);
        }
    };

    Node* readNode(NodeId nodeId, Buffer *outBufer, bool isConstructor = false);
    NodeId writeNode(Node* node, NodeId nodeId = INVALID_NODEID_1);
    void freeNode(NodeId nodeId);

    /// TableId of the backing table used to store the indexing structure
    uint64_t tableId;

    /// Pointer to object manager to read/write Node's
    ObjectManager *objMgr;

    /// Indicates what the next nodeId will be if the SkipList needs to write a
    /// new Node. A special value of HEAD_NODEID indicates an empty SkipList
    uint64_t nextNodeId;

    /// Used to buffer multiple Node writes into a batch for objectManager
    Buffer logBuffer;

    /// Corresponds to the number of entries stored in logBuffer
    uint32_t numEntries;


    /// Number of node writes (includes overwrites)
    uint64_t nodesWritten;

    /// Number of nodes read (includes nodes traversed during lookups)
    uint64_t nodesRead;

    /// Number of bytes written corresponding with node writes
    uint64_t bytesWritten;

    /// Number of bytes read corresponding with nodeReads
    uint64_t bytesRead;

  PUBLIC:

    /**
     * A forward iterator-like class for SkipList that limits its forward
     * iteration by checking the current position's value with an IndexEntry
     * passed in at compile time.
     *
     * The iterator is invalidated upon updates to the SkipList.
     */
    class range_iterator {
        /// STL-magic - iterator category
        typedef std::forward_iterator_tag iterator_category;

      public:
        /// SkipList structure that this iterator is attached to
        SkipList *sk;

        /// Marks the position on the 0th level linked list of the SkipList
        Node *curr;

        /// The greatest IndexEntry this will iterate up to or right before
        /// controlled by endInclusive
        IndexEntry end;

        /// If true, the iterator will iterate up to the end value. Otherwise,
        /// it will iterate up to everything less than end.
        bool endInclusive;

        /// Stores the end.key and the Node curr
        Buffer buffer;

        /// Current entry corresponding to Node curr
        IndexEntry currEntry;

        range_iterator();
        range_iterator(SkipList* sk, Node* curr,
                        IndexEntry end, bool endInclusive);
        range_iterator(const range_iterator& other);

        range_iterator& operator=(range_iterator &other);
        range_iterator& operator++();

        const IndexEntry operator*() { return currEntry; }
        const IndexEntry *operator->() { return &currEntry; }

        bool operator==(const range_iterator& rhs);
        bool operator!=(const range_iterator& rhs) {return !(curr == rhs.curr);}
    };

    SkipList(ObjectManager *objMgr, uint64_t tableId,
                uint64_t nextNodeId = HEAD_NODEID + 1);
    ~SkipList() {}

  PRIVATE:
    bool findLower(IndexEntry entry, std::pair<Node*, NodeId>* result,
                    Buffer *buffer, bool beginInclusive,
                    Node** nextNode = NULL);

  PUBLIC:
    void insert(IndexEntry entry, uint8_t levels); // For testing only.
    void insert(IndexEntry entry);
    void flush();
    bool remove(IndexEntry entry);

    static bool isGreaterOrEqual(Buffer *nodeObjectValue, IndexEntry e);
    uint64_t getNextNodeId() { return nextNodeId; }
    void setNextNodeId(uint64_t nextNodeId) { this->nextNodeId = nextNodeId; }
    void print();

    void findRange(IndexEntry begin, IndexEntry end, range_iterator *out,
        bool begInclusive = true, bool endInclusive = true);
    range_iterator end() { return range_iterator(); }

    DISALLOW_COPY_AND_ASSIGN(SkipList);
};

} // namespace RAMCloud

#endif // !RAMCLOUD_SKIPLIST_H

