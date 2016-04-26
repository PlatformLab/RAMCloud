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

#include <cstdlib>
#include <cstring>

#include "SkipList.h"
#include "ObjectManager.h"

namespace RAMCloud {
/**
 * Constructor
 *
 * \param objMgr - ObjectManager to perform reads and writes from
 * \param tableId - Backing table to store the SkipList data structure
 * \param nextNodeId - (optional) determines what the nodeId of the next
 *                      new node to be written will be. Typically, this is
 *                      specified after a recovery to indicate the range of
 *                      unused nodeIds
 */
SkipList::SkipList(ObjectManager *objMgr, uint64_t tableId, uint64_t nextNodeId)
    : tableId(tableId)
    , objMgr(objMgr)
    , nextNodeId(nextNodeId)
    , logBuffer()
    , numEntries(0)
    , nodesWritten(0)
    , nodesRead(0)
    , bytesWritten(0)
    , bytesRead(0)
{
    // If nextNodeId is passed in, that means a recovery occurred and the head
    // is already in the log.
    if (nextNodeId != HEAD_NODEID + 1)
        return;

    // If it's not passed in, double check that there isn't already a head
    // node before creating a new one.
    Buffer buffer;
    Node *potentialHead = readNode(HEAD_NODEID, &buffer, true);
    if (potentialHead != NULL)
        return;

    uint32_t nodeSize =
            sizeof(SkipList::Node) + MAX_FORWARD_POINTERS*sizeof(NodeId);
    Node* head = static_cast<Node*>(buffer.alloc(nodeSize));
    bzero(head, nodeSize);
    head->levels = MAX_FORWARD_POINTERS;
    writeNode(head, HEAD_NODEID);
    flush();
}

/**
 * Flushes the writes buffered in logBuffer to the log.
 */
void
SkipList::flush() {
    if (!objMgr->flushEntriesToLog(&logBuffer, numEntries)) {
        RAMCLOUD_DIE("Unable to flush SkipList entries to log!"
                " %u nodes totaling %u bytes buffered.",
                numEntries, logBuffer.size());
    }
}

/**
 * Reads a Node from objectManager with the specified NodeId. Returns NULL
 * when the Node is not found.
 *
 * \param nodeId - NodeId to look up
 * \param outBuffer (out) - buffer to write the Node object to
 * \param ignoreFailure - if false, this function will not log lookup errors
 *                          (set to false when a lookup failure is expected)
 *
 * \return - a pointer to a copy of the Node in the log or NULL if not found
 */
SkipList::Node*
SkipList::readNode(NodeId nodeId, Buffer *outBuffer, bool ignoreFailure)
{
    Key key(tableId, &nodeId, sizeof(NodeId));
    uint32_t nodeOffset = outBuffer->size();
    Status status = objMgr->readObject(key, outBuffer, NULL, NULL, true);
    if (status != STATUS_OK) {
        if (!ignoreFailure) {
            RAMCLOUD_LOG(ERROR, "Could not read SkipList Node %lu "
                    "from table %lu:\"%s\"",
                    nodeId, tableId, statusToString(status));
        }
        return NULL;
    }

    // The object may be fragmented in the log, so we reconstitute it here.
    // Note: It's okay to modify the data returned from getRange() because
    //      a) The node doesn't change size
    //      b) writeNode() takes in a pointer to contiguous space, not a buffer
    Node *ptr = static_cast<Node*>(outBuffer->getRange(
                                   nodeOffset, outBuffer->size() - nodeOffset));

    RAMCLOUD_LOG(DEBUG, "Read node %lu of size %u bytes from table %lu",
                                        nodeId, ptr->size(), tableId);

    bytesRead += ptr->size();
    nodesRead++;
    return ptr;
}

/**
 * Buffers a node to write to the log. The user must invoke flush() to
 * complete the write.
 *
 * \param node - Node to write to the log
 * \param nodeId (optional) - nodeId to assign to the Node. If unset, the
 *                              SkipList will assign one.
 *
 * \return - NodeId of the buffered Node to be written
 */
uint64_t
SkipList::writeNode(Node *node, NodeId nodeId)
{
    if (nodeId == INVALID_NODEID_1)
        nodeId = nextNodeId++;

    Buffer buffer;
    Key key(tableId, &nodeId, sizeof(NodeId));
    Object object(key, node, node->size(), 1, 0, buffer);

    bool tombstoneAdded = false;
    Status status = objMgr->prepareForLog(object, &logBuffer, NULL,
                                                            &tombstoneAdded);
    numEntries += (tombstoneAdded) ? 2 : 1;

    if (status != STATUS_OK) {
        RAMCLOUD_DIE("Could not stage SkipList node %lu "
                "write for table %lu: %s",
                nodeId, tableId, statusToString(status));
    }

    RAMCLOUD_LOG(DEBUG, "Staged node write %lu of size %u byes for table %lu",
                                nodeId, node->size(), tableId);

    bytesWritten += node->size();
    nodesWritten++;
    return nodeId;
}

/**
 * Buffers a delete of a Node object from the log. The user must invoke flush()
 * to complete the write.
 *
 * \param nodeId - NodeId to delete
 */
void
SkipList::freeNode(NodeId nodeId)
{
    if (nodeId <= HEAD_NODEID) {
        RAMCLOUD_LOG(ERROR, "SkipList attempted to delete an invalid node %lu",
                nodeId);
        return;
    }

    Key key(tableId, &nodeId, sizeof(NodeId));
    Status status = objMgr->writeTombstone(key, &logBuffer);
    if (status != STATUS_OK) {
        RAMCLOUD_DIE("Could not stage node %lu delete from "
                "table %lu into buffer: \"%s\"",
                nodeId, tableId, statusToString(status));
    }

    numEntries++;
}

/**
 * Finds the node at every level of the SkipList that is less and/or equal to
 * the entry passed in. If beginInclusive is true, the result will contain
 * nodes that are strictly less than the entry.
 *
 * \param entry - IndexEntry containing the key to search for
 * \param result (out) - An array of <Node*, NodeId> pairs for the nodes at
 *                       every level
 * \param buffer (out) - A buffer to use for the findLower operation
 * \param beginInclusive - true means that the matching node is included in the
 *                         result array
 * \param nextNode (out) - Sets a pointer to the next Node after the Node
 *                          in the lowest level of result (out).
 *
 * \return - true if the exact match for a node is found and is
 *           returned via nextNode
 */
bool
SkipList::findLower(IndexEntry entry, std::pair<Node*, NodeId>* result,
        Buffer *buffer, bool beginInclusive, Node** nextNode)
{
    NodeId prevNodeId = HEAD_NODEID;
    Node *prev = readNode(prevNodeId, buffer);
    bool exactMatchFound = false;
    Node *next = NULL;

    if (nextNode)
        *nextNode = NULL;

    // Save the nodeId that contains an entry equal to or larger than our
    // search entry so don't do duplicate reads.
    NodeId upperBound = INVALID_NODEID_1;

    for (int level = MAX_FORWARD_POINTERS - 1; level >= 0; --level) {
        NodeId nextNodeId = prev->next()[level];

        // Only search down a level if it's not the same as the one above.
        if (level == MAX_FORWARD_POINTERS - 1 ||
                nextNodeId != prev->next()[level + 1])
        {
            while (nextNodeId != INVALID_NODEID_1 &&
                    nextNodeId != upperBound) {
                next = readNode(nextNodeId, buffer);
                IndexEntry other = {next->key(), next->keyLen, next->pkHash};
                int cmp = compareIndexEntries(other, entry);
                if (cmp > 0) {
                    if (nextNode)
                        *nextNode = next;

                    upperBound = nextNodeId;
                    break;
                }

                if (cmp == 0) {
                    exactMatchFound = true;
                    if (beginInclusive) {
                        if (nextNode)
                            *nextNode = next;

                        upperBound = nextNodeId;
                        break;
                    }
                }

                prev = next;
                prevNodeId = nextNodeId;
                nextNodeId = prev->next()[level];
            }
        }
        result[level] = {prev, prevNodeId};
    }
    return exactMatchFound;
}

void
SkipList::insert(IndexEntry entry, uint8_t levels)
{
    assert(levels <= MAX_FORWARD_POINTERS && levels > 0);
    Buffer buffer;
    std::pair<Node*, NodeId> result[MAX_FORWARD_POINTERS];
    findLower(entry, result, &buffer, true);

    NodeId newNodeId = this->nextNodeId++;
    Node* newNode = static_cast<Node*>(buffer.alloc(
            sizeof(SkipList::Node) + levels*sizeof(NodeId) + entry.keyLen));
    newNode->levels = levels;
    newNode->keyLen = entry.keyLen;
    newNode->pkHash = entry.pkHash;

    // Note: levels must to be set invoking key(), otherwise the offsets will
    // not be computed correctly and cause a segfault.
    memcpy(newNode->key(), entry.key, entry.keyLen);

    for (uint8_t level = 0; level < newNode->levels; ++level) {
        newNode->next()[level] = result[level].first->next()[level];
        result[level].first->next()[level] = newNodeId;

        // Only write the node if it's the last time it will be modified
        if (level == newNode->levels - 1 ||
                result[level].second != result[level + 1].second) {
            writeNode(result[level].first, result[level].second);
        }
    }

    writeNode(newNode, newNodeId);
    flush();
}

/**
 * Insert an IndexEntry into the SkipList data structure
 *
 * \param entry - IndexEntry to insert
 */
void
SkipList::insert(IndexEntry entry)
{
    // Determine the number of levels the Node will participate in
    uint8_t levels;
    for (levels = 1; levels < MAX_FORWARD_POINTERS; ++levels) {
        if (randomNumberGenerator(1000)/static_cast<double>(1000)
                                                                > PROBABILITY)
            break;
    }

    insert(entry, levels);
}

/**
 * Removes an IndexEntry from the SkipList data structure.
 *
 * \param entry - Entry to remove
 *
 * \return - true if the entry was removed
 */
bool
SkipList::remove(IndexEntry entry) {
    Buffer buffer;
    std::pair<Node*, NodeId> result[MAX_FORWARD_POINTERS];
    Node *exactNode = NULL;
    bool exactMatchFound
                    = findLower(entry, result, &buffer, true, &exactNode);

    if (!exactMatchFound)
        return false;

    NodeId exactMatchNodeId = result[0].first->next()[0];
    for (uint8_t level = 0; level < exactNode->levels; ++level) {
        Node *prev = result[level].first;

        // Try to propagate the pointer reassignment as far up as possible
        // within the prev node before writing back.
        for (; level < prev->levels && level < exactNode->levels; ++level)
            prev->next()[level] = exactNode->next()[level];

        --level; // Bring level back to something within range.

        writeNode(result[level].first, result[level].second);
    }

    freeNode(exactMatchNodeId);
    flush();
    return true;
}

/**
 * Checks a node within a Buffer to see if it indexes a IndexEntry that
 * is greater than or equal to e.
 *
 * \param nodeObjectValue - Buffer containing the Node only
 * \param e - Index Entry to compare against
 *
 * \return - true if Node >= e
 */
bool
SkipList::isGreaterOrEqual(Buffer *nodeObjectValue, IndexEntry e)
{
    Node *node = nodeObjectValue->getStart<Node>();
    IndexEntry nodeEntry = {node->key(), node->keyLen, node->pkHash};
    return compareIndexEntries(nodeEntry, e) >= 0;
}
/**
 * Constructor for a range_iterator. Equivalent to SkipList.end();
 */
SkipList::range_iterator::range_iterator()
    : sk(NULL)
    , curr(NULL)
    , end()
    , endInclusive(false)
    , buffer()
    , currEntry({NULL, 0, 0UL})
{}

/**
 * Constructor for range_iterator starting at a certain SkipList Node.
 *
 * \param sk - SkipList to iterate through
 * \param curr - current SkipList::Node
 * \param end - IndexEntry to stop at/before
 * \param endInclusive - if true, stops at end, else stops before
 */
SkipList::range_iterator::range_iterator(SkipList* sk, SkipList::Node* curr,
                                            IndexEntry end, bool endInclusive)
    : sk(sk)
    , curr(curr)
    , end(end)
    , endInclusive(endInclusive)
    , buffer()
    , currEntry()
{
    buffer.append(end.key, end.keyLen);
    end.key = buffer.getOffset<const char>(0);

    if (curr == NULL)
        return;

    this->curr = static_cast<Node*>(buffer.alloc(curr->size()));
    memcpy(this->curr, curr, curr->size());

    currEntry.key = this->curr->key();
    currEntry.keyLen = this->curr->keyLen;
    currEntry.pkHash = this->curr->pkHash;

    int cmp = compareIndexEntries(currEntry, end);
    if (cmp > 0 || (cmp == 0 && !endInclusive)) {
        currEntry = { NULL, 0, 0 };
        this->curr = NULL;
    }
}

/**
 * Copy constructor
 * \param other - other range_iterator to copy
 */
SkipList::range_iterator::range_iterator(const SkipList::range_iterator& other)
    : sk(other.sk)
    , curr(NULL)
    , end()
    , endInclusive(other.endInclusive)
    , buffer()
    , currEntry()
{
    RAMCLOUD_TEST_LOG("Copy Constructor Invoked");
    Buffer& otherBuffer = const_cast<Buffer&>(other.buffer);
    buffer.append(&otherBuffer, 0);

    end.key = buffer.getStart<const char>();
    end.keyLen = other.end.keyLen;
    end.pkHash = other.end.pkHash;

    if (other.curr == NULL) {
        curr = NULL;
        currEntry.key = 0;
        currEntry.keyLen = 0;
        currEntry.pkHash = 0;
    } else {
        curr = buffer.getOffset<Node>(end.keyLen);
        currEntry.key = curr->key();
        currEntry.keyLen = curr->keyLen;
        currEntry.pkHash = curr->pkHash;
    }
}

/**
 * Assignment constructor
 *
 * \param other - range_iterator to copy
 */
SkipList::range_iterator&
SkipList::range_iterator::operator=(SkipList::range_iterator &other)
{
    RAMCLOUD_TEST_LOG("Assignment Operator Invoked");
    sk = other.sk;
    endInclusive = other.endInclusive;

    buffer.reset();
    buffer.append(&other.buffer, 0);

    end.key = buffer.getStart<const char>();
    end.keyLen = other.end.keyLen;
    end.pkHash = other.end.pkHash;

    if (other.curr == NULL) {
        curr = NULL;
        currEntry.key = 0;
        currEntry.keyLen = 0;
        currEntry.pkHash = 0;
    } else {
        curr = buffer.getOffset<Node>(end.keyLen);
        currEntry.key = curr->key();
        currEntry.keyLen = curr->keyLen;
        currEntry.pkHash = curr->pkHash;
    }

    return *this;
}

/**
 * Advances the iterator
 *
 * \return - self
 */
SkipList::range_iterator&
SkipList::range_iterator::operator++()
{
    if (curr == NULL)
        return *this;

    NodeId nextNodeId = curr->next()[0];
    if (nextNodeId == SkipList::INVALID_NODEID_1) {
        currEntry = { NULL, 0, 0 };
        curr = NULL;
        return *this;
    }

    buffer.truncate(end.keyLen);
    curr = sk->readNode(nextNodeId, &buffer);
    currEntry = { curr->key(), curr->keyLen, curr->pkHash };
    int cmp = compareIndexEntries(currEntry, end);
    if (cmp > 0 || (cmp == 0 && !endInclusive)) {
        currEntry = { NULL, 0, 0 };
        curr = NULL;
    }

    return *this;
}

/**
 * Equality checker based on iterator position.
 *
 * \param rhs - other range_iterator to compare against
 *
 * \return true if equal
 */
bool
SkipList::range_iterator::operator==(const range_iterator& rhs) {
    if ((curr == NULL && rhs.curr != NULL) ||
            (curr != NULL && rhs.curr == NULL))
    {
        return false;
    } else if (curr == NULL && rhs.curr == NULL) {
        return true;
    } else {
        // If the next pointers are the same, they should be in the
        // same position in the linked list
        return curr->next()[0] == rhs.curr->next()[0];
    }
}

/**
 * Returns a range_iterator that iterates between begin and end with
 * adjustable inclusivity.
 *
 * \param begin - IndexEntry to begin iteration at
 * \param end - IndexEntry to stop iteration at
 * \param out(out) - range_iterator that's constructed
 * \param begInclusive - if true, includes begin, else excludes
 * \param endInclusive - if true, includes end, else excludes
 */
void
SkipList::findRange(IndexEntry begin, IndexEntry end, range_iterator *out,
        bool begInclusive, bool endInclusive)
{
    Buffer buffer;
    std::pair<Node*, NodeId> results[MAX_FORWARD_POINTERS];
    Node *nextNode = NULL;
    findLower(begin, results, &buffer, begInclusive, &nextNode);
    new(out) range_iterator(this, nextNode, end, endInclusive);
}

/**
 * For debugging purposes, convenience function to print the SkipList
 * data structure to stdout.
 */
void
SkipList::print()
{
    const int MAX_LEN = 7;
    const int TOTAL_SPACE = MAX_LEN*11 + MAX_LEN/2;
    NodeId currId = HEAD_NODEID;

    while (currId != INVALID_NODEID_1) {
        Buffer buffer;
        Node *curr = readNode(currId, &buffer);
        printf("%-*lu", MAX_LEN, currId);
        printf("%-*u", MAX_LEN/2, curr->levels);
        int spaceLeft = TOTAL_SPACE - MAX_LEN - MAX_LEN/2;

        for (int i = 0; i < curr->levels; ++i) {
            printf("%-*lu", MAX_LEN, curr->next()[i]);
            spaceLeft -= MAX_LEN;
        }
        printf("%*s", spaceLeft, "");
        printf("|%s|%lu|\r\n", curr->key(), curr->pkHash);

        currId = curr->next()[0];
    }
}

} // namespace RAMCloud
