/* Copyright (c) 2009-2015 Stanford University
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

#include "RecoverySegmentBuilder.h"
#include "Object.h"
#include "RpcResult.h"
#include "SegmentIterator.h"
#include "ServerId.h"
#include "ShortMacros.h"
#include "PreparedOps.h"
#include "TxDecisionRecord.h"

namespace RAMCloud {

/**
 * Construct recovery segments for this replica data splitting data among
 * them according to \a partitions. Walks the replica, finds which recovery
 * segment each should be a part of, and appends it to the segment.
 *
 * \param buffer
 *      Contiguous region of \a length bytes that contains the replica contents.
 * \param length
 *      Bytes which contain replica data starting at \a buffer.
 * \param certificate
 *      Certificate to use to iterate the replica at \a buffer. Used to check
 *      the integrity of the replica metadata before it is walked. Provided by
 *      backups which store it as part of the metadata this keep for each
 *      replica on storage.
 * \param partitions
 *      Describes how the coordinator would like the backup to split up the
 *      contents of the replicas for delivery to different recovery masters.
 *      The partition ids inside each entry act as an index describing which
 *      recovery segment for a particular replica each object should be placed
 *      in.
 * \param numPartitions
 *      Total number of partitions that the replica data will be divided
 *      among.
 * \param recoverySegments
 *      Array of Segments to which objects will be appended to construct
 *      recovery segments. Guaranteed to have numPartitions elements
 *      (not the same as the number of entries in \a partitions).
 * \throw SegmentIteratorException
 *      If the metadata of the replica doesn't match up with the certificate.
 *      Either the replica or the certificate is incorrect, corrupt, or
 *      mismatched.
 * \throw SegmentRecoveryFailedException
 *      If one of the recovery segments couldn't be appended to.
 */
void
RecoverySegmentBuilder::build(const void* buffer, uint32_t length,
                              const SegmentCertificate& certificate,
                              int numPartitions,
                              const ProtoBuf::RecoveryPartition& partitions,
                              Segment* recoverySegments)
{
    SegmentIterator it(buffer, length, certificate);
    it.checkMetadataIntegrity();

    // Buffer must be retained for iteration to provide storage for header.
    Buffer headerBuffer;
    const SegmentHeader* header = NULL;
    for (; !it.isDone(); it.next()) {
        LogEntryType type = it.getType();

        if (type == LOG_ENTRY_TYPE_SEGHEADER) {
            it.appendToBuffer(headerBuffer);
            header = headerBuffer.getStart<SegmentHeader>();
            continue;
        }
        if (type != LOG_ENTRY_TYPE_OBJ && type != LOG_ENTRY_TYPE_OBJTOMB
            && type != LOG_ENTRY_TYPE_SAFEVERSION
            && type != LOG_ENTRY_TYPE_RPCRESULT
            && type != LOG_ENTRY_TYPE_PREP
            && type != LOG_ENTRY_TYPE_PREPTOMB
            && type != LOG_ENTRY_TYPE_TXDECISION
            && type != LOG_ENTRY_TYPE_TXPLIST)
            continue;

        if (header == NULL) {
            DIE("Found log entry before header while "
                "building recovery segments");
        }

        Buffer entryBuffer;
        it.appendToBuffer(entryBuffer);

        uint64_t tableId = -1;
        if (type == LOG_ENTRY_TYPE_SAFEVERSION ||
            type == LOG_ENTRY_TYPE_TXPLIST)
        {
            // Copy SAFEVERSION and ParticipantLists to all the partitions for
            // safeVersion and ParticipantList recovery on all recovery masters
            LogPosition position(header->segmentId, it.getOffset());
            for (int i = 0; i < numPartitions; i++) {
                if (!recoverySegments[i].append(type, entryBuffer)) {
                    LOG(WARNING, "Failure appending to a recovery segment "
                        "for a replica of <%s,%lu>",
                        ServerId(header->logId).toString().c_str(),
                        header->segmentId);
                    throw SegmentRecoveryFailedException(HERE);
                }
            }
            continue;
        }

        KeyHash keyHash = -1;
        if (type == LOG_ENTRY_TYPE_OBJ) {
            Object object(entryBuffer);
            tableId = object.getTableId();
            keyHash = Key::getHash(tableId,
                                   object.getKey(), object.getKeyLength());
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
            ObjectTombstone tomb(entryBuffer);
            tableId = tomb.getTableId();
            keyHash = Key::getHash(tableId,
                                   tomb.getKey(), tomb.getKeyLength());
        } else if (type == LOG_ENTRY_TYPE_RPCRESULT) {
            RpcResult rpcResult(entryBuffer);
            tableId = rpcResult.getTableId();
            keyHash = rpcResult.getKeyHash();
        } else if (type == LOG_ENTRY_TYPE_PREP) {
            PreparedOp op(entryBuffer, 0, entryBuffer.size());
            tableId = op.object.getTableId();
            keyHash = Key::getHash(tableId,
                                   op.object.getKey(),
                                   op.object.getKeyLength());
        } else if (type == LOG_ENTRY_TYPE_PREPTOMB) {
            PreparedOpTombstone opTomb(entryBuffer, 0);
            tableId = opTomb.header.tableId;
            keyHash = opTomb.header.keyHash;
        } else if (type == LOG_ENTRY_TYPE_TXDECISION) {
            TxDecisionRecord decisionRecord(entryBuffer);
            tableId = decisionRecord.getTableId();
            keyHash = decisionRecord.getKeyHash();
        } else {
            LOG(WARNING, "Unknown LogEntry (id=%u)", type);
            throw SegmentRecoveryFailedException(HERE);
        }

        const auto* partition = whichPartition(tableId, keyHash, partitions);
        if (!partition) {
            // This log record doesn't belong to any of the current
            // partitions. This can happen when it takes several passes
            // to complete a recovery: each pass will recover only a subset
            // of the data.
            TEST_LOG("Couldn't place object");
            continue;
        }
        uint64_t partitionId = partition->user_data();

        LogPosition position(header->segmentId, it.getOffset());
        if (!isEntryAlive(position, partition)) {
            LOG(NOTICE, "Skipping object with <tableId, keyHash> of "
                "<%lu,%lu> because it appears to have existed prior "
                "to this tablet's creation.", tableId, keyHash);
            continue;
        }

        if (!recoverySegments[partitionId].append(type, entryBuffer)) {
            LOG(WARNING, "Failure appending to a recovery segment "
                "for a replica of <%s,%lu>",
                ServerId(header->logId).toString().c_str(), header->segmentId);
            throw SegmentRecoveryFailedException(HERE);
        }
    }
}

/**
 * Scan \a buffer for a LogDigest and a TableStats::Digest.  If either exists,
 * replace the contents of \a digestBuffer with it.
 *
 * \param buffer
 *      Contiguous region of \a length bytes that contains the replica contents
 *      from which a log digest should be extracted, if one exists.
 * \param length
 *      Bytes which contain replica data starting at \a buffer.
 * \param certificate
 *      Certificate to use to iterate the replica at \a buffer. Used to check
 *      the integrity of the replica metadata before it is walked. Provided by
 *      backups which store it as part of the metadata this keep for each
 *      replica on storage.
 * \param[out] digestBuffer
 *      Buffer to replace the contents of with a log digest if found. If no
 *      log digest is found the buffer is left unchanged.
 * \param[out] tableStatsBuffer
 *      Buffer to replace the contents of with a table stats digest if found. If
 *      no table stats digest is found the buffer is left unchanged.
 * \return
 *      True if the digest was found and placed in the given buffer, otherwise
 *      false. The return value will not indicate whether a table stats digest
 *      was found or returned.
 */
bool
RecoverySegmentBuilder::extractDigest(const void* buffer, uint32_t length,
                                      const SegmentCertificate& certificate,
                                      Buffer* digestBuffer,
                                      Buffer* tableStatsBuffer)
{
    // If the Segment is malformed somehow, just ignore it. The
    // coordinator will have to deal.
    SegmentIterator it(buffer, length, certificate);
    bool foundDigest = false;
    bool foundTableStats = false;
    try {
        it.checkMetadataIntegrity();
    } catch (SegmentIteratorException& e) {
        LOG(NOTICE, "Replica failed integrity check; skipping extraction of "
            "log digest: %s", e.str().c_str());
        return false;
    }
    while (!it.isDone()) {
        if (it.getType() == LOG_ENTRY_TYPE_LOGDIGEST) {
            digestBuffer->reset();
            it.appendToBuffer(*digestBuffer);
            foundDigest = true;
        }
        if (it.getType() == LOG_ENTRY_TYPE_TABLESTATS) {
            tableStatsBuffer->reset();
            it.appendToBuffer(*tableStatsBuffer);
            foundTableStats = true;
        }
        if (foundDigest && foundTableStats) {
            return true;
        }
        it.next();
    }
    return foundDigest;
}

// - private -

/**
 * Returns true if the entry is alive and should be recovered, otherwise
 * false if it should be ignored.
 *
 * Slays zombies. When migrating a tablet away and then back again, it's
 * possible for old objects to be in the log from the first instance of the
 * tablet that are no longer alive. If the server fails, we need to filter them
 * out (lest they come back to feast on our delicious brains).
 *
 * To combat this, the coordinator keeps the minimum log offset at which valid
 * objects in each tablet may exist. Any objects that fall into that tablet and
 * come before this point could not have been part of the tablet.
 *
 * The situation is slightly complicated by the cleaner, since it may create
 * segments with identifiers ahead of the log head despite containing data
 * from only segments before the head at the time of cleaning. To address
 * this, the master server will roll the log to a new head segment with the
 * largest existing segment identifier and record that as the tablet's minimum
 * offset.
 *
 * \param position
 *      LogPosition indicating where this entry occurred in the log. Used to
 *      determine if it was written before a tablet it could belong to was
 *      created.
 * \param tablet
 *      Tablet to which this entry would belong if were current. It's up to
 *      the caller to ensure that the given entry falls within this tablet
 *      range.
 * \return
 *      true if this object belongs to the given tablet, false if it is from
 *      a previous instance of the tablet and should be dropped.
 */
bool
RecoverySegmentBuilder::isEntryAlive(const LogPosition& position,
                                     const ProtoBuf::Tablets::Tablet* tablet)
{
    LogPosition minimum(tablet->ctime_log_head_id(),
                          tablet->ctime_log_head_offset());
    return position >= minimum;
}

/**
 * Find which of \a partitions this object or tombstone is in.
 *
 * \param tableId
 *      Id of the table which this object was created in.
 * \param keyHash
 *      Hash of the string key contained inside this object.
 * \param partitions
 *      Describes how the coordinator would like the backup to split up the
 *      contents of the replicas for delivery to different recovery masters.
 *      The partition ids inside each entry act as an index describing which
 *      recovery segment for a particular replica each object should be placed
 *      in.
 * \return
 *      Pointer to an entry in \a partitions which this object or tombstone
 *      belongs in. Note this object may still not be safe to include in
 *      a recovered segment depending on its position in the log relative to
 *      the time the partition was assigned to the crashed master (see
 *      isEntryLive()). If the object doesn't fit into any partition given
 *      by \a partitions NULL is returned.
 */
const ProtoBuf::Tablets::Tablet*
RecoverySegmentBuilder::whichPartition(uint64_t tableId, KeyHash keyHash,
                            const ProtoBuf::RecoveryPartition& partitions)
{
    for (int i = 0; i < partitions.tablet_size(); i++) {
        const ProtoBuf::Tablets::Tablet& tablet(partitions.tablet(i));
        if (tablet.table_id() == tableId &&
            (tablet.start_key_hash() <= keyHash &&
            tablet.end_key_hash() >= keyHash)) {
            return &tablet;
        }
    }
    return NULL;
}

} // namespace RAMCloud
