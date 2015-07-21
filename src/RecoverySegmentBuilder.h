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

#include "Common.h"
#include "Buffer.h"
#include "Key.h"
#include "Log.h"
#include "Segment.h"
#include "Tablets.pb.h"

#ifndef RAMCLOUD_RECOVERYSEGMENTBUILDER_H
#define RAMCLOUD_RECOVERYSEGMENTBUILDER_H

namespace RAMCloud {

/**
 * Collects all the logic that must understand the contents of replicas for
 * master recovery on the backups. All functions herein are logically part of
 * master recovery logic on the backups. This is a DMZ; no details about the
 * working of masters or backups other than the replica content/structure
 * should leak into these functions. This keeps these routines easy to test
 * without needing a lot of infrastructure from both modules, AND keeps masters
 * and backups easy to test by moving this tightly intertwined code out.
 *
 * Backups should NEVER peek inside replicas except using these routines which
 * are as loosely coupled to the backup code as possible.
 *
 * All the functions are static. This class cannot be instantiated.
 */
class RecoverySegmentBuilder {
  PUBLIC:
    static void build(const void* buffer, uint32_t length,
                      const SegmentCertificate& certificate,
                      int numPartitions,
                      const ProtoBuf::RecoveryPartition& partitions,
                      Segment* recoverySegments);
    static bool extractDigest(const void* buffer, uint32_t length,
                              const SegmentCertificate& certificate,
                              Buffer* digestBuffer, Buffer* tableStatsBuffer);
  PRIVATE:
    static bool isEntryAlive(const LogPosition& position,
                             const ProtoBuf::Tablets::Tablet* tablet);
    static const ProtoBuf::Tablets::Tablet*
    whichPartition(uint64_t tableId, KeyHash keyHash,
                   const ProtoBuf::RecoveryPartition& partitions);

    // Disallow construction.
    RecoverySegmentBuilder() {}
};

} // namespace RAMCloud

#endif
