/* Copyright (c) 2015 Stanford University
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

#ifndef RAMCLOUD_LEASECOMMON_H
#define RAMCLOUD_LEASECOMMON_H

#include "ClusterTime.h"

namespace RAMCloud {

/**
 * Provides a common place to define related constants for lease related modules
 * such as ClientLease and ClientLeaseAuthority.
 */
namespace LeaseCommon {

/// Defines the length of time that a lease will be extended upon renewal.
/// Currently set to 30 min (1,800,000,000,000 ns).
static const ClusterTimeDuration LEASE_TERM =
                                ClusterTimeDuration::fromNanoseconds(1800*1e9);

/// Defines the remaining lease time below which a module should start trying
/// to renew the lease.  During this period, the lease has probably not expired
/// so it is safe to perform the renewals asynchronously.  This value should
/// be set conservatively to around half the LeaseCommon::LEASE_TERM.
/// Currently set to 15 min (900,000,000,000 ns).
static const ClusterTimeDuration RENEW_THRESHOLD =
                                ClusterTimeDuration::fromNanoseconds(900*1e9);

/// Defines the remaining lease time below which the lease should be considered
/// possibly invalid; no new linearizable RPCs should be issued with this lease
/// until it can be renewed or replaced (lease expiration does not cause
/// problems if there are no outstanding RPCs).  This value is used to ensure
/// a client "thinks" the lease is invalid before it actually expires; the value
/// must be large enough so that this property holds even if the client and
/// coordinator's clocks are loosely synchronized.  However, the value should
/// also be small enough so that this DANGER threshold is not normally reached
/// if the lease is renewed when the RENEW threshold is reached.
/// Currently set to 1 min (60,000,000,000 ns).
static const ClusterTimeDuration DANGER_THRESHOLD =
                                ClusterTimeDuration::fromNanoseconds(60*1e9);

} // namespace LeaseCommon

} // namespace RAMCloud


#endif  /* RAMCLOUD_LEASECOMMON_H */

