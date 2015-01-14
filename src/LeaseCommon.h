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

namespace RAMCloud {

namespace LeaseCommon {

/// Defines the length of time (in nanoseconds) that a lease will be extended
/// upon renewal.
const uint64_t LEASE_TERM_US = 300*1e6;      // 5 min = 300,000,000 us

/// Defines the remaining lease time below which a given lease should not be
/// assumed renewable.  Modules that rely on leases should not use leases that
/// fall into this category; it is possible that the lease may expire before
/// the lease dependent operation completes (e.g. linearizable rpcs).  This
/// value, however, should be set large enough so that (during all expected
/// failure modes) we are very confident that a lease CAN be renewed if its
/// remaining lease time is above this value.  In other words, the value should
/// be larger than the answer to the question, "what is the most time a lease
/// renewal request will take during all expected failure cases?"
static const uint64_t DANGER_THRESHOLD_US = 500;

/// Defines the remaining lease time below which a module should start trying
/// to renew the lease.  During this period, the lease has probably not expired
/// so it is safe to perform the renewals asynchronously.  This value should
/// be set conservatively to around half the LeaseManager::LEASE_TERM_US.
static const uint64_t RENEW_THRESHOLD_US = 150*1e6;     // 2.5 min



} // namespace LeaseCommon

} // namespace RAMCloud


#endif  /* RAMCLOUD_LEASECOMMON_H */

