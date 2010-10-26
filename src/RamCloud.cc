
/* Copyright (c) 2010 Stanford University
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

#include "RamCloud.h"
#include "MasterClient.h"

namespace RAMCloud {

/**
 * Construct a RamCloud for a particular service: opens a connection with the
 * service.
 *
 * \param serviceLocator
 *      The service locator for the coordinator.
 *      See \ref ServiceLocatorStrings.
 * \exception CouldntConnectException
 *      Couldn't connect to the server.
 */
RamCloud::RamCloud(const char* serviceLocator)
    : status(STATUS_OK)
    , counterValue(0)
    , counterType(PERF_COUNTER_INC)
    , beginMark(MARK_NONE)
    , endMark(MARK_NONE)
    , coordinator(serviceLocator)
    , objectFinder(coordinator) { }

/// \copydoc MasterClient::create
uint64_t
RamCloud::create(uint32_t tableId, const void* buf, uint32_t length,
                 uint64_t* version)
{
    MasterClient master(objectFinder.lookupHead(tableId));
    master.selectPerfCounter(counterType, beginMark, endMark);
    uint64_t retVal = master.create(tableId, buf, length, version);
    counterValue = master.counterValue;
    return retVal;
}

/// \copydoc MasterClient::createTable
void
RamCloud::createTable(const char* name)
{
    MasterClient master(objectFinder.lookupHead(0));
    master.selectPerfCounter(counterType, beginMark, endMark);
    master.createTable(name);
    counterValue = master.counterValue;
}

/// \copydoc MasterClient::dropTable
void
RamCloud::dropTable(const char* name)
{
    MasterClient master(objectFinder.lookupHead(0));
    master.selectPerfCounter(counterType, beginMark, endMark);
    master.dropTable(name);
    counterValue = master.counterValue;
}

/// \copydoc MasterClient::openTable
uint32_t
RamCloud::openTable(const char* name)
{
    MasterClient master(objectFinder.lookupHead(0));
    master.selectPerfCounter(counterType, beginMark, endMark);
    uint32_t retVal = master.openTable(name);
    counterValue = master.counterValue;
    return retVal;
}

/// \copydoc CoordinatorClient::ping
void
RamCloud::ping()
{
    coordinator.selectPerfCounter(counterType, beginMark, endMark);
    coordinator.ping();
    counterValue = coordinator.counterValue;
}

/// \copydoc MasterClient::read
void
RamCloud::read(uint32_t tableId, uint64_t id, Buffer* value,
               const RejectRules* rejectRules, uint64_t* version)
{
    MasterClient master(objectFinder.lookup(tableId, id));
    master.selectPerfCounter(counterType, beginMark, endMark);
    master.read(tableId, id, value, rejectRules, version);
    counterValue = master.counterValue;
}

/// \copydoc MasterClient::remove
void
RamCloud::remove(uint32_t tableId, uint64_t id,
                 const RejectRules* rejectRules, uint64_t* version)
{
    MasterClient master(objectFinder.lookup(tableId, id));
    master.selectPerfCounter(counterType, beginMark, endMark);
    master.remove(tableId, id, rejectRules, version);
    counterValue = master.counterValue;
}

/// \copydoc MasterClient::write
void
RamCloud::write(uint32_t tableId, uint64_t id,
                const void* buf, uint32_t length,
                const RejectRules* rejectRules, uint64_t* version)
{
    MasterClient master(objectFinder.lookup(tableId, id));
    master.selectPerfCounter(counterType, beginMark, endMark);
    master.write(tableId, id, buf, length, rejectRules, version);
    counterValue = master.counterValue;
}

}  // namespace RAMCloud
