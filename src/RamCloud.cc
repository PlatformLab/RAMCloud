
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
    , coordinator(serviceLocator)
    , objectFinder(coordinator) { }

/// \copydoc CoordinatorClient::createTable
void
RamCloud::createTable(const char* name)
{
    coordinator.createTable(name);
}

/// \copydoc CoordinatorClient::dropTable
void
RamCloud::dropTable(const char* name)
{
    coordinator.dropTable(name);
}

/// \copydoc CoordinatorClient::openTable
uint32_t
RamCloud::openTable(const char* name)
{
    return coordinator.openTable(name);
}

/// \copydoc MasterClient::create
uint64_t
RamCloud::create(uint32_t tableId, const void* buf, uint32_t length,
                 uint64_t* version, bool async)
{
    return Create(*this, tableId, buf, length, version, async)();
}

/// \copydoc CoordinatorClient::ping
void
RamCloud::ping()
{
    coordinator.ping();
}

/// \copydoc MasterClient::read
void
RamCloud::read(uint32_t tableId, uint64_t id, Buffer* value,
               const RejectRules* rejectRules, uint64_t* version)
{
    MasterClient master(objectFinder.lookup(tableId, id));
    master.read(tableId, id, value, rejectRules, version);
}

/// \copydoc MasterClient::remove
void
RamCloud::remove(uint32_t tableId, uint64_t id,
                 const RejectRules* rejectRules, uint64_t* version)
{
    MasterClient master(objectFinder.lookup(tableId, id));
    master.remove(tableId, id, rejectRules, version);
}

/// \copydoc MasterClient::write
void
RamCloud::write(uint32_t tableId, uint64_t id,
                const void* buf, uint32_t length,
                const RejectRules* rejectRules, uint64_t* version,
                bool async)
{
    Write(*this, tableId, id, buf, length, rejectRules, version, async)();
}

}  // namespace RAMCloud
