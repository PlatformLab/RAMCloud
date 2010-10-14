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

/**
 * \file
 * This file provides a C wrapper around the RAMCloud client code.
 */

#include "RamCloudClient.h"
#include "RamCloudCClient.h"
#include "ClientException.h"

using namespace RAMCloud;

/**
 * Wrapper structure for C clients.
 */
struct rc_client {
    RamCloudClient* client;
};

/**
 * Create a new client connection to a RAMCloud cluster.
 *
 * \param serverLocator
 *      A service locator string for the desired server host.
 * \param[out] newClient
 *      A pointer to the new client connection is returned here
 *      if the return value is STATUS_OK.  This pointer is passed
 *      to other "rc_" functions to invoke RAMCloud operations.
 *
 * \return
 *      STATUS_OK or STATUS_COULDNT_CONNECT.
 */
Status rc_connect(const char* serverLocator, struct rc_client** newClient)
{
    struct rc_client* client = new rc_client;
    try {
        client->client = new RamCloudClient(serverLocator);
    } catch (CouldntConnectException& e) {
        delete client;
        return e.status;
    }
    *newClient = client;
    return STATUS_OK;
}

/**
 * Create a C rc_client based on an existing C++ RamCloudClient.  Intended
 * primarily for testing.
 *
 * \param existingClient
 *      An existing RamCloudClient object that will be used for communication
 *      with the server.
 * \param[out] newClient
 *      A pointer to the new client connection is returned here.
 *      This pointer is passed to other "rc_" functions to invoke
 *      RAMCloud operations.
 *
 * \return
 *      STATUS_OK or STATUS_COULDNT_CONNECT.
 */
Status rc_connectWithClient(
        struct RamCloudClient* existingClient,
        struct rc_client** newClient)
{
    struct rc_client* client = new rc_client;
    client->client = existingClient;
    *newClient = client;
    return STATUS_OK;
}

/*
 * End a connection to a RAMCloud cluster and delete the client object.
 *
 * \param client
 *      Handle for the RAMCloud connection.  This is freed by this
 *      function, so it should not be used again after the function
 *      returns.
 */
void rc_disconnect(struct rc_client* client) {
    delete client->client;
    delete client;
}

/**
 * Cancel any performance counter request previously specified by a call to
 * rc_selectPerfCounter.  After this call, future RPCs will not return
 * any performance metrics
 */
void
rc_clearPerfCounter(struct rc_client* client)
{
    client->client->clearPerfCounter();
}

// Most of the methods below are all just wrappers around the corresponding
// RamCloudClient methods, except for the following differences:
// * RPC requests here return Status values, whereas the C++ methods
//   generate exceptions.
// * Anything returned as result by a C++ method is returned by a
//   pointer argument here.
// See the documentation in RamCloudClient.cc for details.

Status rc_create(struct rc_client* client, uint32_t tableId,
        const void* buf, uint32_t length, uint64_t* id, uint64_t* version)
{
    try {
        *id = client->client->create(tableId, buf, length, version);
    } catch (ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

Status
rc_createTable(struct rc_client* client, const char* name)
{
    try {
        client->client->createTable(name);
    } catch (ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

Status
rc_dropTable(struct rc_client* client, const char* name)
{
    try {
        client->client->dropTable(name);
    } catch (ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

/**
 * Return the performance counter provided by the server in its response
 * to the most recent RPC. If no performance counter has been enabled
 * (via rc_selectPerfCounter) then 0 will be returned.
 *
 * \return
 *      See above.
 */
uint32_t
rc_getCounterValue(struct rc_client* client) {
    return client->client->counterValue;
}

/**
 * Return the completion status from the most recent RPC.
 *
 * \return
 *      See above.
 */
Status
rc_getStatus(struct rc_client* client) {
    return client->client->status;
}

Status
rc_openTable(struct rc_client* client, const char* name,
        uint32_t* tableId)
{
    try {
        *tableId = client->client->openTable(name);
    } catch (ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

Status
rc_ping(struct rc_client* client)
{
    try {
        client->client->ping();
    } catch (ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

/**
 * Similar to RamCloudClient::read, except copies the return value out to a
 * fixed-length buffer rather than returning a Buffer object.
 *
 * \param client
 *      Handle for the RAMCloud connection.
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to openTable).
 * \param id
 *      Identifier within tableId of the object to be read.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the read
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned
 *      here.
 * \param[out] buf
 *      The contents of the desired object are copied to this location.
 * \param[out] maxLength
 *      Number of bytes of space available at buf: if the object is
 *      larger than this that only this many bytes will be copied to
 *      buf.
 * \param[out] actualLength
 *      The total size of the object is stored here; this may be
 *      larger than maxLength.
 *      
 * \return
 *      0 means success, anything else indicates an error.
 */
Status
rc_read(struct rc_client* client, uint32_t tableId, uint64_t id,
        const struct RejectRules* rejectRules, uint64_t* version,
        void* buf, uint32_t maxLength, uint32_t* actualLength)
{
    Buffer result;
    try {
        client->client->read(tableId, id, &result, rejectRules, version);
        *actualLength = result.getTotalLength();
        uint32_t bytesToCopy = *actualLength;
        if (bytesToCopy > maxLength) {
            bytesToCopy = maxLength;
        }
        result.copy(0, bytesToCopy, buf);
    } catch (ClientException& e) {
        *actualLength = 0;
        return e.status;
    }
    return STATUS_OK;
}

Status
rc_remove(struct rc_client* client, uint32_t tableId, uint64_t id,
        const struct RejectRules* rejectRules, uint64_t* version)
{
    try {
        client->client->remove(tableId, id, rejectRules, version);
    } catch (ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}

/**
 * Arrange for a performance metric to be collected by the server
 * during each future RPC. The value of the metric can be read after
 * each RPC using rc_getCounterValue.
 *
 * \param client
 *      Handle for the RAMCloud connection (return value from a
 *      previous call to rc_connect).
 * \param type
 *      Specifies what to measure (elapsed time, cache misses, etc.)
 * \param begin
 *      Indicates a point during the RPC when measurement should start.
 * \param end
 *      Indicates a point during the RPC when measurement should stop.
 */
void
rc_selectPerfCounter(struct rc_client* client,
        enum PerfCounterType type,
        enum Mark begin, enum Mark end)
{
    client->client->selectPerfCounter(type, begin, end);
}

Status
rc_write(struct rc_client* client, uint32_t tableId, uint64_t id,
        const void* buf, uint32_t length,
        const struct RejectRules* rejectRules,
        uint64_t* version)
{
    try {
        client->client->write(tableId, id, buf, length, rejectRules,
                version);
    } catch (ClientException& e) {
        return e.status;
    }
    return STATUS_OK;
}
