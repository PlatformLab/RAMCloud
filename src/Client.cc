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

#include <assert.h>
#include "Client.h"
#include "ClientException.h"
#include "TransportManager.h"

namespace RAMCloud {

// Default RejectRules to use if none are provided by the caller.
RejectRules defaultRejectRules;

/**
 * For temporary use until service locator strings are fully adopted.
 * See the other constructor for details.
 */
Client::Client(const char* serverAddr, int serverPort)
        : status(STATUS_OK),  counterValue(0),
          session(transportManager.getSession(serverAddr, serverPort)),
          objectFinder(session),
          perfCounter() { }

/**
 * Construct a Client for a particular service: opens a connection with the
 * service.
 *
 * \param serviceLocator
 *      The service locator for the master (later this will be for the
 *      coordinator).
 *      See \ref ServiceLocatorStrings.
 * \exception CouldntConnectException
 *      Couldn't connect to the server.
 */
Client::Client(const char* serviceLocator)
        : status(STATUS_OK),  counterValue(0),
          session(transportManager.getSession(serviceLocator)),
          objectFinder(session),
          perfCounter() { }


/**
 * Destructor for Client objects: releases all resources for the
 * cluster and aborts RPCs in progress.
 */
Client::~Client()
{
}

/**
 * Cancel any performance counter request previously specified by a call to
 * selectPerfCounter.
 */
void
Client::clearPerfCounter()
{
    static RpcPerfCounter none = {0, 0, 0};
    perfCounter = none;
}

/**
 * Create a new object in a table, with an id assigned by the server.
 *
 * \param tableId
 *      The table in which the new object is to be created (return
 *      value from a previous call to openTable).
 * \param buf
 *      Address of the first byte of the contents for the new object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new object.
 * \param[out] version
 *      If non-NULL, the version number of the new object is returned
 *      here; guaranteed to be greater than that of any previous
 *      object that used the same id in the same table.
 *      
 * \return
 *      The identifier for the new object: unique within the table
 *      and guaranteed not to be in use already. Generally, servers
 *      choose ids sequentially starting at 1 (but they may need
 *      to skip over ids previously created using \c write).
 *
 * \exception InternalError
 */
uint64_t
Client::create(uint32_t tableId, const void* buf, uint32_t length,
        uint64_t* version)
{
    Buffer req, resp;
    CreateRequest* reqHdr;
    const CreateResponse* respHdr;

    reqHdr = new(&req, APPEND) CreateRequest;
    reqHdr->common.type = CREATE;
    reqHdr->common.perfCounter = perfCounter;
    reqHdr->tableId = tableId;
    reqHdr->length = length;
    Buffer::Chunk::appendToBuffer(&req, buf, length);
    Transport::Transport::SessionRef master(objectFinder.lookupHead(tableId));
    master->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const CreateResponse*>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL) {
        throwShortResponseError(&resp);
    }
    counterValue = respHdr->common.counterValue;
    if (version != NULL) {
        *version = respHdr->version;
    }
    status = respHdr->common.status;
    if (status != STATUS_OK) {
        ClientException::throwException(status);
    }
    return respHdr->id;
}

/**
 * Create a new table.
 *
 * \param name
 *      Name for the new table (NULL-terminated string).
 *
 * \exception NoTableSpaceException
 * \exception InternalError
 */
void
Client::createTable(const char* name)
{
    Buffer req, resp;
    uint32_t length = strlen(name) + 1;
    CreateTableRequest* reqHdr;
    const CreateTableResponse* respHdr;

    reqHdr = new(&req, APPEND) CreateTableRequest;
    reqHdr->common.type = CREATE_TABLE;
    reqHdr->common.perfCounter = perfCounter;
    reqHdr->nameLength = length;
    memcpy(new(&req, APPEND) char[length], name, length);
    session->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const CreateTableResponse*>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL) {
        throwShortResponseError(&resp);
    }
    counterValue = respHdr->common.counterValue;
    status = respHdr->common.status;
    if (status != STATUS_OK) {
        ClientException::throwException(status);
    }
}

/**
 * Delete a table.
 *
 * All objects in the table are implicitly deleted, along with any
 * other information associated with the table (such as, someday,
 * indexes).  If the table does not currently exist than the operation
 * returns successfully without actually doing anything.
 *
 * \param name
 *      Name of the table to delete (NULL-terminated string).
 *  
 * \exception InternalError
 */
void
Client::dropTable(const char* name)
{
    Buffer req, resp;
    uint32_t length = strlen(name) + 1;
    DropTableRequest* reqHdr;
    const DropTableResponse* respHdr;

    reqHdr = new(&req, APPEND) DropTableRequest;
    reqHdr->common.type = DROP_TABLE;
    reqHdr->common.perfCounter = perfCounter;
    reqHdr->nameLength = length;
    memcpy(new(&req, APPEND) char[length], name, length);
    session->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const DropTableResponse*>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL) {
        throwShortResponseError(&resp);
    }
    counterValue = respHdr->common.counterValue;
    status = respHdr->common.status;
    if (status != STATUS_OK) {
        ClientException::throwException(status);
    }
}

/**
 * Look up a table by name and return a small integer handle that
 * can be used to access the table.
 *
 * \param name
 *      Name of the desired table (NULL-terminated string).
 *      
 * \return
 *      The return value is an identifier for the table; this is used
 *      instead of the table's name for most RAMCloud operations
 *      involving the table.
 *
 * \exception TableDoesntExistException
 * \exception InternalError
 */
uint32_t
Client::openTable(const char* name)
{
    Buffer req, resp;
    uint32_t length = strlen(name) + 1;
    OpenTableRequest* reqHdr;
    const OpenTableResponse* respHdr;

    reqHdr = new(&req, APPEND) OpenTableRequest;
    reqHdr->common.type = OPEN_TABLE;
    reqHdr->common.perfCounter = perfCounter;
    reqHdr->nameLength = length;
    memcpy(new(&req, APPEND) char[length], name, length);
    session->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const OpenTableResponse*>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL) {
        throwShortResponseError(&resp);
    }
    counterValue = respHdr->common.counterValue;
    status = respHdr->common.status;
    if (status != STATUS_OK) {
        ClientException::throwException(status);
    }
    return respHdr->tableId;
}

/**
 * Test that a server exists and is responsive.
 *
 * This operation issues a no-op RPC request, which causes
 * communication with the given server but doesn't actually do
 * anything on the server.
 *
 * \exception InternalError
 */
void
Client::ping()
{
    Buffer req, resp;
    PingRequest* reqHdr;
    const PingResponse* respHdr;

    reqHdr = new(&req, APPEND) PingRequest;
    reqHdr->common.type = PING;
    reqHdr->common.perfCounter = perfCounter;
    session->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const PingResponse*>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL) {
        throwShortResponseError(&resp);
    }
    counterValue = respHdr->common.counterValue;
    status = respHdr->common.status;
    if (status != STATUS_OK) {
        ClientException::throwException(status);
    }
}

/**
 * Read the current contents of an object.
 *
 * \param tableId
 *      The table containing the desired object (return value from
 *      a previous call to openTable).
 * \param id
 *      Identifier within tableId of the object to be read.
 * \param[out] value
 *      After a successful return, this Buffer will hold the
 *      contents of the desired object.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the read
 *      should be aborted with an error.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned
 *      here.
 *
 * \exception RejectRulesException
 * \exception InternalError
 */
void
Client::read(uint32_t tableId, uint64_t id, Buffer* value,
        const RejectRules* rejectRules, uint64_t* version)
{
    Buffer req;
    ReadRequest* reqHdr;
    const ReadResponse* respHdr;
    uint32_t length;

    reqHdr = new(&req, APPEND) ReadRequest;
    reqHdr->common.type = READ;
    reqHdr->common.perfCounter = perfCounter;
    reqHdr->id = id;
    reqHdr->tableId = tableId;
    reqHdr->pad1 = 0;                            // Needed only for tesing.
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    Transport::SessionRef master(objectFinder.lookup(tableId, id));
    master->clientSend(&req, value)->getReply();

    respHdr = static_cast<const ReadResponse*>(
             value->getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL) {
        throwShortResponseError(value);
    }
    counterValue = respHdr->common.counterValue;
    status = respHdr->common.status;
    if (version != NULL) {
        *version = respHdr->version;
    }
    length = respHdr->length;

    // Truncate the response Buffer so that it consists of nothing
    // but the object data.
    value->truncateFront(sizeof(*respHdr));
    uint32_t extra = value->getTotalLength() - length;
    if (extra > 0) {
        value->truncateEnd(extra);
    }
    if (status != STATUS_OK) {
        ClientException::throwException(status);
    }
}

/**
 * Delete an object from a table. If the object does not currently exist
 * and no rejectRules match, then the operation succeeds without doing
 * anything.
 *
 * \param tableId
 *      The table containing the object to be deleted (return value from
 *      a previous call to openTable).
 * \param id
 *      Identifier within tableId of the object to be deleted.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the delete
 *      should be aborted with an error.  If NULL, the object is
 *      deleted unconditionally.
 * \param[out] version
 *      If non-NULL, the version number of the object (prior to
 *      deletion) is returned here.  If the object didn't exist
 *      then 0 will be returned.
 *
 * \exception RejectRulesException
 * \exception InternalError
 */
void
Client::remove(uint32_t tableId, uint64_t id,
        const RejectRules* rejectRules, uint64_t* version)
{
    Buffer req, resp;
    RemoveRequest* reqHdr;
    const RemoveResponse* respHdr;

    reqHdr = new(&req, APPEND) RemoveRequest;
    reqHdr->common.type = REMOVE;
    reqHdr->common.perfCounter = perfCounter;
    reqHdr->id = id;
    reqHdr->tableId = tableId;
    reqHdr->pad1 = 0;                            // Needed only for testing.
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    Transport::SessionRef master(objectFinder.lookup(tableId, id));
    master->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const RemoveResponse*>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL) {
        throwShortResponseError(&resp);
    }
    counterValue = respHdr->common.counterValue;
    if (version != NULL) {
        *version = respHdr->version;
    }
    status = respHdr->common.status;
    if (status != STATUS_OK) {
        ClientException::throwException(status);
    }
}

/**
 * Arrange for a performance metric to be collected by the server
 * during each future RPC. The value of the metric can be read from
 * the "counterValue" variable after each RPC.
 *
 * \param type
 *      Specifies what to measure (elapsed time, cache misses, etc.)
 * \param begin
 *      Indicates a point during the RPC when measurement should start.
 * \param end
 *      Indicates a point during the RPC when measurement should stop.
 */

void
Client::selectPerfCounter(PerfCounterType type, Mark begin, Mark end)
{
    perfCounter.beginMark = begin;
    perfCounter.endMark = end;
    perfCounter.counterType = type;
}

/**
 * Write a specific object in a table; overwrite any existing
 * object, or create a new object if none existed.
 *
 * \param tableId
 *      The table containing the desired object (return value from a
 *      previous call to openTable).
 * \param id
 *      Identifier within tableId of the object to be written; may or
 *      may not refer to an existing object.
 * \param buf
 *      Address of the first byte of the new contents for the object;
 *      must contain at least length bytes.
 * \param length
 *      Size in bytes of the new contents for the object.
 * \param rejectRules
 *      If non-NULL, specifies conditions under which the write
 *      should be aborted with an error. NULL means the object should
 *      be written unconditionally.
 * \param[out] version
 *      If non-NULL, the version number of the object is returned
 *      here. If the operation was successful this will be the new
 *      version for the object; if this object has ever existed
 *      previously the new version is guaranteed to be greater than
 *      any previous version of the object. If the operation failed
 *      then the version number returned is the current version of
 *      the object, or 0 if the object does not exist.
 *
 * \exception RejectRulesException
 * \exception InternalError
 */
void
Client::write(uint32_t tableId, uint64_t id, const void* buf, uint32_t length,
        const RejectRules* rejectRules, uint64_t* version)
{
    Buffer req, resp;
    WriteRequest* reqHdr;
    const WriteResponse* respHdr;

    reqHdr = new(&req, APPEND) WriteRequest;
    reqHdr->common.type = WRITE;
    reqHdr->common.perfCounter = perfCounter;
    reqHdr->id = id;
    reqHdr->tableId = tableId;
    reqHdr->length = length;
    reqHdr->rejectRules = rejectRules ? *rejectRules : defaultRejectRules;
    Buffer::Chunk::appendToBuffer(&req, buf, length);
    Transport::SessionRef master(objectFinder.lookup(tableId, id));
    master->clientSend(&req, &resp)->getReply();

    respHdr = static_cast<const WriteResponse *>(
             resp.getRange(0, sizeof(*respHdr)));
    if (respHdr == NULL) {
        throwShortResponseError(&resp);
    }
    counterValue = respHdr->common.counterValue;
    if (version != NULL) {
        *version = respHdr->version;
    }
    status = respHdr->common.status;
    if (status != STATUS_OK) {
        ClientException::throwException(status);
    }
}

/**
 * Generate an appropriate exception when an RPC response arrives that
 * is too short to hold the full response header expected for this RPC.
 * If this method is invoked it means the server found a problem before
 * dispatching to a type-specific handler (e.g. the server didn't understand
 * the RPC's type, or basic authentication failed).
 *
 * \param response
 *      Contains the full response message from the server.
 *
 * \exception ClientException
 *      This method always throws an exception; it never returns.
 *      The exact type of the exception will depend on the status
 *      value present in the packet (if any).
 */
void
Client::throwShortResponseError(Buffer* response)
{
    const RpcResponseCommon* common =
            static_cast<const RpcResponseCommon*>(
            response->getRange(0, sizeof(RpcResponseCommon)));
    if (common != NULL) {
        counterValue = common->counterValue;
        if (common->status == STATUS_OK) {
            // This makes no sense: the server claims to have handled
            // the RPC correctly, but it didn't return the right size
            // response for this RPC; report an error.
            status = STATUS_RESPONSE_FORMAT_ERROR;
        } else {
            status = common->status;
        }
    } else {
        // The packet wasn't even long enough to hold a standard
        // header.
        status = STATUS_RESPONSE_FORMAT_ERROR;
    }
    ClientException::throwException(status);
}

}  // namespace RAMCloud
