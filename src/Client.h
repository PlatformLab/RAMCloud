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
 * Header file for Client.cc.
 */

#ifndef RAMCLOUD_CLIENT_H
#define RAMCLOUD_CLIENT_H

#include "Buffer.h"
#include "ClientException.h"
#include "Common.h"
#include "Mark.h"
#include "PerfCounterType.h"
#include "Rpc.h"
#include "Service.h"
#include "Status.h"
#include "Transport.h"

namespace RAMCloud {

/**
 * The Client class provides the primary interface used by applications to
 * access a RAMCloud cluster.
 *
 * Each Client object provides access to a particular RAMCloud cluster;
 * all of the RAMCloud RPC requests appear as methods on this object.
 */
class Client {
  public:
    Client(const char* serverAddr, int serverPort);
    Client(Service* service, Transport* transport);
    virtual ~Client();
    void clearPerfCounter();
    uint64_t create(uint32_t tableId, const void* buf, uint32_t length,
                    uint64_t* version = NULL);
    void createTable(const char* name);
    void dropTable(const char* name);
    uint32_t openTable(const char* name);
    void ping();
    void read(uint32_t tableId, uint64_t id, Buffer* value,
              const RejectRules* rejectRules = NULL,
              uint64_t* version = NULL);
    void remove(uint32_t tableId, uint64_t id,
                const RejectRules* rejectRules = NULL,
                uint64_t* version = NULL);
    void selectPerfCounter(PerfCounterType type, Mark begin, Mark end);
    void write(uint32_t tableId, uint64_t id, const void* buf,
               uint32_t length, const RejectRules* rejectRules = NULL,
               uint64_t* version = NULL);

    /**
     * Completion status from the most recent RPC completed for this client.
     */
    Status status;

    /**
     * Performance metric from the response in the most recent RPC (as
     * requested by selectPerfCounter). If no metric was requested and done
     * most recent RPC, then this value is 0.
     */
    uint32_t counterValue;

  protected:
    Service* service;              //!< For now we only know how to talk
                                   //!< to a single RAMCloud server; this
                                   //!< is a handle for that server.
    Transport* transport;          //!< Provides communication with a service.
    bool weOwnTransportAndService; //!< True means that transport and service
                                   //!< were created by us (the Client class)
                                   //!< and should be destroyed by us; false
                                   //!< means they were provided externally.
    RpcPerfCounter perfCounter;    //!< Every RPC request will ask the server
                                   //!< to measure this during the execution
                                   //!< of the RPC.

    void throwShortResponseError(Buffer* response) __attribute__((noreturn));

    DISALLOW_COPY_AND_ASSIGN(Client);
};
} // namespace RAMCloud

#endif // RAMCLOUD_CLIENT_H
