/* Copyright (c) 2010-2013 Stanford University
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
 * This file defines RAMCloud features available from C rather than C++.
 */

#ifndef RAMCLOUD_CRAMCLOUD_H
#define RAMCLOUD_CRAMCLOUD_H

#include <inttypes.h>

#include "RejectRules.h"
#include "Status.h"

#ifdef __cplusplus
extern "C" {
using namespace RAMCloud;
#else
/// Forward Declarations
struct RamCloud;
struct rc_client;
#endif

typedef enum MultiOp {
  MULTI_OP_READ = 0,
  MULTI_OP_WRITE,
  MULTI_OP_REMOVE
} MultiOp;

Status    rc_connect(const char* serverLocator,
                            const char* clusterName,
                            struct rc_client** newClient);
Status    rc_connectWithClient(
                            struct RamCloud* existingClient,
                            struct rc_client** newClient);
void      rc_disconnect(struct rc_client* client);

Status    rc_createTable(struct rc_client* client,
                         const char* name,
                         uint32_t serverSpan);
Status    rc_dropTable(struct rc_client* client, const char* name);
Status    rc_getStatus(struct rc_client* client);
Status    rc_getTableId(struct rc_client* client, const char* name,
                            uint64_t* tableId);

Status    rc_read(struct rc_client* client, uint64_t tableId,
                            const void* key, uint16_t keyLength,
                            const struct RejectRules* rejectRules,
                            uint64_t* version, void* buf, uint32_t maxLength,
                            uint32_t* actualLength);
Status    rc_remove(struct rc_client* client, uint64_t tableId,
                              const void* key, uint16_t keyLength,
                              const struct RejectRules* rejectRules,
                              uint64_t* version);
Status    rc_write(struct rc_client* client, uint64_t tableId,
                             const void* key, uint16_t keyLength,
                             const void* buf, uint32_t length,
                             const struct RejectRules* rejectRules,
                             uint64_t* version);

void      rc_multiReadCreate(uint64_t tableId,
                                  const void *key, uint16_t keyLength,
                                  void* buf, uint32_t maxLength,
                                  uint32_t* actualLength,
                                  void *where);
void      rc_multiWriteCreate(uint64_t tableId,
                                   const void *key, uint16_t keyLength,
                                   const void* buf, uint32_t length,
                                   const struct RejectRules* rejectRules,
                                   void *where);
void      rc_multiRemoveCreate(uint64_t tableId,
                                    const void *key, uint16_t keyLength,
                                    const struct RejectRules* rejectRules,
                                    void *where);

uint16_t  rc_multiOpSizeOf(MultiOp type);
Status    rc_multiOpStatus(const void *multiOpObject, MultiOp type);
uint64_t  rc_multiOpVersion(const void *multiOpObject, MultiOp type);
void      rc_multiOpDestroy(void *multiOpObject, MultiOp type);

void      rc_multiRead(struct rc_client* client,
                             void **requests, uint32_t numRequests);
void      rc_multiWrite(struct rc_client* client,
                              void **requests, uint32_t numRequests);
void      rc_multiRemove(struct rc_client* client,
                              void **requests, uint32_t numRequests);

Status    rc_testing_kill(struct rc_client* client, uint64_t tableId,
                                    const void* key, uint16_t keyLength);
Status    rc_testing_get_server_id(struct rc_client* client,
                                             uint64_t tableId,
                                             const void* key,
                                             uint16_t keyLength,
                                             uint64_t* serverId);
Status    rc_testing_get_service_locator(struct rc_client* client,
                                                   uint64_t tableId,
                                                   const void* key,
                                                   uint16_t keyLength,
                                                   char* locatorBuffer,
                                                   size_t bufferLength);
Status    rc_testing_fill(struct rc_client* client, uint64_t tableId,
                                    const void* key, uint16_t keyLength,
                                    uint32_t objectCount, uint32_t objectSize);
Status    rc_set_runtime_option(struct rc_client* client,
                                                  const char* option,
                                                  const char* value);
void rc_testing_wait_for_all_tablets_normal(struct rc_client* client,
                                            uint64_t timeoutNs);
void rc_set_log_file(const char* path);

#ifdef __cplusplus
}
#endif

#endif // RAMCLOUD_CRAMCLOUD_H
