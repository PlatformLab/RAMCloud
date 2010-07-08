/* Copyright (c) 2009 Stanford University
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

// RAMCloud pragma [CPPLINT=0]

#ifndef RAMCLOUD_SERVER_H
#define RAMCLOUD_SERVER_H

#include <Common.h>

#include <Object.h>
#include <Log.h>
#include <BackupClient.h>

#include <HashTable.h>
#include <Transport.h>

namespace RAMCloud {

class Table {
  public:

    /**
     * The maximum length of a table name, including the null terminator.
     */
    static const int TABLE_NAME_MAX_LEN = 64;

    explicit Table() : next_key(0), next_version(1), object_map(HASH_NLINES) {
    }

    const char *GetName() { return &name[0]; }

    /**
     * \param new_name
     *      A string with a length within #TABLE_NAME_MAX_LEN, including the null
     *      terminator.
     */
    void SetName(const char *new_name) {
        strncpy(&name[0], new_name, TABLE_NAME_MAX_LEN);
        name[TABLE_NAME_MAX_LEN - 1] = '\0';
    }

    /**
     * Increment and return the next table-assigned object ID.
     * \return
     *      The next available object ID in the table.
     * \warning
     *      A client could have already placed an object here by fabricating the
     *      object ID.
     */
    uint64_t AllocateKey() {
        while (Get(next_key))
            ++next_key;
        return next_key;
    }

    /**
     * Increment and return the master vector clock.
     * \return
     *      The next version available from the master vector clock.
     * \see #next_version
     */
    uint64_t AllocateVersion() {
        return next_version++;
    }

    /**
     * Ensure the master master vector clock is at least a certain version.
     * \param minimum
     *      The minimum version the master vector clock can be set to after this
     *      operation.
     * \see #next_version
     */
    void RaiseVersion(uint64_t minimum) {
        if (minimum > next_version)
            next_version = minimum;
    }

    /**
     * \return
     *      The #RAMCloud::Object at \a key, or \c NULL if no such object
     *      exists.
     */
    const Object *Get(uint64_t key) {
        return object_map.lookup(key);
    }

    /**
     * \param key
     *      The object ID at which to store \a o.
     * \param o
     *      The #RAMCloud::Object to store at \a key. May not be \c NULL.
     */
    void Put(uint64_t key, const Object *o) {
        assert(o != NULL);
        object_map.replace(key, o);
    }

    void Delete(uint64_t key) {
        object_map.remove(key);
    }

  private:

    /**
     * The name of the table.
     * \see #SetName().
     */
    char name[64];

    /**
     * The next available object ID in the table.
     * \see #AllocateKey().
     */
    uint64_t next_key;

    /**
     * The master vector clock for the table.
     *
     * \li We guarantee that every distinct blob ever at a particular object ID
     * will have a distinct version number, even across generations, so that
     * they can be uniquely identified across all time with a version number.
     *
     * \li We guarantee that version numbers for a particular object ID
     * monotonically increase over time, so that comparing two version numbers
     * tells which one is more recent.
     *
     * \li We guarantee that the version number of an object increases by
     * exactly one when it is updated, so that clients can accurately predict
     * the version numbers that they will write before the write completes.
     *
     * These guarantees are implemented as follows:
     *
     * \li #next_version, the master vector clock, contains the next available
     * version number for the table on the master. It is initialized to a small
     * integer when the table is created and is recoverable after crashes.
     *
     * \li When an object is created, its new version number is set to the value
     * of the master vector clock, and the master vector clock is incremented.
     * See #AllocateVersion.
     *
     * \li When an object is updated, its new version number is set the old
     * blob's version number plus one.
     *
     * \li When an object is deleted, set the master vector clock to the higher
     * of the master vector clock and the deleted blob's version number plus
     * one. See #RaiseVersion.
     */
    uint64_t next_version;

    /**
     * The object ID to #RAMCloud::Object pointer map for the table.
     */
    HashTable object_map;

    DISALLOW_COPY_AND_ASSIGN(Table);
};

struct ServerConfig {
    // Restore from backups before resuming operation
    bool restore;
  ServerConfig() : restore(false) {}
};

class Server {
  public:
    explicit Server(const ServerConfig *sconfig,
                    Transport* transIn,
                    BackupClient *backupClient=0);
    ~Server();
    void Run();

    void Ping(Transport::ServerRPC *rpc);
    void Read(Transport::ServerRPC *rpc);
    void Write(Transport::ServerRPC *rpc);
    void InsertKey(Transport::ServerRPC *rpc);
    void DeleteKey(Transport::ServerRPC *rpc);
    void CreateTable(Transport::ServerRPC *rpc);
    void OpenTable(Transport::ServerRPC *rpc);
    void DropTable(Transport::ServerRPC *rpc);


  private:
    static bool RejectOperation(const rcrpc_reject_rules *reject_rules,
                                uint64_t version);
    void Restore();
    void HandleRPC();
    bool StoreData(uint64_t table,
                   uint64_t key,
                   const rcrpc_reject_rules *reject_rules,
                   Buffer *data, uint32_t dataOffset, uint32_t dataLength,
                   uint64_t *new_version);

    const ServerConfig *config;
    Transport* trans;
    BackupClient *backup;
    Log *log;
    Table tables[RC_NUM_TABLES];
    friend void ObjectEvictionCallback(log_entry_type_t type,
                                    const void *p,
                                    uint64_t len,
                                    void *cookie);
    friend void TombstoneEvictionCallback(log_entry_type_t type,
                                    const void *p,
                                    uint64_t len,
                                    void *cookie);
    friend void SegmentReplayCallback(Segment *seg, void *cookie);
    friend void ObjectReplayCallback(log_entry_type_t type,
                                     const void *p,
                                     uint64_t len,
                                     void *cookie);
    DISALLOW_COPY_AND_ASSIGN(Server);
};

} // namespace RAMCloud

#endif
