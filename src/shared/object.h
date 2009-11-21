#ifndef RAMCLOUD_SHARED_OBJECT_H
#define RAMCLOUD_SHARED_OBJECT_H

#include <shared/common.h>

#include <inttypes.h>

namespace RAMCloud {

enum storage_type {
    STORAGE_INVALID_TYPE = 0,
    STORAGE_CHUNK_HDR_TYPE = 0x0B1EC7B1750B1EC7, // "OBJECT BITS OBJECT"
    ___STORAGE_TYPE_MAX___ = 0xFFFFFFFFFFFFFFFF
};

struct chunk_entry {
    uint64_t len;
    uint64_t index_id;
    char data[0];                       // Variable length, but contiguous
};

struct chunk_hdr {
    uint64_t checksum;
    enum storage_type type;
    uint64_t key;
    // TODO(stutsman) - only leaving enough room here for the data entry
    // with no indexes - this is enough to let me hack the 0.1 impl into
    // something that is compatible with the backup format, once we have
    // real memory allocation this will change to a 0-ary array again
    struct chunk_entry entries[1];
};

} // namespace RAMCloud

#endif
