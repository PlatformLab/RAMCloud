#ifndef RAMCLOUD_SHARED_COMMON_H
#define RAMCLOUD_SHARED_COMMON_H

#include <inttypes.h>

// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);             \
    void operator=(const TypeName&)

void debug_dump64(const void *buf, uint64_t bytes);

#endif
