#include <cstdio>
#include <inttypes.h>

#include <shared/common.h>

void
debug_dump64(const void *buf, uint64_t bytes)
{
    const unsigned char *cbuf = reinterpret_cast<const unsigned char *>(buf);
    for (int i = 0;; i++) {
        if (!(i % 16) && i)
            printf("\n");
        for (int64_t j = 7; j >= 0; j--) {
            if (bytes == 0)
                goto done;
            printf("%02x", cbuf[i * 8 + j]);
            bytes--;
        }
        printf(" ");
    }
done:
    printf("\n");
}
