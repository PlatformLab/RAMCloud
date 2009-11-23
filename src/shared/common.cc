#include <cstdio>
#define __STDC_FORMAT_MACROS	// Required for inttypes.h in C++. Ugh.
#include <inttypes.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>

#include <shared/common.h>

// Output a binary buffer in 'hexdump -C' style.
// Note that this exceeds 80 characters due to 64-bit offsets. Oh, well.
void
debug_dump64(const void *buf, uint64_t bytes)
{
    const unsigned char *cbuf = reinterpret_cast<const unsigned char *>(buf);
    uint64_t i, j;

    for (i = 0; i < bytes; i += 16) {
        char offset[17];
        char hex[16][3];
        char ascii[17];

        snprintf(offset, sizeof(offset), "%016" PRIx64, i);
        offset[sizeof(offset) - 1] = '\0';

        for (j = 0; j < 16; j++) {
            if ((i + j) >= bytes) {
                strcpy(hex[j], "  ");
                ascii[j] = '\0';
            } else {
                snprintf(hex[j], sizeof(hex[0]), "%02x",
                    cbuf[i + j]);
                hex[j][sizeof(hex[0]) - 1] = '\0';
                if (isprint((int)cbuf[i + j]))
                    ascii[j] = cbuf[i + j];
                else
                    ascii[j] = '.';
            }
        }
        ascii[sizeof(ascii) - 1] = '\0';

        printf("%s  %s %s %s %s %s %s %s %s  %s %s %s %s %s %s %s %s  "
            "|%s|\n", offset, hex[0], hex[1], hex[2], hex[3], hex[4],
            hex[5], hex[6], hex[7], hex[8], hex[9], hex[10], hex[11],
            hex[12], hex[13], hex[14], hex[15], ascii);
    }
}
