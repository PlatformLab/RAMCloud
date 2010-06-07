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

#include <stdio.h>
#include <string.h>

#include <Client.h>
#include <rcrpc.h>

#include <assert.h>

uint64_t multirow;
uint64_t count;
uint64_t size;

struct rc_client client;
struct rcrpc_reject_rules read_any;
struct rcrpc_reject_rules write_any;
uint64_t table;

uint64_t
cycles_per_sec()
{
    static uint64_t cycles_per_sec = 0;
    if (cycles_per_sec)
        return cycles_per_sec;
    uint64_t start = rdtsc();
    usleep(500 * 1000);
    uint64_t end = rdtsc();
    cycles_per_sec = (end - start) * 2;
    return cycles_per_sec;
}

void
setup()
{
    memset(&read_any, 0, sizeof(read_any));
    read_any.object_doesnt_exist = true;

    memset(&write_any, 0, sizeof(write_any));

    rc_connect(&client);

    assert(!rc_create_table(&client, "test"));
    assert(!rc_open_table(&client, "test", &table));
}

void
cleanup()
{
    assert(!rc_drop_table(&client, "test"));
    rc_disconnect(&client);
}

int
main(int argc, char *argv[])
{
    double cycles_per_ns = static_cast<double>(cycles_per_sec()) /
        (1000 * 1000 * 1000);

    count = 1;
    size = 100;
    multirow = 0;
    int opt;
    while ((opt = getopt(argc, argv, "c:s:m")) != -1) {
        switch (opt) {
        case 'c':
            count = atol(optarg);
            break;
        case 's':
            size = atol(optarg);
            break;
        case 'm':
            multirow = 1;
            break;
        default:
            fprintf(stderr, "Usage: %s [-c count] [-s size] [-m]\n",
                    argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    printf("Reads: %lu, Size: %lu, Multirow: %lu\n", count, size, multirow);

    setup();

    char buf[size];
    uint64_t buf_len;
    memset(&buf[0], 0xFF, size);
    buf[size - 1] = 0;

    int r;
    uint64_t start, end, cycles;

    if (multirow) {
        start = rdtsc();
        for (uint64_t i = 0; i < count; i++) {
            r = rc_write(&client, table, i, &write_any, NULL, &buf[0], size);
            if (r) {
                fprintf(stderr, "write failed\n");
                cleanup();
                return -1;
            }
        }
        end = rdtsc();

        cycles = end - start;
        printf("write: %.0f total ns\n",
               static_cast<double>(cycles) / cycles_per_ns);
        printf("write: %.0f avg ns\n",
               static_cast<double>(cycles) /
               static_cast<double>(count) /
               cycles_per_ns);
    } else {
        r = rc_write(&client, table, 0, &write_any, NULL, &buf[0], size);
        if (r) {
            fprintf(stderr, "write failed\n");
            cleanup();
            return -1;
        }
    }

    start = rdtsc();
    for (uint64_t i = 0; i < count; i++) {
        r = rc_read(&client, table, multirow ? i : 0,
                    &read_any, NULL, &buf[0], &buf_len);
        if (r) {
            fprintf(stderr, "read failed\n");
            cleanup();
            return -1;
        }
    }
    end = rdtsc();

    cycles = end - start;
    printf("read: %.0f total ns\n",
           static_cast<double>(cycles) / cycles_per_ns);
    printf("read: %.0f avg ns\n",
           static_cast<double>(cycles) /
           static_cast<double>(count) /
           cycles_per_ns);

    cleanup();

    return 0;
}
