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
#include <stdlib.h>

#include <Client.h>
#include <rcrpc.h>

#include <assert.h>

namespace RC = RAMCloud;

uint64_t multirow;
uint64_t randomReads;
bool pmcInsteadOfTSC;
uint64_t count;
uint64_t size;
double cyclesPerNs;

struct rc_client client;
struct rcrpc_reject_rules read_any;
struct rcrpc_reject_rules write_any;
uint64_t table;

uint64_t
cyclesPerSec()
{
    static uint64_t cyclesPerSec = 0;
    if (cyclesPerSec)
        return cyclesPerSec;
    uint64_t start = rdtsc();
    usleep(500 * 1000);
    uint64_t end = rdtsc();
    cyclesPerSec = (end - start) * 2;
    return cyclesPerSec;
}

void
setup()
{
    memset(&read_any, 0, sizeof(read_any));
    read_any.object_doesnt_exist = true;

    memset(&write_any, 0, sizeof(write_any));

    rc_connect(&client);

    RC::PerfCounterType type;
    type = pmcInsteadOfTSC ? RC::PERF_COUNTER_PMC : RC::PERF_COUNTER_TSC;
    rc_select_perf_counter(&client, type,
                           RC::MARK_RPC_PROCESSING_BEGIN,
                           RC::MARK_RPC_PROCESSING_END);

    assert(!rc_create_table(&client, "test"));
    assert(!rc_open_table(&client, "test", &table));
}

void
cleanup()
{
    assert(!rc_drop_table(&client, "test"));
    rc_disconnect(&client);
}

void
write_one()
{
    int r;

    char buf[size];
    memset(&buf[0], 0xFF, size);
    buf[size - 1] = 0;

    r = rc_write(&client, table, 0, &write_any, NULL, &buf[0], size);
    if (r) {
        fprintf(stderr, "write failed\n");
        cleanup();
        exit(-1);
    }
}

void
write_many()
{
    int r;
    uint64_t serverCycles;
    uint64_t start, end, cycles;

    char buf[size];
    memset(&buf[0], 0xFF, size);
    buf[size - 1] = 0;

    serverCycles = 0;
    start = rdtsc();
    for (uint64_t i = 0; i < count; i++) {
        r = rc_write(&client, table, i, &write_any, NULL, &buf[0], size);
        if (r) {
            fprintf(stderr, "write failed\n");
            cleanup();
            exit(-1);
        }
        serverCycles += rc_read_perf_counter(&client);
    }
    end = rdtsc();

    cycles = end - start;
    printf("write: %.0f total ns\n",
           static_cast<double>(cycles) / cyclesPerNs);
    printf("write: %.0f avg ns\n",
           static_cast<double>(cycles) /
           static_cast<double>(count) /
           cyclesPerNs);
    printf("write-on-server: %.0f total count\n",
           static_cast<double>(serverCycles));
    printf("write-on-server: %.2f per write\n",
           static_cast<double>(serverCycles) /
           static_cast<double>(count));
}

void
read_many()
{
    int r;
    uint64_t start, end, cycles;
    uint64_t serverCycles;
    uint64_t key;

    char buf[size];
    uint64_t buf_len;
    memset(&buf[0], 0xFF, size);
    buf[size - 1] = 0;

    serverCycles = 0;
    start = rdtsc();
    for (uint64_t i = 0; i < count; i++) {
        key = randomReads ? rand() % count : i;
        r = rc_read(&client, table, multirow ? key : 0,
                    &read_any, NULL, &buf[0], &buf_len);
        if (r) {
            fprintf(stderr, "read failed\n");
            cleanup();
            exit(-1);
        }
        serverCycles += rc_read_perf_counter(&client);
    }
    end = rdtsc();

    cycles = end - start;
    printf("read: %.0f total ns\n",
           static_cast<double>(cycles) / cyclesPerNs);
    printf("read: %.0f avg ns\n",
           static_cast<double>(cycles) /
           static_cast<double>(count) /
           cyclesPerNs);
    printf("read-on-server: %.0f total count\n",
           static_cast<double>(serverCycles));
    printf("read-on-server: %.2f per read\n",
           static_cast<double>(serverCycles) /
           static_cast<double>(count));
}

int
main(int argc, char *argv[])
{
    assert(!atexit(cleanup));

    cyclesPerNs = static_cast<double>(cyclesPerSec()) /
        (1000 * 1000 * 1000);

    count = 10000;
    size = 100;
    multirow = 0;
    int opt;
    while ((opt = getopt(argc, argv, "c:s:MRP")) != -1) {
        switch (opt) {
        case 'c':
            count = atol(optarg);
            break;
        case 's':
            size = atol(optarg);
            break;
        case 'M':
            multirow = 1;
            break;
        case 'R':
            multirow = 1;
            randomReads = 1;
            break;
        case 'P':
            pmcInsteadOfTSC = 1;
            break;
        default:
            fprintf(stderr, "Usage: %s [-c count] [-s size] [-m] [-P]\n",
                    argv[0]);
            exit(EXIT_FAILURE);
        }
    }

    printf("Reads: %lu, Size: %lu, Multirow: %lu, RandomReads: %lu\n",
           count, size, multirow, randomReads);

    setup();


    if (multirow) {
        write_many();
    } else {
        write_one();
    }

    read_many();

    return 0;
}
