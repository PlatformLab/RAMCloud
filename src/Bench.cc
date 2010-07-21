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
#include <getopt.h>
#include <errno.h>

namespace RC = RAMCloud;

uint64_t multirow;
uint64_t randomReads;
bool pmcInsteadOfTSC;
uint64_t count;
uint64_t size;
char address[50];
int port;
int cpu = -1;
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
cleanup()
{
    assert(!rc_drop_table(&client, "test"));
    rc_disconnect(&client);
}

void
setup()
{
    if (cpu != -1) {
        cpu_set_t cpus;
        CPU_ZERO(&cpus);
        CPU_SET(cpu, &cpus);

        int r = sched_setaffinity(0, sizeof(cpus), &cpus);
        if (r < 0) {
            fprintf(stderr, "Bench: Couldn't pin to core %d: %s\n",
                    cpu, strerror(errno));
            exit(EXIT_FAILURE);
        }
        printf("Bench: Pinned to core %d\n", cpu);
    }

    memset(&read_any, 0, sizeof(read_any));
    read_any.object_doesnt_exist = true;

    memset(&write_any, 0, sizeof(write_any));

    rc_connect(&client, address, port);

    assert(!atexit(cleanup));

    RC::PerfCounterType type;
    type = pmcInsteadOfTSC ? RC::PERF_COUNTER_PMC : RC::PERF_COUNTER_TSC;
    rc_select_perf_counter(&client, type,
                           RC::MARK_RPC_PROCESSING_BEGIN,
                           RC::MARK_RPC_PROCESSING_END);

    if (rc_create_table(&client, "test"))
        fprintf(stderr, "Warning: table \"test\" already exists.\n");
    assert(!rc_open_table(&client, "test", &table));
}

void
bench(const char *name, uint64_t (f)(void))
{
    uint64_t start, end, cycles;

    start = rdtsc();
    uint64_t serverCounter = f();
    end = rdtsc();

    cycles = end - start;
    printf("%s ns     %12.0f\n", name,
           static_cast<double>(cycles) / cyclesPerNs);
    printf("%s avgns  %12.2f\n", name,
           static_cast<double>(cycles) /
           static_cast<double>(count) /
           cyclesPerNs);
    printf("%s ctr    %12.0f\n", name,
           static_cast<double>(serverCounter));
    printf("%s avgctr %12.2f\n", name,
           static_cast<double>(serverCounter) /
           static_cast<double>(count));
}

#define BENCH(fname) bench(#fname, fname)

uint64_t
writeOne()
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

    return rc_read_perf_counter(&client);
}

uint64_t
writeMany(void)
{
    int r;
    uint64_t serverCounter;

    char buf[size];
    memset(&buf[0], 0xFF, size);
    buf[size - 1] = 0;

    serverCounter = 0;
    for (uint64_t i = 0; i < count; i++) {
        r = rc_write(&client, table, i, &write_any, NULL, &buf[0], size);
        if (r) {
            fprintf(stderr, "write failed\n");
            cleanup();
            exit(-1);
        }
        serverCounter += rc_read_perf_counter(&client);
    }

    return serverCounter;
}

uint64_t
readMany()
{
    int r;
    uint64_t serverCounter;
    uint64_t key;

    char buf[size];
    uint64_t bufLen;
    memset(&buf[0], 0xFF, size);
    buf[size - 1] = 0;

    serverCounter = 0;
    for (uint64_t i = 0; i < count; i++) {
        key = randomReads ? rand() % count : i;
        r = rc_read(&client, table, multirow ? key : 0,
                    &read_any, NULL, &buf[0], &bufLen);
        if (r) {
            fprintf(stderr, "read failed\n");
            cleanup();
            exit(-1);
        }
        serverCounter += rc_read_perf_counter(&client);
    }

    return serverCounter;
}

void __attribute__ ((noreturn))
usage(char *arg0)
{
    printf("Usage: %s [-n number] [-s size] [-M] [-R] [-P] "
            "[-p port] [-a address] [-c cpu]\n"
           "\t-n\t--number\tNumber of iterations to write/read.\n"
           "\t-s\t--size\t\tSize of objects to write/read.\n"
           "\t-M\t--multirow\tWrite objects equal to number parameter..\n"
           "\t-R\t--random\tRestore from backup before serving.\n"
           "\t-P\t--performance\tReturn CPU performance counter from server.\n"
           "\t-p\t--port\t\tChoose which port to connect to.\n"
           "\t-a\t--address\tChoose which address to connect to.\n"
           "\t-c\t--cpu\t\tRestrict the test to a specific CPU (0 indexed).\n",
           arg0);
    exit(EXIT_FAILURE);
}

void
cmdline(int argc, char *argv[])
{
    count = 10000;
    size = 100;
    multirow = 0;
    port = SVRPORT;
    strncpy(address, SVRADDR, sizeof(address));
    address[sizeof(address) - 1] = '\0';

    struct option long_options[] = {
        {"number", required_argument, NULL, 'n'},
        {"size", required_argument, NULL, 's'},
        {"multirow", no_argument, NULL, 'M'},
        {"random", no_argument, NULL, 'R'},
        {"performance", no_argument, NULL, 'P'},
        {"address", required_argument, NULL, 'a'},
        {"port", required_argument, NULL, 'p'},
        {"cpu", required_argument, NULL, 'a'},
        {0, 0, 0, 0},
    };

    int c;
    int i = 0;
    while ((c = getopt_long(argc, argv, "n:s:MRPa:p:c:",
                            long_options, &i)) >= 0)
    {
        switch (c) {
        case 'n':
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
        case 'a':
            strncpy(address, optarg, sizeof(address));
            address[sizeof(address) - 1] = '\0';
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 'c':
            cpu = atoi(optarg);
            break;
        default:
            usage(argv[0]);
        }
    }
}

int
main(int argc, char *argv[])
try
{
    cmdline(argc, argv);

    cyclesPerNs = static_cast<double>(cyclesPerSec()) /
        (1000 * 1000 * 1000);

    printf("Reads: %lu, Size: %lu, Multirow: %lu, RandomReads: %lu\n",
           count, size, multirow, randomReads);

    setup();

    if (multirow) {
        BENCH(writeMany);
    } else {
        BENCH(writeOne);
    }

    BENCH(readMany);

    return 0;
} catch (RAMCloud::Exception e) {
    fprintf(stderr, "Bench: %s\n", e.message.c_str());
}
