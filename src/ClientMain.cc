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

//#include <Common.h>
#include <Client.h>
#include <stdio.h>
#include <string.h>

#include <assert.h>

#include <rcrpc.h>

int
main()
try
{
    struct rc_client client;
    struct rcrpc_reject_rules read_any;
    struct rcrpc_reject_rules write_any;
    int r;

    memset(&read_any, 0, sizeof(read_any));
    read_any.object_doesnt_exist = true;

    memset(&write_any, 0, sizeof(write_any));

    rc_connect(&client);
    rc_select_perf_counter(&client,
                           RAMCloud::PERF_COUNTER_TSC,
                           RAMCloud::MARK_RPC_PROCESSING_BEGIN,
                           RAMCloud::MARK_RPC_PROCESSING_END);

    uint64_t b;

    b = rdtsc();
    r = rc_create_table(&client, "test");
    assert(!r);
    uint64_t table;
    r = rc_open_table(&client, "test", &table);
    printf("create+open table took %lu ticks\n", rdtsc() - b);
    assert(!r);
    printf("create+open took %u ticks on the server\n",
           rc_read_perf_counter(&client));

    b = rdtsc();
    r = rc_ping(&client);
    printf("ping took %lu ticks on the client\n", rdtsc() - b);
    assert(!r);
    printf("ping took %u ticks on the server\n",
           rc_read_perf_counter(&client));

    b = rdtsc();
    r = rc_write(&client, table, 42, &write_any, NULL, "Hello, World!", 14);
    printf("write took %lu ticks\n", rdtsc() - b);
    assert(!r);
    printf("write took %u ticks on the server\n",
           rc_read_perf_counter(&client));

    b = rdtsc();
    const char *value = "012345678900123456789001234567890"
        "1234567890123456789012345678901234567890";
    r = rc_write(&client, table, 43, &write_any,
                 NULL, value, strlen(value) + 1);
    printf("write took %lu ticks\n", rdtsc() - b);
    assert(!r);
    printf("write took %u ticks on the server\n",
           rc_read_perf_counter(&client));

    char buf[2048];
    b = rdtsc();
    uint64_t buf_len;

    r = rc_read(&client, table, 43, &read_any, NULL, &buf[0], &buf_len);
    printf("read took %lu ticks\n", rdtsc() - b);
    assert(!r);
    printf("read took %u ticks on the server\n",
           rc_read_perf_counter(&client));

    printf("Got back [%s] len %lu\n", buf, buf_len);

    r = rc_read(&client, table, 42, &read_any, NULL, &buf[0], &buf_len);
    printf("read took %lu ticks\n", rdtsc() - b);
    assert(!r);
    printf("read took %u ticks on the server\n",
           rc_read_perf_counter(&client));
    printf("Got back [%s] len %lu\n", buf, buf_len);

    b = rdtsc();
    uint64_t key = 0xfffffff;
    r = rc_insert(&client, table, "Hello, World?", 14, &key);
    printf("insert took %lu ticks\n", rdtsc() - b);
    assert(!r);
    printf("insert took %u ticks on the server\n",
           rc_read_perf_counter(&client));
    printf("Got back [%lu] key\n", key);

    b = rdtsc();
    r = rc_read(&client, table, key, &read_any, NULL, buf, &buf_len);
    printf("read took %lu ticks\n", rdtsc() - b);
    assert(!r);
    printf("read took %u ticks on the server\n",
           rc_read_perf_counter(&client));
    printf("Got back [%s] len %lu\n", buf, buf_len);

    b = rdtsc();
    int count = 16384;
    key = 0xfffffff;
    const char *val = "0123456789ABCDEF";
    uint64_t sum = 0;
    for (int j = 0; j < count; j++) {
        r = rc_insert(&client, table, val, strlen(val) + 1, &key);
        assert(!r);
        sum += rc_read_perf_counter(&client);
    }
    printf("%d inserts took %lu ticks\n", count, rdtsc() - b);
    printf("avg insert took %lu ticks\n", (rdtsc() - b) / count);
    printf("%d inserts took %lu ticks on the server\n", count, sum);
    printf("%d avg insert took %lu ticks on the server\n", count,
           sum / count);

    r = rc_drop_table(&client, "test");
    assert(!r);

    rc_disconnect(&client);

    return 0;
} catch (RAMCloud::Exception e) {
    fprintf(stderr, "client: %s\n", e.message.c_str());
}
