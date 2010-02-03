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

#include <Client.h>

#include <stdio.h>
#include <inttypes.h>

#include <assert.h>

#include "rcrpc.h"

static uint64_t
rdtsc()
{
        uint32_t lo, hi;

#ifdef __GNUC__
        __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
#else
        asm("rdtsc" : "=a" (lo), "=d" (hi));
#endif

        return (((uint64_t)hi << 32) | lo);
}

int
main()
{
    struct rc_client client;
    struct rcrpc_reject_rules read_any;
    struct rcrpc_reject_rules write_any;

    memset(&read_any, 0, sizeof(read_any));
    read_any.object_doesnt_exist = true;

    memset(&write_any, 0, sizeof(write_any));

    rc_connect(&client);

    uint64_t b;

    b = rdtsc();
    assert(!rc_create_table(&client, "test"));
    uint64_t table;
    assert(!rc_open_table(&client, "test", &table));
    printf("create+open table took %lu ticks\n", rdtsc() - b);

    b = rdtsc();
    assert(!rc_ping(&client));
    printf("ping took %lu ticks\n", rdtsc() - b);

    b = rdtsc();
    assert(!rc_write(&client, table, 42, &write_any, NULL, "Hello, World!", 14));
    printf("write took %lu ticks\n", rdtsc() - b);

    b = rdtsc();
    const char *value = "0123456789001234567890012345678901234567890123456789012345678901234567890";
    assert(!rc_write(&client, table, 43, &write_any, NULL, value, strlen(value) + 1));
    printf("write took %lu ticks\n", rdtsc() - b);

    char buf[2048];
    b = rdtsc();
    uint64_t buf_len;

    assert(!rc_read(&client, table, 43, &read_any, NULL, &buf[0], &buf_len));
    printf("read took %lu ticks\n", rdtsc() - b);
    printf("Got back [%s] len %lu\n", buf, buf_len);

    assert(!rc_read(&client, table, 42, &read_any, NULL, &buf[0], &buf_len));
    printf("read took %lu ticks\n", rdtsc() - b);
    printf("Got back [%s] len %lu\n", buf, buf_len);

    b = rdtsc();
    uint64_t key = 0xfffffff;
    assert(!rc_insert(&client, table, "Hello, World?", 14, &key));
    printf("insert took %lu ticks\n", rdtsc() - b);
    printf("Got back [%lu] key\n", key);

    b = rdtsc();
    assert(!rc_read(&client, table, key, &read_any, NULL, buf, &buf_len));
    printf("read took %lu ticks\n", rdtsc() - b);
    printf("Got back [%s] len %lu\n", buf, buf_len);

    b = rdtsc();
    int count = 16384;
    key = 0xfffffff;
    const char *val = "0123456789ABCDEF";
    for (int j = 0; j < count; j++)
        assert(!rc_insert(&client, table, val, strlen(val) + 1, &key));
    printf("%d inserts took %lu ticks\n", count, rdtsc() - b);
    printf("avg insert took %lu ticks\n", (rdtsc() - b) / count);

    assert(!rc_drop_table(&client, "test"));

    rc_disconnect(&client);

    return 0;
}
