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

#include <client/client.h>

#include <stdio.h>
#include <inttypes.h>

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
    RAMCloud::Client *client = new RAMCloud::DefaultClient();
    uint64_t b;
    uint64_t table;

    b = rdtsc();
    client->CreateTable("test");
    table = client->OpenTable("test");
    printf("create+open table took %lu ticks\n", rdtsc() - b);

    b = rdtsc();
    client->Ping();
    printf("ping took %lu ticks\n", rdtsc() - b);

    b = rdtsc();
    client->Write(table, 42, "Hello, World!", 14);
    printf("write took %lu ticks\n", rdtsc() - b);

    char buf[100];
    b = rdtsc();
    uint64_t buf_len;
    client->Read(table, 42, buf, &buf_len);
    printf("read took %lu ticks\n", rdtsc() - b);
    printf("Got back [%s] len %lu\n", buf, buf_len);

    b = rdtsc();
    uint64_t key = 0xfffffff;
    client->Insert(table, "Hello, World?", 14, &key);
    printf("insert took %lu ticks\n", rdtsc() - b);
    printf("Got back [%lu] key\n", key);

    b = rdtsc();
    client->Read(table, key, buf, &buf_len);
    printf("read took %lu ticks\n", rdtsc() - b);
    printf("Got back [%s] len %lu\n", buf, buf_len);

    b = rdtsc();
    int count = 256;
    key = 0xfffffff;
    for (int j = 0; j < count; j++) {
        client->Insert(table, "Hello, World?", 14, &key);
    }
    printf("%d inserts took %lu ticks\n", count, rdtsc() - b);
    printf("avg insert took %lu ticks\n", (rdtsc() - b) / count);

    client->DropTable("test");

    delete client;
    return (0);
}
