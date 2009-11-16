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

    b = rdtsc();
    client->ping();
    printf("ping took %lu ticks\n", rdtsc() - b);

    b = rdtsc();
    client->write100(42, "Hello, World!", 14);
    printf("write100 took %lu ticks\n", rdtsc() - b);

    char buf[100];
    b = rdtsc();
    client->read100(42, buf, 100);
    printf("read100 took %lu ticks\n", rdtsc() - b);
    printf("Got back [%s]\n", buf);

    delete client;
    return (0);
}
