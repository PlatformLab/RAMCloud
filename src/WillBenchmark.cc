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

#include "Cycles.h"
#include "Log.h"
#include "Logging.h"
#include "Memory.h"
#include "Table.h"
#include "Tablets.pb.h"
#include "TabletProfiler.h"
#include "Will.h"

namespace RAMCloud {

class WillBenchmark {

    ProtoBuf::Tablets::Tablet
    createTablet(uint64_t serverId, uint64_t tableId, uint64_t firstKey,
        uint64_t lastKey)
    {
        ProtoBuf::Tablets_Tablet tablet;
        tablet.set_table_id(tableId);
        tablet.set_start_object_id(firstKey);
        tablet.set_end_object_id(lastKey);
        tablet.set_state(ProtoBuf::Tablets_Tablet_State_NORMAL);
        tablet.set_server_id(serverId);
        return tablet;
    }

    Table*
    createAndAddTablet(ProtoBuf::Tablets &tablets, uint64_t serverId,
                 uint64_t tableId, uint64_t firstKey, uint64_t lastKey)
    {
        ProtoBuf::Tablets_Tablet tablet = createTablet(serverId, tableId,
            firstKey, lastKey);
        Table* table = new Table(tablet.table_id());
        tablet.set_user_data(reinterpret_cast<uint64_t>(table));
        *tablets.add_tablet() = tablet;
        return table;
    }

    void
    destroyTables(ProtoBuf::Tablets &tablets)
    {
        std::set<Table*> tables;
        foreach (const ProtoBuf::Tablets::Tablet& tablet, tablets.tablet())
            tables.insert(reinterpret_cast<Table*>(tablet.user_data()));
        foreach (Table* table, tables)
            delete table;
    }


  public:
    WillBenchmark()
    {
    }

    void
    run(uint64_t serverBytes, uint64_t serverTablets, int objectBytes)
    {
        uint64_t bytesPerTablet = serverBytes / serverTablets;
        ProtoBuf::Tablets tablets;

        const uint64_t maxBytesPerPartition = 640 * 1024 * 1024;
        const uint64_t maxReferentsPerPartition = 10 * 1000 * 1000;

        for (uint64_t i = 0; i < serverTablets; i++) {
            Table* t = createAndAddTablet(tablets, 0, i, 0, -1);
            for (uint64_t j = 0; j < bytesPerTablet/objectBytes; j++)
                t->profiler.track(j, objectBytes, LogTime(i, j));
        }

        const int loops = 3;
        uint64_t totalTicks = 0;
        uint64_t willLength = 0;
        for (int i = 0; i < loops; i++) {
            // try to trash the cache
            void* foo = Memory::xmalloc(HERE, 100 * 1024 * 1024);
            memset(foo, 0, 100 * 1024 * 1024);
            free(foo);

            uint64_t b = Cycles::rdtsc();
            Will w(tablets, maxBytesPerPartition, maxReferentsPerPartition);
            willLength += w.entries.size();
            totalTicks += Cycles::rdtsc() - b;
        }
        printf("%luMB, %lu tablets, %d bytes/object, %lu will entries: "
            "%.0f usec\n", serverBytes / 1024 / 1024, serverTablets,
            objectBytes, willLength / loops,
            RAMCloud::Cycles::toSeconds(totalTicks / loops) * 1e06);

        destroyTables(tablets);
    }

    DISALLOW_COPY_AND_ASSIGN(WillBenchmark);
};

}  // namespace RAMCloud

int
main()
{
    RAMCloud::WillBenchmark wb;
    for (int j = 16384; j >= 64; j /= 2) {
        if (j != 16384)
            printf("\n");
        for (int i = 1; i <= 1000000; i *= 10)
            wb.run(64UL * 1024 * 1024 * 1024, i, j);
    }
    return 0;
}
