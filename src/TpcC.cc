/* Copyright (c) 2015 Stanford University
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

#include "TpcC.h"

#include <set>
#include "TimeTrace.h"
#include "Transaction.h"
#include "Util.h"

namespace RAMCloud { namespace TPCC {

uint64_t tableId[100];

Driver::Driver(RamCloud* ramcloud, uint32_t numWarehouse, int serverSpan)
    : ramcloud(ramcloud)
    , context(numWarehouse, serverSpan)
{
    for (uint32_t W_ID = 1; W_ID <= context.numWarehouse; ++W_ID) {
        char tname[20];
        snprintf(tname, 20, "Warehouse%d", W_ID);
        tableId[W_ID] = ramcloud->createTable(tname, context.serverSpan);
        tableId[W_ID] = ramcloud->getTableId(tname);
    }
}

Driver::Driver(RamCloud* ramcloud, TpccContext& c)
    : ramcloud(ramcloud)
    , context(c)
{
    for (uint32_t W_ID = 1; W_ID <= context.numWarehouse; ++W_ID) {
        char tname[20];
        snprintf(tname, 20, "Warehouse%d", W_ID);
        tableId[W_ID] = ramcloud->createTable(tname, context.serverSpan);
        tableId[W_ID] = ramcloud->getTableId(tname);
    }
}

static int
genRandomAString(char* target, int minLen, int maxLen, bool endingNull = true)
{
    const char alphanum[] =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    int nameLen = rand() % (maxLen - minLen + 1) + minLen;
    for (int i = 0; i < nameLen; ++i) {
        target[i] = alphanum[rand() % (sizeof(alphanum))];
    }
    if (endingNull)
        target[nameLen] = 0;
    return nameLen;
}

static void
genRandomNString(char* target, int minLen, int maxLen, bool endingNull = true)
{
    const char num[] = "0123456789";
    int nameLen = rand() % (maxLen - minLen + 1) + minLen;
    for (int i = 0; i < nameLen; ++i) {
        target[i] = num[rand() % (sizeof(num))];
    }
    if (endingNull)
        target[nameLen] = 0;
}

static void
genZip(char* target)
{
    genRandomNString(target, 4, 4, false);
    for (int i = 0; i < 5; ++i) {
        target[i] = '1';
    }
}

void
genLastName(char* target, int rand)
{
    char names[10][6] =
        {"BAR", "OUGHT", "ABLE", "PRI", "PRES",
         "ESE", "ANTI", "CALLY", "ATION", "EING"};
    assert(rand < 1000);
    strncpy(target, names[rand / 100], 6);
    strncat(target, names[(rand / 10) % 10], 6);
    strncat(target, names[rand % 10], 6);
}

uint32_t
random(uint32_t x, uint32_t y)
{
    return rand() % (y - x + 1) + x;
}

uint16_t
random16(uint32_t x, uint32_t y)
{
    return static_cast<uint16_t>(random(x, y));
}

uint8_t
random8(uint32_t x, uint32_t y)
{
    return static_cast<uint8_t>(random(x, y));
}

uint32_t
NURand(uint32_t A, uint32_t x, uint32_t y)
{
    return (((random(0, A) | random(x, y)) + random(0, A)) % (y - x + 1)) + x;
}

void
Driver::addItem(uint32_t I_ID, uint32_t W_ID)
{
    Item i(I_ID, W_ID);
    i.data.I_IM_ID = rand() % 10000 + 1;
    genRandomAString(i.data.I_NAME, 14, 24);
    i.data.I_PRICE = static_cast<double>(rand() % 9901 + 100) / 100;
    int dataLen = genRandomAString(i.data.I_DATA, 26, 50);

    if (rand() % 10 == 0) {
        int startAt = rand() % (dataLen - 8 + 1);
        const char s[] = "ORIGINAL";
        for (int j = 0; j < 8; ++j) {
            i.data.I_DATA[startAt + j] = s[j];
        }
    }
    write(tableId[W_ID], i);
}

void
Driver::addWarehouse(uint32_t W_ID)
{
    Warehouse w(W_ID);
    genRandomAString(w.data.W_NAME, 6, 10);
    genRandomAString(w.data.W_STREET_1, 10, 20);
    genRandomAString(w.data.W_STREET_2, 10, 20);
    genRandomAString(w.data.W_CITY, 10, 20);
    genRandomAString(w.data.W_STATE, 2, 2, false);
    genZip(w.data.W_ZIP);
    w.data.W_TAX = static_cast<float>(rand() % 2001) / 10000;
    w.data.W_YTD = 300000.0;

    write(tableId[W_ID], w);
}

void
Driver::addStock(uint32_t I_ID, uint32_t W_ID)
{
    Stock s(I_ID, W_ID);
    s.data.S_QUANTITY = static_cast<uint16_t>(rand() % 91 + 10);
    for (int dist = 0; dist < 10; ++dist) {
        genRandomAString(s.data.S_DIST[dist], 24, 24, false);
    }
    s.data.S_YTD = 0;
    s.data.S_ORDER_CNT = 0;
    s.data.S_REMOTE_CNT = 0;
    int dataLen = genRandomAString(s.data.S_DATA, 26, 50);

    if (rand() % 10 == 0) {
        int startAt = rand() % (dataLen - 8 + 1);
        const char str[] = "ORIGINAL";
        for (int j = 0; j < 8; ++j) {
            s.data.S_DATA[startAt + j] = str[j];
        }
    }

    write(tableId[W_ID], s);
}

void
Driver::addDistrict(uint32_t D_ID, uint32_t W_ID)
{
    District d(D_ID, W_ID);
    genRandomAString(d.data.D_NAME, 6, 10);
    genRandomAString(d.data.D_STREET_1, 10, 20);
    genRandomAString(d.data.D_STREET_2, 10, 20);
    genRandomAString(d.data.D_CITY, 10, 20);
    genRandomAString(d.data.D_STATE, 2, 2, false);
    genZip(d.data.D_ZIP);
    d.data.D_TAX = static_cast<float>(rand() % 2001) / 10000;
    d.data.D_YTD = 30000.0;
    d.data.D_NEXT_O_ID = 3001;
    d.data.lowestToDeliverOid = 2101;

    write(tableId[W_ID], d);
}

void
Driver::addCustomer(uint32_t C_ID, uint32_t D_ID, uint32_t W_ID,
                    std::string* lastName)
{
    Customer c(C_ID, D_ID, W_ID);
    if (C_ID <= 1000) {
        genLastName(c.data.C_LAST, rand() % 1000);
    } else {
        genLastName(c.data.C_LAST, NURand(255, 0, 999));
    }
    c.data.C_MIDDLE[0] = 'O';
    c.data.C_MIDDLE[1] = 'E';
    genRandomAString(c.data.C_FIRST, 8, 16);
    genRandomAString(c.data.C_STREET_1, 10, 20);
    genRandomAString(c.data.C_STREET_2, 10, 20);
    genRandomAString(c.data.C_CITY, 10, 20);
    genRandomAString(c.data.C_STATE, 2, 2, false);
    genZip(c.data.C_ZIP);
    genRandomNString(c.data.C_PHONE, 16, 16, false);
    c.data.C_SINCE = time(NULL);
    if (rand() % 10 == 0) {
        memcpy(c.data.C_CREDIT, "BC", 2);
    } else {
        memcpy(c.data.C_CREDIT, "GC", 2);
    }
    c.data.C_CREDIT_LIM = 50000.00;
    c.data.C_DISCOUNT = (rand() % 5001) / 10000;
    c.data.C_BALANCE = -10.00;
    c.data.C_YTD_PAYMENT = 10.00;
    c.data.C_PAYMENT_CNT = 1;
    c.data.C_DELIVERY_CNT = 0;
    genRandomAString(c.data.C_DATA, 300, 500);

    write(tableId[W_ID], c);

    lastName->append(c.data.C_LAST);

    // TODO(seojin): Add last name to id table row.
}

void
Driver::addHistory(uint32_t C_ID, uint32_t D_ID, uint32_t W_ID)
{
    History h(C_ID, D_ID, W_ID);
    h.data.H_D_ID = D_ID;
    h.data.H_W_ID = W_ID;
    h.data.H_DATE = time(NULL);
    h.data.H_AMOUNT = 10.00;
    genRandomAString(h.data.H_DATA, 12, 24);

    write(tableId[W_ID], h);
}

void
Driver::addOrder(uint32_t O_ID, uint32_t D_ID, uint32_t W_ID, uint32_t C_ID)
{
    Order o(O_ID, D_ID, W_ID);
    o.data.O_C_ID = C_ID;
    o.data.O_ENTRY_D = time(NULL);
    if (O_ID < 2101) {
        o.data.O_CARRIER_ID = static_cast<uint8_t>(random(1, 10));
    } else {
        o.data.O_CARRIER_ID = 0;
    }
    o.data.O_OL_CNT = static_cast<uint8_t>(random(5, 15));
    o.data.O_ALL_LOCAL = 1;

    vector<MultiWriteObject*> mulWrites;
    mulWrites.push_back(multiWrite(tableId[W_ID], o));

    Tub<OrderLine> ols[15];
    for (uint8_t OL_NUMBER = 0; OL_NUMBER < o.data.O_OL_CNT; ++OL_NUMBER) {
        ols[OL_NUMBER].construct(O_ID, D_ID, W_ID, OL_NUMBER);
        OrderLine& ol = *ols[OL_NUMBER];

        ol.data.OL_I_ID = random(1, 100000);
        ol.data.OL_SUPPLY_W_ID = W_ID;
        if (O_ID < 2101) {
            ol.data.OL_DELIVERY_D = o.data.O_ENTRY_D;
        } else {
            ol.data.OL_DELIVERY_D = 0;
        }
        ol.data.OL_QUANTITY = 5;
        if (O_ID < 2101) {
            ol.data.OL_AMOUNT = 0.00;
        } else {
            ol.data.OL_AMOUNT = (rand() % 999999 + 1) / 100;
        }
        genRandomAString(ol.data.OL_DIST_INFO, 24, 24, false);

        mulWrites.push_back(multiWrite(tableId[W_ID], ol));
    }
    ramcloud->multiWrite(mulWrites.data(),
                         static_cast<uint32_t>(mulWrites.size()));
    for (size_t i = 0; i < mulWrites.size(); ++i) {
        delete mulWrites[i];
    }

    Customer c(C_ID, D_ID, W_ID);
    ramcloud->write(tableId[W_ID], c.pKey(), c.lastOidKeyLength(),
                    &O_ID, sizeof32(O_ID));
}

void
Driver::addNewOrder(uint32_t O_ID, uint32_t D_ID, uint32_t W_ID)
{
    NewOrder no(O_ID, D_ID, W_ID);
    write(tableId[W_ID], no);
}

void
Driver::initBenchmark()
{
    assert(!context.initialized);
    uint64_t startCycles = Cycles::rdtsc();
    createTables();
    RAMCLOUD_LOG(NOTICE, "TPCC benchmark initialization: tables created.");

    for (uint32_t W_ID = 1; W_ID <= context.numWarehouse; ++W_ID) {

        addWarehouse(W_ID);

        for (uint32_t I_ID = 1; I_ID <= 100000; ++I_ID) {
            addItem(I_ID, W_ID);
        }

        for (uint32_t I_ID = 1; I_ID <= 100000; ++I_ID) {
            addStock(I_ID, W_ID);
        }

        for (uint32_t D_ID = 1; D_ID <= 10; ++D_ID) {
            addDistrict(D_ID, W_ID);
            //printf("WID:%d DID:%d district added.\n", W_ID, D_ID);

            std::vector<uint32_t> rand_cid;
            std::unordered_map<std::string, std::vector<uint32_t>> nameToCid;
            for (uint32_t C_ID = 1; C_ID <= 3000; ++C_ID) {
                std::string lastName;
                addCustomer(C_ID, D_ID, W_ID, &lastName);
                nameToCid[lastName].push_back(C_ID);

                addHistory(C_ID, D_ID, W_ID);
                rand_cid.push_back(C_ID);
            }
            //printf("WID:%d DID:%d c and h added.\n", W_ID, D_ID);

            for (auto it = nameToCid.begin();
                    it != nameToCid.end(); ++it) {
                Buffer toWrite;
                uint32_t size = static_cast<uint32_t>(it->second.size());
                toWrite.appendCopy(&size, sizeof(size));
                toWrite.appendExternal(it->second.data(),
                            static_cast<uint32_t>(it->second.size()) * 4);
                cNameKey nameKey;
                memcpy(nameKey.lastName, it->first.data(), it->first.size());
                ramcloud->write(tableId[W_ID], &nameKey,
                                static_cast<uint16_t>(sizeof(nameKey)),
                                toWrite.getRange(0, toWrite.size()),
                                toWrite.size());
            }

            std::random_shuffle (rand_cid.begin(), rand_cid.end());
            for (uint32_t O_ID = 1; O_ID <= 3000; ++O_ID) {
                addOrder(O_ID, D_ID, W_ID, rand_cid[O_ID - 1]);
            }
            //printf("WID:%d DID:%d o added.\n", W_ID, D_ID);

            for (uint32_t O_ID = 2101; O_ID <= 3000; ++O_ID) {
                addNewOrder(O_ID, D_ID, W_ID);
            }
            //printf("WID:%d DID:%d ol added.\n", W_ID, D_ID);
        }
    }
    uint64_t elapsed = Cycles::rdtsc() - startCycles;
    RAMCLOUD_LOG(NOTICE, "TPCC benchmark initialization completed in %lfms.",
                 Cycles::toSeconds(elapsed) *1e03);
    context.initialized = true;
}

void
Driver::initBenchmark(uint32_t W_ID)
{
    uint64_t startCycles = Cycles::rdtsc();
    createTable(W_ID);
    RAMCLOUD_LOG(NOTICE, "TPCC benchmark initialization: tables created.");


    addWarehouse(W_ID);

    for (uint32_t I_ID = 1; I_ID <= 100000; ++I_ID) {
        addItem(I_ID, W_ID);
    }

    for (uint32_t I_ID = 1; I_ID <= 100000; ++I_ID) {
        addStock(I_ID, W_ID);
    }

    for (uint32_t D_ID = 1; D_ID <= 10; ++D_ID) {
        addDistrict(D_ID, W_ID);
        //printf("WID:%d DID:%d district added.\n", W_ID, D_ID);

        std::vector<uint32_t> rand_cid;
        std::unordered_map<std::string, std::vector<uint32_t>> nameToCid;
        for (uint32_t C_ID = 1; C_ID <= 3000; ++C_ID) {
            std::string lastName;
            addCustomer(C_ID, D_ID, W_ID, &lastName);
            nameToCid[lastName].push_back(C_ID);

            addHistory(C_ID, D_ID, W_ID);
            rand_cid.push_back(C_ID);
        }
        //printf("WID:%d DID:%d c and h added.\n", W_ID, D_ID);

        for (auto it = nameToCid.begin();
                it != nameToCid.end(); ++it) {
            Buffer toWrite;
            uint32_t size = static_cast<uint32_t>(it->second.size());
            toWrite.appendCopy(&size, sizeof(size));
            toWrite.appendExternal(it->second.data(),
                        static_cast<uint32_t>(it->second.size()) * 4);
            cNameKey nameKey;
            memcpy(nameKey.lastName, it->first.data(), it->first.size());
            ramcloud->write(tableId[W_ID], &nameKey,
                            static_cast<uint16_t>(sizeof(nameKey)),
                            toWrite.getRange(0, toWrite.size()),
                            toWrite.size());
        }

        std::random_shuffle (rand_cid.begin(), rand_cid.end());
        for (uint32_t O_ID = 1; O_ID <= 3000; ++O_ID) {
            addOrder(O_ID, D_ID, W_ID, rand_cid[O_ID - 1]);
        }
        //printf("WID:%d DID:%d o added.\n", W_ID, D_ID);

        for (uint32_t O_ID = 2101; O_ID <= 3000; ++O_ID) {
            addNewOrder(O_ID, D_ID, W_ID);
        }
        //printf("WID:%d DID:%d ol added.\n", W_ID, D_ID);
    }
    uint64_t elapsed = Cycles::rdtsc() - startCycles;
    RAMCLOUD_LOG(NOTICE, "TPCC benchmark initialization completed in %lfms.",
                 Cycles::toSeconds(elapsed) *1e03);
    context.initialized = true;
}

void
Driver::createTables()
{
    for (uint32_t W_ID = 1; W_ID <= context.numWarehouse; ++W_ID) {
        char tname[20];
        snprintf(tname, 20, "Warehouse%d", W_ID);
        tableId[W_ID] = ramcloud->createTable(tname, context.serverSpan);
    }
}

void
Driver::createTable(uint32_t W_ID)
{
    char tname[20];
    snprintf(tname, 20, "Warehouse%d", W_ID);
    tableId[W_ID] = ramcloud->createTable(tname, context.serverSpan);
}

void
Driver::write(uint64_t tid, Row& r)
{
    ramcloud->write(tid, r.pKey(), r.pKeyLength(), r.value(), r.valueLength());
}

void
Driver::read(Row* row)
{
    Buffer buf;
    ramcloud->read(row->tid(), row->pKey(), row->pKeyLength(), &buf);
    row->parseBuffer(buf);
}

MultiWriteObject*
Driver::multiWrite(uint64_t tid, Row& r)
{
    return new MultiWriteObject(tid, r.pKey(), r.pKeyLength(),
                                r.value(), r.valueLength());
}

/////////////////////////////////////////////
// Transaction Operations                  //
/////////////////////////////////////////////
/*
static void
readRows(Transaction* t, Row** rows, int numRows)
{
    Buffer bufs[numRows];
    Tub<Transaction::ReadOp> ops[numRows];
    for (int i = 0; i < numRows; ++i) {
        ops[i].construct(t, rows[i]->tid(),
                         rows[i]->pKey(), rows[i]->pKeyLength(),
                         &bufs[i]);
    }
    for (int i = 0; i < numRows; ++i) {
        ops[i]->wait();
        rows[i]->parseBuffer(bufs[i]);
    }
} */

static void
readRow(Transaction* t, Row* row)
{
    Buffer buf;
    t->read(row->tid(), row->pKey(), row->pKeyLength(), &buf);
    row->parseBuffer(buf);
}

static void
readRows(Transaction* t, std::vector<Row*>& rows)
{
    Buffer bufs[rows.size()];
    Tub<Transaction::ReadOp> ops[rows.size()];
    for (size_t i = 0; i < rows.size(); ++i) {
        ops[i].construct(t, rows[i]->tid(),
                         rows[i]->pKey(), rows[i]->pKeyLength(),
                         &bufs[i], true);
    }
    for (size_t i = 0; i < rows.size(); ++i) {
        ops[i]->wait();
        rows[i]->parseBuffer(bufs[i]);
    }
}

/*
static void
readRowsMulti(Transaction* t, std::vector<Row*>& rows)
{
    {
        Tub<ObjectBuffer> bufs[rows.size()];
        Tub<MultiReadObject> ops[rows.size()];
        MultiReadObject* requests[rows.size()];

        for (size_t i = 0; i < rows.size() / 2; ++i) {
            ops[i].construct(rows[i]->tid(),
                             rows[i]->pKey(), rows[i]->pKeyLength(),
                             &bufs[i]);
            requests[i] = ops[i].get();
        }

        Transaction::MultiReadOp txRead(t, requests, (uint32_t) rows.size() / 2);
        txRead.wait();
        for (size_t i = 0; i < rows.size() / 2; ++i) {
            rows[i]->parseBuffer(*bufs[i]);
        }
    }
    {
        Tub<ObjectBuffer> bufs[rows.size()];
        Tub<MultiReadObject> ops[rows.size()];
        MultiReadObject* requests[rows.size()];

        for (size_t i = rows.size() / 2; i < rows.size(); ++i) {
            ops[i].construct(rows[i]->tid(),
                             rows[i]->pKey(), rows[i]->pKeyLength(),
                             &bufs[i]);
            requests[i] = ops[i].get();
        }

        Transaction::MultiReadOp txRead(t, &requests[(rows.size() / 2)], (uint32_t) (rows.size() - (rows.size() / 2)));
        txRead.wait();
        for (size_t i = rows.size() / 2; i < rows.size(); ++i) {
            rows[i]->parseBuffer(*bufs[i]);
        }
    }
}*/

/*
static void
readRowsSync(Transaction* t, std::vector<Row*>& rows)
{
    for (size_t i = 0; i < rows.size(); ++i) {
        Buffer buf;
        t->read(rows[i]->tid(),
                rows[i]->pKey(), rows[i]->pKeyLength(),
                &buf);
        rows[i]->parseBuffer(buf);
    }
}*/

static void
writeRow(Transaction* t, Row* row)
{
    t->write(row->tid(), row->pKey(), row->pKeyLength(),
             row->value(), row->valueLength());
}

/**
 * It generates an input randomly according to the TPC-C specification.
 */
void
InputNewOrder::generate(int W_ID, int numWarehouse)
{
    D_ID = random(1, 10);
    C_ID = NURand(1023, 1, 3000);
    O_OL_CNT = random8(5, 15);
    for (int i = 0; i < O_OL_CNT; ++i) {
        I_IDs[i] = NURand(8191, 1, 100000);
        OL_QUANTITY[i] = random8(1, 10);
        if (rand() % 100 == 0) {
            OL_SUPPLY_W_ID[i] = random(1, numWarehouse);
        } else {
            OL_SUPPLY_W_ID[i] = W_ID;
        }
    }
}

double
Driver::txNewOrder(uint32_t W_ID, bool* outcome, InputNewOrder* in)
{
    ///////////////////////////
    // Input Data Generation
    ///////////////////////////
    Tub<InputNewOrder> realInput;
    if (!in) {
        realInput.construct();
        realInput->generate(W_ID, context.numWarehouse);
        in = realInput.get();
    }
    uint32_t D_ID = in->D_ID;
    uint32_t C_ID = in->C_ID;
    uint8_t O_OL_CNT = in->O_OL_CNT;
    uint32_t* I_IDs = in->I_IDs;
    uint8_t* OL_QUANTITY = in->OL_QUANTITY;
    uint32_t* OL_SUPPLY_W_ID = in->OL_SUPPLY_W_ID;

    ///////////////////////////
    // Create Order header.
    ///////////////////////////
    uint64_t startCycles = Cycles::rdtsc();
    Transaction t(ramcloud);

    // Reads
    std::vector<Row*> readList;
    Warehouse w(W_ID);
    readList.push_back(&w);

    District d(D_ID, W_ID);
    readList.push_back(&d);

    Customer c(C_ID, D_ID, W_ID);
    readList.push_back(&c);

    Tub<Item> items[15];
    Tub<Stock> stocks[15];
    for (int i = 0; i < O_OL_CNT; ++i) {
        items[i].construct(I_IDs[i], W_ID);
        readList.push_back(items[i].get());
        stocks[i].construct(I_IDs[i], W_ID);
        readList.push_back(stocks[i].get());
    }
    //TODO: Try-catch not found exception.
    //readRowsMulti(&t, readList);
    readRows(&t, readList);
    //readRowsSync(&t, readList);

    // Writes
    uint32_t O_ID = d.data.D_NEXT_O_ID++;
    writeRow(&t, &d);

    NewOrder no(O_ID, D_ID, W_ID);
    writeRow(&t, &no);

    Order o(O_ID, D_ID, W_ID);
    o.data.O_C_ID = C_ID;
    o.data.O_ENTRY_D = time(NULL);
    o.data.O_CARRIER_ID = 0;
    o.data.O_OL_CNT = O_OL_CNT;
    o.data.O_ALL_LOCAL = 1;

    Tub<OrderLine> ols[15];
    double sum = 0;
    for (uint8_t i = 0; i < O_OL_CNT; ++i) {
        // STOCK
        if (stocks[i]->data.S_QUANTITY >= 10) {
            stocks[i]->data.S_QUANTITY = static_cast<uint16_t>(
                    stocks[i]->data.S_QUANTITY - OL_QUANTITY[i]);
        } else {
            stocks[i]->data.S_QUANTITY = static_cast<uint16_t>(
                    stocks[i]->data.S_QUANTITY + 91 - OL_QUANTITY[i]);
        }
        stocks[i]->data.S_YTD += OL_QUANTITY[i];
        stocks[i]->data.S_ORDER_CNT++;
        if (OL_SUPPLY_W_ID[i] != W_ID) {
            stocks[i]->data.S_REMOTE_CNT++;
            o.data.O_ALL_LOCAL = 0;
        }
        writeRow(&t, stocks[i].get());

        // ORDER LINE
        ols[i].construct(O_ID, D_ID, W_ID, i);
        ols[i]->data.OL_I_ID = I_IDs[i];
        ols[i]->data.OL_SUPPLY_W_ID = OL_SUPPLY_W_ID[i];
        ols[i]->data.OL_DELIVERY_D = 0;
        ols[i]->data.OL_QUANTITY = OL_QUANTITY[i];
        ols[i]->data.OL_AMOUNT = OL_QUANTITY[i] * items[i]->data.I_PRICE;
        sum += ols[i]->data.OL_AMOUNT;
        memcpy(ols[i]->data.OL_DIST_INFO, stocks[i]->data.S_DIST[D_ID-1], 24);
        writeRow(&t, ols[i].get());
    }

    writeRow(&t, &o);

    t.write(tableId[W_ID], c.pKey(), c.lastOidKeyLength(),
                    &O_ID, sizeof32(O_ID));


    *outcome = t.commit();

    uint64_t elapsed = Cycles::rdtsc() - startCycles;

    /*
    double totalAmount = sum * (1 - c.data.C_DISCOUNT)
                             * (1 + w.data.W_TAX + d.data.D_TAX);
    char brandGeneric[15];
    for (uint8_t i = 0; i < O_OL_CNT; ++i) {
        if (strstr(items[i]->data.I_DATA, "ORIGINAL") != NULL &&
            strstr(stocks[i]->data.S_DATA, "ORIGINAL") != NULL) {
            brandGeneric[i] = 'B';
        } else {
            brandGeneric[i] = 'G';
        }
    }
    */

    //TODO: IO requirement TPC-C 2.4.3.5.
    //      H-Store just pass values back to terminal.

    t.sync();

    return Cycles::toSeconds(elapsed) *1e06;
}

/**
 * It generates an input randomly according to the TPC-C specification.
 */
void
InputPayment::generate(int W_ID, int numWarehouse)
{
    D_ID = random(1, 10);
    byLastName = false;
    C_ID = 0;

    if (rand() % 100 < 85) {
        C_D_ID = D_ID;
        C_W_ID = W_ID;
    } else {
        C_D_ID = random(1, 10);
        C_W_ID = random(1, numWarehouse); //Exclude local warehouse?
    }
    if (rand() % 100 < 60) {
        byLastName = true;
        genLastName(lastName, NURand(255, 0, 999));
    } else {
        byLastName = false;
        C_ID = NURand(1023, 1, 3000);
    }
    H_AMOUNT = (double)random(100, 500000) / 100.0;
}

double
Driver::txPayment(uint32_t W_ID, bool *outcome, InputPayment* in)
{
    ///////////////////////////
    // Input Data Generation
    ///////////////////////////
    Tub<InputPayment> realInput;
    if (!in) {
        realInput.construct();
        realInput->generate(W_ID, context.numWarehouse);
        in = realInput.get();
    }
    uint32_t D_ID = in->D_ID;
    uint32_t C_ID = in->C_ID;
    uint32_t C_W_ID = in->C_W_ID;
    uint32_t C_D_ID = in->C_D_ID;
    bool byLastName = in->byLastName;
    cNameKey nameKey;
    strncpy(nameKey.lastName, in->lastName, 17);
    double H_AMOUNT = in->H_AMOUNT;

    ///////////////////////////
    // Process: Read.
    ///////////////////////////
    uint64_t startCycles = Cycles::rdtsc();
    Transaction t(ramcloud);

    Buffer buf_cid;
    int numCustomer = 0;
    uint32_t buf_idx = 0;

    if (byLastName) {
        t.read(tableId[W_ID], &nameKey, static_cast<uint16_t>(sizeof(nameKey)),
               &buf_cid);
        numCustomer = *(buf_cid.getStart<uint32_t>());
        buf_idx += sizeof32(numCustomer);
        if (numCustomer == 0) {
            //TODO abort TX.
            return 0;
        }

        uint32_t* cids;
        cids = static_cast<uint32_t*>(buf_cid.getRange(buf_idx, numCustomer*4));
        in->C_ID = cids[(int)((numCustomer - 1) / 2)];
        C_ID = in->C_ID;
    }
    assert(C_ID);

    std::vector<Row*> readList;
    Warehouse w(W_ID);
    readList.push_back(&w);

    District d(D_ID, W_ID);
    readList.push_back(&d);

    Customer c(C_ID, C_D_ID, C_W_ID);
    readList.push_back(&c);

    readRows(&t, readList);

    ///////////////////////////
    // Process: Write.
    ///////////////////////////
    w.data.W_YTD += H_AMOUNT;
    writeRow(&t, &w);

    d.data.D_YTD += H_AMOUNT;
    writeRow(&t, &d);

    c.data.C_BALANCE -= H_AMOUNT;
    c.data.C_YTD_PAYMENT += H_AMOUNT;
    c.data.C_PAYMENT_CNT++;
    if (strncmp(c.data.C_CREDIT, "BC", 2) == 0) {
        std::ostringstream newData;
        newData << C_ID << " " << C_D_ID << " " << C_W_ID << " "
                << D_ID << " " << W_ID << " " << H_AMOUNT << std::endl;
        std::string newDataStr = newData.str();
        for (size_t i = 499; i >= newDataStr.size(); --i) {
            c.data.C_DATA[i] = c.data.C_DATA[i - newDataStr.size()];
        }
        memcpy(c.data.C_DATA, newDataStr.c_str(), newDataStr.size());
        c.data.C_DATA[500] = 0;
    }
    writeRow(&t, &c);

    //TODO (seojin): allow multiple rows with same key. Prevent overwrite by
    //               adding sequence number to pKey.
    History h(C_ID, C_D_ID, C_W_ID);
    std::string h_data;
    h_data.append(w.data.W_NAME);
    h_data.append("    ");
    h_data.append(d.data.D_NAME);
    strncpy(h.data.H_DATA, h_data.c_str(), 24);
    h.data.H_DATA[24] = 0;
    h.data.H_D_ID = D_ID;
    h.data.H_W_ID = W_ID;
    h.data.H_AMOUNT = H_AMOUNT;
    h.data.H_DATE = time(NULL);
    writeRow(&t, &h);

    if (!t.commit()) {
        RAMCLOUD_LOG(DEBUG, "Payment TX aborted.");
        *outcome = false;
    } else {
        *outcome = true;
    }
    t.sync();
    uint64_t elapsed = Cycles::rdtsc() - startCycles;
    return Cycles::toSeconds(elapsed) *1e06;
}

void
InputOrderStatus::generate()
{
    D_ID = random(1, 10);
    C_ID = 0;
    byLastName = false;

    if (rand() % 100 < 60) {
        byLastName = true;
        genLastName(lastName, NURand(255, 0, 999));
    } else {
        byLastName = false;
        C_ID = NURand(1023, 1, 3000);
    }
}

double
Driver::txOrderStatus(uint32_t W_ID, bool *outcome, InputOrderStatus* in)
{
    ///////////////////////////
    // Input Data Generation
    ///////////////////////////
    Tub<InputOrderStatus> realInput;
    if (!in) {
        realInput.construct();
        realInput->generate();
        in = realInput.get();
    }
    uint32_t D_ID = in->D_ID;
    uint32_t C_ID = in->C_ID;
    bool byLastName = in->byLastName;
    cNameKey nameKey;
    strncpy(nameKey.lastName, in->lastName, 17);

    ///////////////////////////
    // Process: Read.
    ///////////////////////////
    uint64_t startCycles = Cycles::rdtsc();
    Transaction t(ramcloud);

    Buffer buf_cid;
    int numCustomer = 0;
    uint32_t buf_idx = 0;
    if (byLastName) {
        t.read(tableId[W_ID], &nameKey, static_cast<uint16_t>(sizeof(nameKey)),
               &buf_cid);
        numCustomer = *(buf_cid.getStart<uint32_t>());
        buf_idx += sizeof32(numCustomer);
        if (numCustomer == 0) {
            return 0;
        }

        uint32_t* cids;
        cids = static_cast<uint32_t*>(buf_cid.getRange(buf_idx, numCustomer*4));
        in->C_ID = cids[(int)((numCustomer - 1) / 2)];
        C_ID = in->C_ID;
    }
    assert(C_ID);

    std::vector<Row*> readList;

    Customer c(C_ID, D_ID, W_ID);
    readList.push_back(&c);

    Buffer buf_oid;
    t.read(tableId[W_ID], c.pKey(), c.lastOidKeyLength(), &buf_oid);
    uint32_t O_ID = *(buf_oid.getStart<uint32_t>());

    Order o(O_ID, D_ID, W_ID);
    readList.push_back(&o);

    readRows(&t, readList);
    readList.clear();

    Tub<OrderLine> ols[15];
    for (uint8_t i = 0; i < o.data.O_OL_CNT; ++i) {
        ols[i].construct(O_ID, D_ID, W_ID, i);
        readList.push_back(ols[i].get());
    }
    readRows(&t, readList);

    *outcome = t.commit();
    uint64_t elapsed = Cycles::rdtsc() - startCycles;
    return Cycles::toSeconds(elapsed) *1e06;
}

double
Driver::txDelivery(uint32_t W_ID, uint32_t D_ID, bool* outcome, InputDelivery *in)
{
    ///////////////////////////
    // Input Data Generation
    ///////////////////////////
    Tub<InputDelivery> realInput;
    if (!in) {
        realInput.construct();
        realInput->generate();
        in = realInput.get();
    }

    ///////////////////////////
    // Transaction Processing.
    ///////////////////////////
    uint64_t startCycles = Cycles::rdtsc();
    Transaction t(ramcloud);

    District d(D_ID, W_ID);
    readRow(&t, &d);

    uint32_t O_ID = d.data.lowestToDeliverOid;
    if (O_ID >= d.data.D_NEXT_O_ID) { //No new order.
        *outcome = true;
        uint64_t elapsed = Cycles::rdtsc() - startCycles;
        return Cycles::toSeconds(elapsed) *1e06;
    }
    d.data.lowestToDeliverOid++;
    writeRow(&t, &d);

    NewOrder no(O_ID, D_ID, W_ID);
    t.remove(no.tid(), no.pKey(), no.pKeyLength());

    Order o(O_ID, D_ID, W_ID);
    readRow(&t, &o);
    o.data.O_CARRIER_ID = in->O_CARRIER_ID;
    writeRow(&t, &o);

    std::vector<Row*> readList;

    Customer c(o.data.O_C_ID, D_ID, W_ID);
    readList.push_back(&c);

    Tub<OrderLine> ols[15];
    for (uint8_t i = 0; i < o.data.O_OL_CNT; ++i) {
        ols[i].construct(O_ID, D_ID, W_ID, i);
        readList.push_back(ols[i].get());
    }
    readRows(&t, readList);

    double sum = 0;
    for (uint8_t i = 0; i < o.data.O_OL_CNT; ++i) {
        ols[i]->data.OL_DELIVERY_D = time(NULL);
        sum += ols[i]->data.OL_AMOUNT;
        writeRow(&t, ols[i].get());
    }

    c.data.C_BALANCE += sum;
    c.data.C_DELIVERY_CNT++;
    writeRow(&t, &c);

    *outcome = t.commit();
    uint64_t elapsed = Cycles::rdtsc() - startCycles;
    return Cycles::toSeconds(elapsed) *1e06;
}

double
Driver::txStockLevel(uint32_t W_ID, uint32_t D_ID, bool* outcome, InputStockLevel* in)
{
    ///////////////////////////
    // Input Data Generation
    ///////////////////////////
    Tub<InputStockLevel> realInput;
    if (!in) {
        realInput.construct();
        realInput->generate();
        in = realInput.get();
    }

    ///////////////////////////
    // Transaction Processing.
    ///////////////////////////
    uint64_t startCycles = Cycles::rdtsc();

    District d(D_ID, W_ID);
    read(&d);

    std::set<uint32_t> iids;
    for (uint32_t O_ID = d.data.D_NEXT_O_ID - 1;
         O_ID >= d.data.D_NEXT_O_ID - 20;
         --O_ID) {
        Order o(O_ID, D_ID, W_ID);
        read(&o);

        for (uint8_t i = 0; i < o.data.O_OL_CNT; ++i) {
            OrderLine ol(O_ID, D_ID, W_ID, i);
            read(&ol);
            iids.insert(ol.data.OL_I_ID);
        }
    }

    vector<uint32_t> lowStock;
    for (auto it = iids.begin();
            it != iids.end();
            ++it) {
        Stock s(*it, W_ID);
        read(&s);
        if (s.data.S_QUANTITY < in->threshold) {
            lowStock.push_back(*it);
        }
    }
    *outcome = true;
    uint64_t elapsed = Cycles::rdtsc() - startCycles;
    return Cycles::toSeconds(elapsed) *1e06;
}

}} // namespace TPCC namespace RAMCloud
