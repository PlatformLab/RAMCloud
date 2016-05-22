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

#include "TestUtil.h"       //Has to be first, compiler complains
#include "TpcC.h"
#include "MockCluster.h"
#include "Util.h"

namespace RAMCloud { namespace TPCC {

class TpcCTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    BindTransport::BindSession* session1;
    BindTransport::BindSession* session2;
    BindTransport::BindSession* session3;
    Tub<TPCC::Driver> driver;

    TpcCTest()
        : logEnabler()
        , context()
        , cluster(&context)
        , ramcloud()
        , session1(NULL)
        , session2(NULL)
        , session3(NULL)
        , driver()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);

        ServerConfig config = ServerConfig::forTesting();
        config.master.logBytes = 500 * 1024 * 1024;
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master1";
        config.maxObjectKeySize = 512;
        config.maxObjectDataSize = 1024;
        config.segmentSize = 128*1024;
        config.segletSize = 128*1024;
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master2";
        cluster.addServer(config);
        config.services = {WireFormat::MASTER_SERVICE,
                           WireFormat::PING_SERVICE};
        config.localLocator = "mock:host=master3";
        cluster.addServer(config);
        ramcloud.construct(&context, "mock:host=coordinator");

        // Get pointers to the master sessions.
        Transport::SessionRef session =
                ramcloud->clientContext->transportManager->getSession(
                "mock:host=master1");
        session1 = static_cast<BindTransport::BindSession*>(session.get());
        session = ramcloud->clientContext->transportManager->getSession(
                "mock:host=master2");
        session2 = static_cast<BindTransport::BindSession*>(session.get());
        session = ramcloud->clientContext->transportManager->getSession(
                "mock:host=master3");
        session3 = static_cast<BindTransport::BindSession*>(session.get());

        driver.construct(ramcloud.get(), 1, 1);

        //uint64_t startCycles = Cycles::rdtsc();
        driver->initBenchmark();
        //uint64_t elapsed = Cycles::rdtsc() - startCycles;
        //printf("initBenchmark took: %f sec\n", Cycles::toSeconds(elapsed));
    }

    void read(TPCC::Row* row)
    {
        Buffer buf;
        ramcloud->read(row->tid(), row->pKey(), row->pKeyLength(), &buf);
        row->parseBuffer(buf);
    }

    void initBenchmark();
    void basic();
    void txNewOrder();
    void txPayment_basic();
    void txPayment_badCredit();
    void txPayment_byLastName();
    void txOrderStatus();
    void txOrderStatus_byLastName();
    void txDelivery();
    void txStockLevel();
    void simulate();

    DISALLOW_COPY_AND_ASSIGN(TpcCTest);
};

void TpcCTest::initBenchmark() {
    TPCC::Warehouse w(1);

    Buffer buf;
    ramcloud->read(w.tid(), w.pKey(), w.pKeyLength(), &buf);
    w.parseBuffer(buf);

//    printf("%s %s %s %s %f %lf\n", w.data.W_NAME, w.data.W_STREET_1,
//                                 w.data.W_STREET_1, w.data.W_CITY,
//                                 w.data.W_TAX, w.data.W_YTD);
    EXPECT_NE(0U, strlen(w.data.W_NAME));
}

void TpcCTest::basic() {
    for (int i = 0; i < 10; ++i) {
        bool outcome;
        double latency = driver->txNewOrder(1U, &outcome);
        printf("latency of txNewOrder %lf, outcome:%d\n", latency, outcome);
    }
    for (int i = 0; i < 10; ++i) {
        bool outcome;
        double latency = driver->txPayment(1U, &outcome);
        printf("latency of txPayment %lf, outcome:%d\n", latency, outcome);
    }
    for (int i = 0; i < 10; ++i) {
        bool outcome;
        double latency = driver->txOrderStatus(1U, &outcome);
        printf("latency of txOrderStatus %lf, outcome:%d\n", latency, outcome);
    }
    for (int i = 0; i < 10; ++i) {
        double latency = 0;
        bool outcome = true;
        for (uint32_t D_ID = 1; D_ID <= 10; D_ID++) {
            latency += driver->txDelivery(1U, D_ID, &outcome);
        }
        printf("latency of txDelivery %lf, outcome:%d\n", latency, outcome);
    }
    for (uint32_t D_ID = 1; D_ID <= 10; ++D_ID){
        bool outcome;
        double latency = driver->txStockLevel(1U, D_ID, &outcome);
        printf("latency of txStockLevel %lf, outcome:%d\n", latency, outcome);
    }
}

void TpcCTest::txNewOrder() {
    bool outcome;
    TPCC::InputNewOrder in;
    uint32_t W_ID = 1;
    in.generate(W_ID, driver->context.numWarehouse);

    //////////////////////////////////////////////////////
    // Preserve the previous state.
    //////////////////////////////////////////////////////
    District d(in.D_ID, W_ID);
    read(&d);
    uint32_t O_ID = d.data.D_NEXT_O_ID;

    NewOrder no(O_ID, in.D_ID, W_ID);
    Order o(O_ID, in.D_ID, W_ID);
    EXPECT_THROW(read(&no), ObjectDoesntExistException);
    EXPECT_THROW(read(&o), ObjectDoesntExistException);

    Tub<Stock> oldStocks[15];
    for (int i = 0; i < in.O_OL_CNT; ++i) {
        oldStocks[i].construct(in.I_IDs[i], W_ID);
        read(oldStocks[i].get());
    }

    //////////////////////////////////////////////////////
    // Execute transaction.
    //////////////////////////////////////////////////////
    double latency = driver->txNewOrder(1U, &outcome, &in);
    EXPECT_TRUE(outcome);
    EXPECT_TRUE(latency);

    //////////////////////////////////////////////////////
    // Check modifications by the transaction.
    //////////////////////////////////////////////////////

    // Next oid incremented.
    read(&d);
    EXPECT_EQ(d.data.D_NEXT_O_ID, O_ID + 1);

    // Order header: 2 rows are inserted.
    read(&no);
    read(&o);
    EXPECT_EQ(o.data.O_C_ID, in.C_ID);
    EXPECT_EQ(o.data.O_CARRIER_ID, 0);
    EXPECT_EQ(o.data.O_OL_CNT, in.O_OL_CNT);

    uint8_t expectedAllLocalVal = 1;
    for (int i = 0; i < in.O_OL_CNT; ++i) {
        if (in.OL_SUPPLY_W_ID[i] != W_ID) {
            expectedAllLocalVal = 0;
        }
    }
    EXPECT_EQ(o.data.O_ALL_LOCAL, expectedAllLocalVal);

    // Order items: 1 row is inserted, 1 row updated.
    Tub<Item> items[15];
    Tub<Stock> stocks[15];
    for (int i = 0; i < in.O_OL_CNT; ++i) {
        items[i].construct(in.I_IDs[i], W_ID);
        read(items[i].get());
        stocks[i].construct(in.I_IDs[i], W_ID);
        read(stocks[i].get());
    }
    Tub<OrderLine> ols[15];
    for (uint8_t i = 0; i < in.O_OL_CNT; ++i) {
        // STOCK
        if (oldStocks[i]->data.S_QUANTITY >= 10) {
            EXPECT_EQ(stocks[i]->data.S_QUANTITY, static_cast<uint16_t>(
                    oldStocks[i]->data.S_QUANTITY - in.OL_QUANTITY[i]));
        } else {
            EXPECT_EQ(stocks[i]->data.S_QUANTITY, static_cast<uint16_t>(
                    oldStocks[i]->data.S_QUANTITY + 91 - in.OL_QUANTITY[i]));
        }
        EXPECT_EQ(stocks[i]->data.S_YTD,
                  oldStocks[i]->data.S_YTD + in.OL_QUANTITY[i]);
        EXPECT_EQ(stocks[i]->data.S_ORDER_CNT,
                  oldStocks[i]->data.S_ORDER_CNT + 1);
        if (in.OL_SUPPLY_W_ID[i] != W_ID) {
            EXPECT_EQ(stocks[i]->data.S_REMOTE_CNT,
                      oldStocks[i]->data.S_REMOTE_CNT + 1);
        }

        // ORDER LINE
        ols[i].construct(O_ID, in.D_ID, W_ID, i);
        read(ols[i].get());

        EXPECT_EQ(in.I_IDs[i], ols[i]->data.OL_I_ID);
        EXPECT_EQ(in.OL_SUPPLY_W_ID[i], ols[i]->data.OL_SUPPLY_W_ID);
        EXPECT_EQ(0, ols[i]->data.OL_DELIVERY_D);
        EXPECT_EQ(in.OL_QUANTITY[i], ols[i]->data.OL_QUANTITY);
        EXPECT_EQ(in.OL_QUANTITY[i] * items[i]->data.I_PRICE,
                  ols[i]->data.OL_AMOUNT);
        EXPECT_FALSE(memcmp(ols[i]->data.OL_DIST_INFO,
                            oldStocks[i]->data.S_DIST[in.D_ID-1],
                            24));
    }

    // Check last orderId of a customer.
    Customer c(in.C_ID, in.D_ID, W_ID);
    Buffer buf_oid;
    ramcloud->read(tableId[W_ID], c.pKey(), c.lastOidKeyLength(), &buf_oid);
    EXPECT_EQ(O_ID, *(buf_oid.getStart<uint32_t>()));
}

void TpcCTest::txPayment_basic() {
    bool outcome;
    TPCC::InputPayment in;
    uint32_t W_ID = 1;
    in.generate(W_ID, driver->context.numWarehouse);
    // Overrides the generated input to use C_ID instead of last name.
    in.byLastName = false;
    in.C_ID = TPCC::NURand(1023, 1, 3000);

    //////////////////////////////////////////////////////
    // Preserve the previous state.
    //////////////////////////////////////////////////////
    Warehouse oldW(W_ID);
    District oldD(in.D_ID, W_ID);
    Customer oldC(in.C_ID, in.C_D_ID, in.C_W_ID);
    read(&oldW);
    read(&oldD);
    read(&oldC);
    // TODO: write a History row checker after fixing RAM-777.

    //////////////////////////////////////////////////////
    // Execute transaction.
    //////////////////////////////////////////////////////
    double latency = driver->txPayment(1U, &outcome, &in);
    EXPECT_TRUE(outcome);
    EXPECT_TRUE(latency);

    //////////////////////////////////////////////////////
    // Check modifications by the transaction.
    //////////////////////////////////////////////////////
    Warehouse w(W_ID);
    District d(in.D_ID, W_ID);
    Customer c(in.C_ID, in.C_D_ID, in.C_W_ID);
    read(&w);
    read(&d);
    read(&c);

    EXPECT_EQ(oldW.data.W_YTD + in.H_AMOUNT, w.data.W_YTD);
    EXPECT_EQ(oldD.data.D_YTD + in.H_AMOUNT, d.data.D_YTD);

    EXPECT_EQ(oldC.data.C_BALANCE - in.H_AMOUNT, c.data.C_BALANCE);
    EXPECT_EQ(oldC.data.C_YTD_PAYMENT + in.H_AMOUNT, c.data.C_YTD_PAYMENT);
    EXPECT_EQ(oldC.data.C_PAYMENT_CNT + 1, c.data.C_PAYMENT_CNT);
}

void TpcCTest::txPayment_badCredit() {
    bool outcome;
    TPCC::InputPayment in;
    uint32_t W_ID = 1;
    in.generate(W_ID, driver->context.numWarehouse);
    // Overrides the generated input to use C_ID instead of last name.
    in.byLastName = false;
    // Retry selection of customer until we find one with bad credit.
    bool isBC;
    do {
        in.C_ID = TPCC::NURand(1023, 1, 3000);
        Customer customer(in.C_ID, in.C_D_ID, in.C_W_ID);
        read(&customer);
        isBC = strncmp(customer.data.C_CREDIT, "BC", 2) == 0;
    } while (!isBC);

    //////////////////////////////////////////////////////
    // Preserve the previous state.
    //////////////////////////////////////////////////////
    Customer oldC(in.C_ID, in.C_D_ID, in.C_W_ID);
    read(&oldC);

    //////////////////////////////////////////////////////
    // Execute transaction.
    //////////////////////////////////////////////////////
    double latency = driver->txPayment(1U, &outcome, &in);
    EXPECT_TRUE(outcome);
    EXPECT_TRUE(latency);

    //////////////////////////////////////////////////////
    // Check modifications by the transaction.
    //////////////////////////////////////////////////////
    Warehouse w(W_ID);
    District d(in.D_ID, W_ID);
    Customer c(in.C_ID, in.C_D_ID, in.C_W_ID);
    read(&w);
    read(&d);
    read(&c);

    EXPECT_TRUE(strncmp(c.data.C_CREDIT, "BC", 2) == 0);
    std::ostringstream newData;
    newData << in.C_ID << " " << in.C_D_ID << " " << in.C_W_ID << " "
            << in.D_ID << " " << W_ID << " " << in.H_AMOUNT << std::endl;
    std::string newDataStr = newData.str();

    size_t expectedLen = strlen(oldC.data.C_DATA) + newDataStr.size();
    if (expectedLen > 500)
        expectedLen = 500;
    EXPECT_EQ(expectedLen, strlen(c.data.C_DATA));

    EXPECT_FALSE(memcmp(newDataStr.data(), c.data.C_DATA, newDataStr.size()));
    EXPECT_FALSE(memcmp(oldC.data.C_DATA,
                        c.data.C_DATA + newDataStr.size(),
                        expectedLen - newDataStr.size()));

}

void TpcCTest::txPayment_byLastName() {
    bool outcome;
    TPCC::InputPayment in;
    uint32_t W_ID = 1;
    in.generate(W_ID, driver->context.numWarehouse);
    // Overrides the generated input to use lastName instead of C_ID.
    in.byLastName = true;
    genLastName(in.lastName, 0); //BARBARBAR
    in.C_ID = 0;

    Buffer toWrite;
    uint32_t nameToIdSize = 100;
    std::vector<uint32_t> nameToId;
    for (uint32_t i = 1; i <= 100; i++) {
        nameToId.push_back(i);
    }
    toWrite.appendCopy(&nameToIdSize, sizeof(nameToIdSize));
    toWrite.appendExternal(nameToId.data(),
                static_cast<uint32_t>(nameToId.size()) * 4);
    cNameKey nameKey;
    strncpy(nameKey.lastName, "BARBARBAR", 17);
    ramcloud->write(tableId[W_ID], &nameKey,
                    static_cast<uint16_t>(sizeof(nameKey)),
                    toWrite.getRange(0, toWrite.size()),
                    toWrite.size());
    //////////////////////////////////////////////////////
    // Preserve the previous state.
    //////////////////////////////////////////////////////
    uint32_t expectedCID = 50;
    Warehouse oldW(W_ID);
    District oldD(in.D_ID, W_ID);
    Customer oldC(expectedCID, in.C_D_ID, in.C_W_ID);
    read(&oldW);
    read(&oldD);
    read(&oldC);

    //////////////////////////////////////////////////////
    // Execute transaction.
    //////////////////////////////////////////////////////
    double latency = driver->txPayment(1U, &outcome, &in);
    EXPECT_TRUE(outcome);
    EXPECT_TRUE(latency);

    //////////////////////////////////////////////////////
    // Check modifications by the transaction.
    //////////////////////////////////////////////////////
    EXPECT_EQ(expectedCID, in.C_ID);

    Warehouse w(W_ID);
    District d(in.D_ID, W_ID);
    Customer c(in.C_ID, in.C_D_ID, in.C_W_ID);
    read(&w);
    read(&d);
    read(&c);

    EXPECT_EQ(oldW.data.W_YTD + in.H_AMOUNT, w.data.W_YTD);
    EXPECT_EQ(oldD.data.D_YTD + in.H_AMOUNT, d.data.D_YTD);

    EXPECT_EQ(oldC.data.C_BALANCE - in.H_AMOUNT, c.data.C_BALANCE);
    EXPECT_EQ(oldC.data.C_YTD_PAYMENT + in.H_AMOUNT, c.data.C_YTD_PAYMENT);
    EXPECT_EQ(oldC.data.C_PAYMENT_CNT + 1, c.data.C_PAYMENT_CNT);
}


void TpcCTest::txOrderStatus() {
    bool outcome;
    TPCC::InputOrderStatus in;
    uint32_t W_ID = 1;
    in.generate();
    // Overrides the generated input to use C_ID instead of last name.
    in.byLastName = false;
    in.C_ID = TPCC::NURand(1023, 1, 3000);

    //////////////////////////////////////////////////////
    // Preserve the previous state.
    //////////////////////////////////////////////////////
    Customer oldC(in.C_ID, in.D_ID, W_ID);
    read(&oldC);

    //////////////////////////////////////////////////////
    // Execute transaction.
    //////////////////////////////////////////////////////
    double latency = driver->txOrderStatus(W_ID, &outcome, &in);
    EXPECT_TRUE(outcome);
    EXPECT_TRUE(latency);
}

void TpcCTest::txOrderStatus_byLastName() {
    bool outcome;
    TPCC::InputOrderStatus in;
    uint32_t W_ID = 1;
    in.generate();
    // Overrides the generated input to use lastName instead of C_ID.
    in.byLastName = true;
    genLastName(in.lastName, 0); //BARBARBAR
    in.C_ID = 0;

    Buffer toWrite;
    uint32_t nameToIdSize = 100;
    std::vector<uint32_t> nameToId;
    for (uint32_t i = 1; i <= 100; i++) {
        nameToId.push_back(i);
    }
    toWrite.appendCopy(&nameToIdSize, sizeof(nameToIdSize));
    toWrite.appendExternal(nameToId.data(),
                static_cast<uint32_t>(nameToId.size()) * 4);
    cNameKey nameKey;
    strncpy(nameKey.lastName, "BARBARBAR", 17);
    ramcloud->write(tableId[W_ID], &nameKey,
                    static_cast<uint16_t>(sizeof(nameKey)),
                    toWrite.getRange(0, toWrite.size()),
                    toWrite.size());

    //////////////////////////////////////////////////////
    // Preserve the previous state.
    //////////////////////////////////////////////////////
    uint32_t expectedCID = 50;
    Customer oldC(expectedCID, in.D_ID, W_ID);
    read(&oldC);

    //////////////////////////////////////////////////////
    // Execute transaction.
    //////////////////////////////////////////////////////
    double latency = driver->txOrderStatus(W_ID, &outcome, &in);
    EXPECT_TRUE(outcome);
    EXPECT_TRUE(latency);

    //////////////////////////////////////////////////////
    // Check modifications by the transaction.
    //////////////////////////////////////////////////////
    EXPECT_EQ(expectedCID, in.C_ID);
}

void TpcCTest::txDelivery() {
    bool outcome;
    TPCC::InputDelivery in;
    uint32_t W_ID = 1;
    in.generate();

    for (uint32_t D_ID = 1; D_ID <= 10; D_ID++) {
        //////////////////////////////////////////////////////
        // Preserve the previous state.
        //////////////////////////////////////////////////////
        District d(D_ID, W_ID);
        read(&d);
        uint32_t O_ID = d.data.lowestToDeliverOid;

        NewOrder no(O_ID, D_ID, W_ID);
        read(&no);
        Order o(O_ID, D_ID, W_ID);
        read(&o);
        EXPECT_EQ(0U, o.data.O_CARRIER_ID);

        Customer oldC(o.data.O_C_ID, D_ID, W_ID);
        read(&oldC);

        double sum = 0;
        Tub<OrderLine> ols[15];
        for (uint8_t i = 0; i < o.data.O_OL_CNT; ++i) {
            ols[i].construct(O_ID, D_ID, W_ID, i);
            read(ols[i].get());
            sum += ols[i]->data.OL_AMOUNT;
            EXPECT_EQ(0, ols[i]->data.OL_DELIVERY_D);
        }

        //////////////////////////////////////////////////////
        // Execute transaction.
        //////////////////////////////////////////////////////
        time_t startTime = time(NULL);
        double latency = driver->txDelivery(W_ID, D_ID, &outcome, &in);
        EXPECT_TRUE(outcome);
        EXPECT_TRUE(latency);

        //////////////////////////////////////////////////////
        // Check modifications by the transaction.
        //////////////////////////////////////////////////////
        read(&d);
        EXPECT_EQ(O_ID + 1, d.data.lowestToDeliverOid);
        EXPECT_THROW(read(&no), ObjectDoesntExistException);
        read(&o);
        EXPECT_EQ(in.O_CARRIER_ID, o.data.O_CARRIER_ID);

        for (uint8_t i = 0; i < o.data.O_OL_CNT; ++i) {
            read(ols[i].get());
            EXPECT_LE(startTime, ols[i]->data.OL_DELIVERY_D);
            EXPECT_GE(time(NULL), ols[i]->data.OL_DELIVERY_D);
        }

        Customer c(o.data.O_C_ID, D_ID, W_ID);
        read(&c);
        EXPECT_EQ(oldC.data.C_BALANCE + sum, c.data.C_BALANCE);
        EXPECT_EQ(oldC.data.C_DELIVERY_CNT + 1, c.data.C_DELIVERY_CNT);
    }
}

void TpcCTest::txStockLevel() {
    bool outcome;
    TPCC::InputStockLevel in;
    uint32_t W_ID = 1;
    uint32_t D_ID = 1;
    in.generate();

    //////////////////////////////////////////////////////
    // Preserve the previous state.
    //////////////////////////////////////////////////////

    //////////////////////////////////////////////////////
    // Execute transaction.
    //////////////////////////////////////////////////////
    double latency = driver->txStockLevel(W_ID, D_ID, &outcome, &in);
    EXPECT_TRUE(outcome);
    EXPECT_TRUE(latency);

    //////////////////////////////////////////////////////
    // Check modifications by the transaction.
    //////////////////////////////////////////////////////

    /*
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
        if (s.data.S_QUANTITY < threshold) {
            lowStock.push_back(*it);
        }
    }
    */

}

void TpcCTest::simulate() {
    uint32_t W_ID = 1;
    double cumulativeLatency[5] = {0};
    int txPerformedCount[5] = {0};
    int txAbortCount[5] = {0};

    for (int i = 0; i < 10000; ++i) {
        bool outcome = false;
        int randNum = rand() % 100;
        double latency = 0;
        int txType;
        try {
            if (randNum < 43) {
                latency = driver->txPayment(W_ID, &outcome);
                txType = 0;
            } else if (randNum < 47) {
                latency = driver->txOrderStatus(W_ID, &outcome);
                txType = 1;
            } else if (randNum < 51) {
                for (uint32_t D_ID = 1; D_ID <= 10; D_ID++) {
                    latency += driver->txDelivery(W_ID, D_ID, &outcome);
                }
                txType = 2;
            } else if (randNum < 55) {
                latency = driver->txStockLevel(W_ID, 1U /*fixed D_ID*/, &outcome);
                txType = 3;
            } else {
                latency = driver->txNewOrder(W_ID, &outcome);
                txType = 4;
            }
            if (outcome) {
                cumulativeLatency[txType] += latency;
                txPerformedCount[txType]++;
            } else {
                txAbortCount[txType]++;
            }
        } catch (Exception e) {
            RAMCLOUD_LOG(ERROR, "exception thrown TX job. randNum=%d", randNum);
        }
    }

    int sum = 0;
    for (int i = 0; i < 5; ++i) {
        sum += txPerformedCount[i];
    }

    for (int i = 0; i < 5; ++i) {
        printf("[%d] %7.2f | %5d (%2.2f) | %5d\n",
                i,
                cumulativeLatency[i] / txPerformedCount[i],
                txPerformedCount[i],
                static_cast<double>(100 * txPerformedCount[i]) / sum,
                txAbortCount[i]);
    }
}

void printSize() {
    printf("   Type    key  Data\n");
    printf("Warehouse: %3d  %3d\n", sizeof(TPCC::Warehouse::Key), sizeof(TPCC::Warehouse::Data));
    printf("District : %3d  %3d\n", sizeof(TPCC::District::Key), sizeof(TPCC::District::Data));
    printf("Customer : %3d  %3d\n", sizeof(TPCC::Customer::Key), sizeof(TPCC::Customer::Data));
    printf("History  : %3d  %3d\n", sizeof(TPCC::History::Key), sizeof(TPCC::History::Data));
    printf("NewOrder : %3d  %3d\n", sizeof(TPCC::NewOrder::Key), sizeof(TPCC::NewOrder::Data));
    printf("Order    : %3d  %3d\n", sizeof(TPCC::Order::Key), sizeof(TPCC::Order::Data));
    printf("OrderLine: %3d  %3d\n", sizeof(TPCC::OrderLine::Key), sizeof(TPCC::OrderLine::Data));
    printf("Item     : %3d  %3d\n", sizeof(TPCC::Item::Key), sizeof(TPCC::Item::Data));
    printf("Stock    : %3d  %3d\n", sizeof(TPCC::Stock::Key), sizeof(TPCC::Stock::Data));
    printf("PreparedOp Header     : %3d\n", sizeof(PreparedOp::Header));
    printf("PreparedOpTomb Header : %3d\n", sizeof(PreparedOpTombstone::Header));
    printf("Object Header         : %3d\n", sizeof(Object::Header));
    printf("RpcResult Header      : %3d\n", sizeof(RpcResult::Header));
}

TEST_F(TpcCTest, runAllTests) {
    printSize();
    initBenchmark();
    txNewOrder();
    txPayment_basic();
    txPayment_badCredit();
    txPayment_byLastName();
    txOrderStatus();
    txOrderStatus_byLastName();
    txDelivery();
    txStockLevel();
    //basic();
    simulate();
}

}} // namespace TPCC namespace RAMCloud
