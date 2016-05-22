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

#ifndef RAMCLOUD_TPCC_H
#define RAMCLOUD_TPCC_H

#include "Common.h"
#include "Buffer.h"
#include "RamCloud.h"

namespace RAMCloud { namespace TPCC {

extern uint64_t tableId[100];

void genLastName(char* target, int rand);
uint32_t random(uint32_t x, uint32_t y);
uint16_t random16(uint32_t x, uint32_t y);
uint8_t random8(uint32_t x, uint32_t y);
uint32_t NURand(uint32_t A, uint32_t x, uint32_t y);

class Row {
  public:
    virtual ~Row() {}
    virtual uint64_t tid() = 0;
    virtual const void* pKey() = 0;
    virtual uint16_t pKeyLength() = 0;
    virtual const void* value() = 0;
    virtual uint32_t valueLength() = 0;
    virtual void parseBuffer(Buffer& buf) = 0;
};

class Warehouse : public Row {
  public:
    // Definitions of primary key fields.
    struct Key {
        uint8_t type;
        uint32_t W_ID;

        Key(uint32_t W_ID) : type(1), W_ID(W_ID) {}
    } __attribute__((__packed__)); //We don't reference individual members.
    Key key;

    // Definitions of fields other than primary key.
    struct Data {
        char W_NAME[11]; //Variable
        char W_STREET_1[21]; //Variable
        char W_STREET_2[21]; //Variable
        char W_CITY[21]; //Variable
        char W_STATE[2];
        char W_ZIP[9];
        float W_TAX;
        double W_YTD;
    };
    Data data;

    /**
     * Constructor only with primary key.
     */
    explicit Warehouse(uint32_t W_ID) : key(W_ID), data() {}

    virtual uint64_t tid() { return tableId[key.W_ID]; }
    virtual const void* pKey() { return &key; }
    virtual uint16_t pKeyLength() { return sizeof(key); }
    virtual const void* value() { return &data; }
    virtual uint32_t valueLength() { return sizeof(data); }
    virtual void parseBuffer(Buffer& buf) { data = *buf.getStart<Data>(); }
};

class District : public Row {
  public:
    // Definitions of primary key fields.
    struct Key {
        uint8_t type;
        uint32_t D_ID;
        uint32_t D_W_ID; //foreign key

        Key(uint32_t D_ID, uint32_t D_W_ID)
        : type(2), D_ID(D_ID), D_W_ID(D_W_ID) {}
    } __attribute__((__packed__)); //We don't reference individual members.
    Key key;

    // Definitions of fields other than primary key.
    struct Data {
        char D_NAME[11]; //Variable
        char D_STREET_1[21]; //Variable
        char D_STREET_2[21]; //Variable
        char D_CITY[21]; //Variable
        char D_STATE[2];
        char D_ZIP[9];
        float D_TAX;
        double D_YTD;
        uint32_t D_NEXT_O_ID;
        uint32_t lowestToDeliverOid; // Hack
    };
    Data data;

    /**
     * Constructor only with primary key.
     */
    explicit District(uint32_t D_ID, uint32_t D_W_ID)
        : key(D_ID, D_W_ID), data() {}

    virtual uint64_t tid() { return tableId[key.D_W_ID]; }
    virtual const void* pKey() { return &key; }
    virtual uint16_t pKeyLength() { return sizeof(key); }
    virtual const void* value() { return &data; }
    virtual uint32_t valueLength() { return sizeof(data); }
    virtual void parseBuffer(Buffer& buf) { data = *buf.getStart<Data>(); }
};

class Customer : public Row {
  public:
    // Definitions of primary key fields.
    struct Key {
        uint8_t type;
        uint32_t C_ID;
        uint32_t C_D_ID;
        uint32_t C_W_ID; //foreign key
        uint8_t feature; //cLastOid table. Included in key for cLastOid.
                          // TODO: rewrite the comment.

        Key(uint32_t C_ID, uint32_t C_D_ID, uint32_t C_W_ID)
        : type(3), C_ID(C_ID), C_D_ID(C_D_ID), C_W_ID(C_W_ID), feature(1) {}
    } __attribute__((__packed__)); //We don't reference individual members.
    Key key;

    // Definitions of fields other than primary key.
    struct Data {
        char C_FIRST[17]; //Variable
        char C_MIDDLE[2];
        char C_LAST[17]; //Variable
        char C_STREET_1[21]; //Variable
        char C_STREET_2[21]; //Variable
        char C_CITY[21]; //Variable
        char C_STATE[2];
        char C_ZIP[9];
        char C_PHONE[16];
        time_t C_SINCE;
        char C_CREDIT[2];
        double C_CREDIT_LIM;
        double C_DISCOUNT;
        double C_BALANCE;
        double C_YTD_PAYMENT;
        uint16_t C_PAYMENT_CNT;
        uint16_t C_DELIVERY_CNT;
        char C_DATA[501]; // variable
    };
    Data data;

    /**
     * Constructor only with primary key.
     */
    explicit Customer(uint32_t C_ID, uint32_t C_D_ID, uint32_t C_W_ID)
        : key(C_ID, C_D_ID, C_W_ID), data() {}

    virtual uint64_t tid() { return tableId[key.C_W_ID]; }
    virtual const void* pKey() { return &key; }
    virtual uint16_t pKeyLength() { return sizeof(key) - 1; }
    virtual uint16_t lastOidKeyLength() { return sizeof(key); }
    virtual const void* value() { return &data; }
    virtual uint32_t valueLength() { return sizeof(data); }
    virtual void parseBuffer(Buffer& buf) { data = *buf.getStart<Data>(); }
};

class History : public Row {
  public:
    // Definitions of primary key fields.
    struct Key {
        uint8_t type;
        uint32_t H_C_ID;
        uint32_t H_C_D_ID;
        uint32_t H_C_W_ID; //foreign key

        Key(uint32_t H_C_ID, uint32_t H_C_D_ID, uint32_t H_C_W_ID)
        : type(4), H_C_ID(H_C_ID), H_C_D_ID(H_C_D_ID), H_C_W_ID(H_C_W_ID) {}
    } __attribute__((__packed__)); //We don't reference individual members.
    Key key;

    // Definitions of fields other than primary key.
    struct Data {
        uint32_t H_D_ID;
        uint32_t H_W_ID;
        time_t H_DATE;
        double H_AMOUNT;
        char H_DATA[25]; // variable
    };
    Data data;

    /**
     * Constructor only with primary key.
     */
    explicit History(uint32_t H_C_ID, uint32_t H_C_D_ID, uint32_t H_C_W_ID)
        : key(H_C_ID, H_C_D_ID, H_C_W_ID), data() {}

    virtual uint64_t tid() { return tableId[key.H_C_W_ID]; }
    virtual const void* pKey() { return &key; }
    virtual uint16_t pKeyLength() { return sizeof(key); }
    virtual const void* value() { return &data; }
    virtual uint32_t valueLength() { return sizeof(data); }
    virtual void parseBuffer(Buffer& buf) { data = *buf.getStart<Data>(); }
};

class NewOrder : public Row {
  public:
    // Definitions of primary key fields.
    struct Key {
        uint8_t type;
        uint32_t NO_O_ID;
        uint32_t NO_D_ID;
        uint32_t NO_W_ID; //foreign key

        Key(uint32_t NO_O_ID, uint32_t NO_D_ID, uint32_t NO_W_ID)
        : type(5), NO_O_ID(NO_O_ID), NO_D_ID(NO_D_ID), NO_W_ID(NO_W_ID) {}
    } __attribute__((__packed__)); //We don't reference individual members.
    Key key;

    // Definitions of fields other than primary key.
    struct Data {
    };
    Data data;

    /**
     * Constructor only with primary key.
     */
    explicit NewOrder(uint32_t NO_O_ID, uint32_t NO_D_ID, uint32_t NO_W_ID)
        : key(NO_O_ID, NO_D_ID, NO_W_ID), data() {}

    virtual uint64_t tid() { return tableId[key.NO_W_ID]; }
    virtual const void* pKey() { return &key; }
    virtual uint16_t pKeyLength() { return sizeof(key); }
    virtual const void* value() { return &data; }
    virtual uint32_t valueLength() { return sizeof(data); }
    virtual void parseBuffer(Buffer& buf) { data = *buf.getStart<Data>(); }
};

class Order : public Row {
  public:
    // Definitions of primary key fields.
    struct Key {
        uint8_t type;
        uint32_t O_ID;
        uint32_t O_D_ID;
        uint32_t O_W_ID; //foreign key

        Key(uint32_t O_ID, uint32_t O_D_ID, uint32_t O_W_ID)
        : type(6), O_ID(O_ID), O_D_ID(O_D_ID), O_W_ID(O_W_ID) {}
    } __attribute__((__packed__)); //We don't reference individual members.
    Key key;

    // Definitions of fields other than primary key.
    struct Data {
        uint32_t O_C_ID;
        time_t O_ENTRY_D;
        uint8_t O_CARRIER_ID;
        uint8_t O_OL_CNT;
        uint8_t O_ALL_LOCAL;
    };
    Data data;

    /**
     * Constructor only with primary key.
     */
    explicit Order(uint32_t O_ID, uint32_t O_D_ID, uint32_t O_W_ID)
        : key(O_ID, O_D_ID, O_W_ID), data() {}

    virtual uint64_t tid() { return tableId[key.O_W_ID]; }
    virtual const void* pKey() { return &key; }
    virtual uint16_t pKeyLength() { return sizeof(key); }
    virtual const void* value() { return &data; }
    virtual uint32_t valueLength() { return sizeof(data); }
    virtual void parseBuffer(Buffer& buf) { data = *buf.getStart<Data>(); }
};

class OrderLine : public Row {
  public:
    // Definitions of primary key fields.
    struct Key {
        uint8_t type;
        uint32_t OL_O_ID;
        uint32_t OL_D_ID;
        uint32_t OL_W_ID; //foreign key
        uint8_t OL_NUMBER;

        Key(uint32_t OL_O_ID, uint32_t OL_D_ID, uint32_t OL_W_ID,
            uint8_t OL_NUMBER)
        : type(7), OL_O_ID(OL_O_ID), OL_D_ID(OL_D_ID), OL_W_ID(OL_W_ID),
          OL_NUMBER(OL_NUMBER) {}
    } __attribute__((__packed__)); //We don't reference individual members.
    Key key;

    // Definitions of fields other than primary key.
    struct Data {
        uint32_t OL_I_ID;
        uint32_t OL_SUPPLY_W_ID;
        time_t OL_DELIVERY_D;
        uint8_t OL_QUANTITY;
        double OL_AMOUNT;
        char OL_DIST_INFO[24];
    };
    Data data;

    /**
     * Constructor only with primary key.
     */
    explicit OrderLine(uint32_t OL_O_ID, uint32_t OL_D_ID, uint32_t OL_W_ID,
                       uint8_t OL_NUMBER)
        : key(OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER), data() {}

    virtual uint64_t tid() { return tableId[key.OL_W_ID]; }
    virtual const void* pKey() { return &key; }
    virtual uint16_t pKeyLength() { return sizeof(key); }
    virtual const void* value() { return &data; }
    virtual uint32_t valueLength() { return sizeof(data); }
    virtual void parseBuffer(Buffer& buf) { data = *buf.getStart<Data>(); }
};

class Item : public Row {
  public:
    // Definitions of primary key fields.
    struct Key {
        uint8_t type;
        uint32_t I_ID;

        Key(uint32_t I_ID) : type(8), I_ID(I_ID) {}
    } __attribute__((__packed__)); //We don't reference individual members.
    Key key;

    // Definitions of fields other than primary key.
    struct Data {
        uint32_t I_IM_ID;
        char I_NAME[25]; // Variable Length.
        double I_PRICE;
        char I_DATA[51]; // Variable length.
    };
    Data data;

    uint32_t W_ID;

    /**
     * Constructor only with primary key.
     */
    explicit Item(uint32_t I_ID, uint32_t W_ID)
        : key(I_ID), data(), W_ID(W_ID) {}

    virtual uint64_t tid() { return tableId[W_ID]; }
    virtual const void* pKey() { return &key; }
    virtual uint16_t pKeyLength() { return sizeof(key); }
    virtual const void* value() { return &data; }
    virtual uint32_t valueLength() { return sizeof(data); }
    virtual void parseBuffer(Buffer& buf) { data = *buf.getStart<Data>(); }
};

class Stock : public Row {
  public:
    // Definitions of primary key fields.
    struct Key {
        uint8_t type;
        uint32_t S_I_ID;
        uint32_t S_W_ID;

        Key(uint32_t S_I_ID, uint32_t S_W_ID)
            : type(9), S_I_ID(S_I_ID), S_W_ID(S_W_ID) {}
    } __attribute__((__packed__)); //We don't reference individual members.
    Key key;

    // Definitions of fields other than primary key.
    struct Data {
        int16_t S_QUANTITY;
        char S_DIST[10][24];
        uint32_t S_YTD;
        uint16_t S_ORDER_CNT;
        uint16_t S_REMOTE_CNT;
        char S_DATA[51]; // Variable length.
    };
    Data data;

    /**
     * Constructor only with primary key.
     */
    explicit Stock(uint32_t S_I_ID, uint32_t S_W_ID)
        : key(S_I_ID, S_W_ID), data() {}

    virtual uint64_t tid() { return tableId[key.S_W_ID]; }
    virtual const void* pKey() { return &key; }
    virtual uint16_t pKeyLength() { return sizeof(key); }
    virtual const void* value() { return &data; }
    virtual uint32_t valueLength() { return sizeof(data); }
    virtual void parseBuffer(Buffer& buf) { data = *buf.getStart<Data>(); }
};

class cNameKey {
  public:
    explicit cNameKey() : typeLastName(11), lastName() {}

    uint32_t typeLastName;
    char lastName[17];
}__attribute__((packed));

struct TpccStat {
    // Commented out since the assignment of default values should be moved to
    // .cc file but it will be harder to read.
    // static const char* const[] txTypeIndexToName =
    //   {"payment", "orderStatus", "delivery", "stockLevel", "newOrder"};
    
    double cumulativeLatency[5];
    int txPerformedCount[5];
    int txAbortCount[5];

    TpccStat() {
        for (int i = 0; i < 5; ++i) {
            cumulativeLatency[i] = 0;
            txAbortCount[i] = 0;
            txPerformedCount[i] = 0;
        }
    }

    TpccStat& operator+=(const TpccStat& rhs) {
        for (int i = 0; i < 5; ++i) {
            cumulativeLatency[i] += rhs.cumulativeLatency[i];
            txAbortCount[i] += rhs.txAbortCount[i];
            txPerformedCount[i] += rhs.txPerformedCount[i];
        }
        return *this;
    }

    friend TpccStat operator+(TpccStat lhs, const TpccStat& rhs) {
        lhs += rhs;
        return lhs;
    }
};

struct InputNewOrder {
    uint32_t D_ID;
    uint32_t C_ID;
    uint8_t O_OL_CNT;
    //bool errorneous = rand() % 100 == 0;
    uint32_t I_IDs[15];
    uint8_t OL_QUANTITY[15];
    uint32_t OL_SUPPLY_W_ID[15];

    void generate(int W_ID, int numWarehouse);
};

struct InputPayment {
    uint32_t D_ID;
    uint32_t C_ID;
    uint32_t C_W_ID;
    uint32_t C_D_ID;
    bool byLastName;
    char lastName[17];
    double H_AMOUNT;

    void generate(int W_ID, int numWarehouse);
};

struct InputOrderStatus {
    uint32_t D_ID; // Same for C_D_ID
    uint32_t C_ID;
    bool byLastName;
    char lastName[17];

    void generate();
};

struct InputDelivery {
    uint8_t O_CARRIER_ID;

    void generate() { O_CARRIER_ID = random8(1, 10); }
};

struct InputStockLevel {
    uint16_t threshold;

    void generate() { threshold = random16(10, 20); }
};

/**
 * TPC-C benchmark driver. This class includes following features:
 * 1. Setup experimental environment.
 * 2. Emulate terminal in TPC-C.
 * 3. Transaction operations.
 */
class Driver {
  public:
    struct TpccContext {
        TpccContext(uint32_t numWarehouses, int serverSpan) :
            initialized(false),
            numWarehouse(numWarehouses),
            serverSpan(serverSpan) {}

        bool initialized;
        uint32_t numWarehouse;
        int serverSpan;
    };

    explicit Driver(RamCloud* ramcloud, uint32_t numWarehouse, int serverSpan);
    Driver(RamCloud* ramcloud, TpccContext& c);

    void initBenchmark();
    void initBenchmark(uint32_t W_ID);
    TpccContext* getContext() { return &context; }

    // returns latency.
    double txNewOrder(uint32_t W_ID, bool* outcome, InputNewOrder* in = NULL);
    double txPayment(uint32_t W_ID, bool* outcome, InputPayment* in = NULL);
    double txOrderStatus(uint32_t W_ID, bool* outcome, InputOrderStatus* in = NULL);
    double txDelivery(uint32_t W_ID, uint32_t D_ID, bool* outcome, InputDelivery *in = NULL);
    double txStockLevel(uint32_t W_ID, uint32_t D_ID, bool* outcome, InputStockLevel *in = NULL);

  PRIVATE:
    void createTables();
    void createTable(uint32_t W_ID);
    void write(uint64_t tid, Row& r);
    void read(Row* row);
    MultiWriteObject* multiWrite(uint64_t tid, Row& r);

    void addItem(uint32_t I_ID, uint32_t W_ID);
    void addWarehouse(uint32_t W_ID);
    void addStock(uint32_t I_ID, uint32_t W_ID);
    void addDistrict(uint32_t D_ID, uint32_t W_ID);
    void addCustomer(uint32_t C_ID, uint32_t D_ID, uint32_t W_ID,
                     std::string* lastName);
    void addHistory(uint32_t C_ID, uint32_t D_ID, uint32_t W_ID);
    void addOrder(uint32_t O_ID, uint32_t D_ID, uint32_t W_ID, uint32_t C_ID);
    void addNewOrder(uint32_t O_ID, uint32_t D_ID, uint32_t W_ID);

    RamCloud* ramcloud;

    TpccContext context;
};

}} // namespace TPCC namespace RAMCloud

#endif // RAMCLOUD_TPCC_H
