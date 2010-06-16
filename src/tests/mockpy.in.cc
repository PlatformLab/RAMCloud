// RAMCloud pragma [CPPLINT=0]
// start file

BEGIN_STUB(stub, base);
    int roflcopter(int a, int b, int c, \
                   char* d);
    void foo(int x);
END_STUB();

BEGIN_MOCK(mock, stub);
    roflcopter(a == 3, b, c, \
               d == NULL) {
        return a + b;
    }
    foo(x) {
    }
    roflcopter(a, b, c, d) {
        return 0;
    }
END_MOCK();

// end file
