#line 1 "src/tests/mockpy.in.cc"
// This file was automatically generated, so don't edit it.
#line 1 "src/tests/mockpy.in.cc"
// RAMCloud pragma [CPPLINT=0]
#line 2 "src/tests/mockpy.in.cc"
// start file
#line 3 "src/tests/mockpy.in.cc"

#line 4 "src/tests/mockpy.in.cc"
// begin generated code from src/tests/mockpy.in.cc
#line 4 "src/tests/mockpy.in.cc"
class stub : public base {
#line 4 "src/tests/mockpy.in.cc"
  public:
#line 4 "src/tests/mockpy.in.cc"
    struct NotImplementedException {};
#line 5 "src/tests/mockpy.in.cc"
    int roflcopter(int a, int b, int c, char* d) __attribute__ ((noreturn)) {
#line 5 "src/tests/mockpy.in.cc"
        throw NotImplementedException();
#line 5 "src/tests/mockpy.in.cc"
    }
#line 7 "src/tests/mockpy.in.cc"
    void foo(int x) __attribute__ ((noreturn)) {
#line 7 "src/tests/mockpy.in.cc"
        throw NotImplementedException();
#line 7 "src/tests/mockpy.in.cc"
    }
#line 8 "src/tests/mockpy.in.cc"
};
#line 8 "src/tests/mockpy.in.cc"
// end generated code
#line 9 "src/tests/mockpy.in.cc"

#line 10 "src/tests/mockpy.in.cc"
// begin generated code from src/tests/mockpy.in.cc
#line 10 "src/tests/mockpy.in.cc"
class mock : public stub {
#line 10 "src/tests/mockpy.in.cc"
  public:
#line 10 "src/tests/mockpy.in.cc"
    mock() : state(0) {
#line 10 "src/tests/mockpy.in.cc"
    }
#line 10 "src/tests/mockpy.in.cc"
    ~mock() {
#line 10 "src/tests/mockpy.in.cc"
        CPPUNIT_ASSERT(state == 3);
#line 10 "src/tests/mockpy.in.cc"
    }
#line 15 "src/tests/mockpy.in.cc"
    void foo(int x) {
#line 15 "src/tests/mockpy.in.cc"
        ++state;
#line 15 "src/tests/mockpy.in.cc"
        switch (state) {
#line 15 "src/tests/mockpy.in.cc"
            case 2: {
#line 15 "src/tests/mockpy.in.cc"
                break;
#line 15 "src/tests/mockpy.in.cc"
            }
#line 15 "src/tests/mockpy.in.cc"
            default:
#line 15 "src/tests/mockpy.in.cc"
                CPPUNIT_ASSERT(false);
#line 15 "src/tests/mockpy.in.cc"
                throw 0;
#line 15 "src/tests/mockpy.in.cc"
        }
#line 15 "src/tests/mockpy.in.cc"
    }
#line 11 "src/tests/mockpy.in.cc"
    int roflcopter(int a, int b, int c, char* d) {
#line 11 "src/tests/mockpy.in.cc"
        ++state;
#line 11 "src/tests/mockpy.in.cc"
        switch (state) {
#line 11 "src/tests/mockpy.in.cc"
            case 1: {
#line 11 "src/tests/mockpy.in.cc"
                CPPUNIT_ASSERT(a == 3);
#line 11 "src/tests/mockpy.in.cc"
                CPPUNIT_ASSERT(d == NULL);
#line 13 "src/tests/mockpy.in.cc"
                return a + b;
#line 11 "src/tests/mockpy.in.cc"
                break;
#line 11 "src/tests/mockpy.in.cc"
            }
#line 17 "src/tests/mockpy.in.cc"
            case 3: {
#line 18 "src/tests/mockpy.in.cc"
                return 0;
#line 17 "src/tests/mockpy.in.cc"
                break;
#line 17 "src/tests/mockpy.in.cc"
            }
#line 11 "src/tests/mockpy.in.cc"
            default:
#line 11 "src/tests/mockpy.in.cc"
                CPPUNIT_ASSERT(false);
#line 11 "src/tests/mockpy.in.cc"
                throw 0;
#line 11 "src/tests/mockpy.in.cc"
        }
#line 11 "src/tests/mockpy.in.cc"
    }
#line 20 "src/tests/mockpy.in.cc"
  private:
#line 20 "src/tests/mockpy.in.cc"
    int state;
#line 20 "src/tests/mockpy.in.cc"
};
#line 20 "src/tests/mockpy.in.cc"
// end generated code
#line 21 "src/tests/mockpy.in.cc"

#line 22 "src/tests/mockpy.in.cc"
// end file
