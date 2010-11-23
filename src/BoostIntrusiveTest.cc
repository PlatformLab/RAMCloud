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

#include "TestUtil.h"
#include "BoostIntrusive.h"

namespace RAMCloud {

class Person {
  public:
    Person() : name(), queueEntries() {}
    string name;
    IntrusiveListHook queueEntries;
  private:
    DISALLOW_COPY_AND_ASSIGN(Person);
};

INTRUSIVE_LIST_TYPEDEF(Person, queueEntries) PersonList;
static PersonList personList;

class BoostIntrusiveTest : public CppUnit::TestFixture {
    CPPUNIT_TEST_SUITE(BoostIntrusiveTest);
    CPPUNIT_TEST(test_list_example);
    CPPUNIT_TEST_SUITE_END();

  public:
    void test_list_example() {
        Person x, y, z;

        personList.push_back(x);
        personList.push_back(y);
        personList.push_back(z);

        CPPUNIT_ASSERT_EQUAL(&x, &personList.front());
        CPPUNIT_ASSERT_EQUAL(&z, &personList.back());

        int count = 0;
        PersonList::iterator iter(personList.begin());
        while (iter != personList.end()) {
            ++count;
            ++iter;
        }
        CPPUNIT_ASSERT_EQUAL(count, personList.size());

        count = 0;
        foreach (Person& person, personList) {
            if (person.name.empty()) {}
            ++count;
        }
        CPPUNIT_ASSERT_EQUAL(count, personList.size());

        // boost has an assertion by default in the destructor for
        // IntrusiveListHook that makes sure it's not currently a part of a
        // list. Without taking x, y, and z off the list before the end of
        // this scope, that assertion will fail.
        personList.clear();
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(BoostIntrusiveTest);

}  // namespace RAMCloud
