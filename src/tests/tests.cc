#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/TestResult.h>

#include <tests/server.h>

int
main(int ac, char *av[])
{
    CppUnit::TextUi::TestRunner runner;
    runner.addTest(ServerTest::suite());
    return runner.run("", false) ? 0 : -1;
}
