#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/TestResult.h>

#include <tests/server.h>

int
main(int ac, char *av[])
{
    using namespace CppUnit;

    TextUi::TestRunner runner;
    TestFactoryRegistry &registry = TestFactoryRegistry::getRegistry();
    runner.addTest(registry.makeTest());
    return !runner.run("", false);
}
