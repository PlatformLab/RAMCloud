/* Copyright (c) 2009 Stanford University
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

/**
 * \file
 * This file is utilized by scripts/transport-smack to exercise a transport
 * module.
 * The tests to run argument is a semicolon-separated list of \ref
 * ServiceLocatorStrings that do not represent service locators (but share the
 * same format). The test command is specified before the colon in place of a
 * transport protocol.
 *
 * These following are the currently available test commands. All of them take:
 * - Argument: string server,
 *      a service locator string for the server with which to communicate (be
 *      sure to escape this; ignored for do and wait)
 * - Argument: bool async, default 0,
 *      if 1, don't immediately wait for the result of the command
 * - Argument: uint32_t repeat, default 1,
 *      the number of times to repeat a command
 * \section echo
 * Send a random message to a server and have it echo the message back.
 * - Argument: uint32_t size, default 16,
 *      the number of bytes in the payload
 * - Argument: uint32_t spinNs, default 0,
 *      the number of nanoseconds for which the server should spin between
 *      getting the request and sending a response
 * \section echoRange
 * Send a series of random messages of increasing size to a server and have it
 * echo the messages back.
 * - Argument: uint32_t start, default 0,
 *      the number of bytes in the first payload
 * - Argument: uint32_t end, default start,
 *      the number of bytes in the last payload
 * - Argument: uint32_t spinNs, default 0,
 *      the number of nanoseconds for which the server should spin between
 *      getting the request and sending a response
 * \section remote
 * Send a test command to a remote server.
 * - Argument: string command,
 *      the test command to be executed (in this same format; don't forget to
 *      escape it)
 * \section do
 * Execute a list of commands.
 * - Argument: string command,
 *      the test command to be executed (in this same format; don't forget to
 *      escape it)
 * \section wait
 * Wait for the most recent commands to finish.
 *
 * \todo(ongaro): Embed an interpreter rather than having this awful language.
 */

#include <boost/assign/list_of.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "BenchUtil.h"
#include "Client.h"
#include "Common.h"
#include "OptionParser.h"
#include "Rpc.h"
#include "Server.h"
#include "ServiceLocator.h"
#include "TransportManager.h"
#include "rabinpoly.h"

using namespace RAMCloud;

namespace {

/// RPC format used by #Echo.
struct EchoRpc {
    static const RpcType type = RpcType(0xd1);
    struct Request {
        RpcRequestCommon common;
        uint64_t spinNs;
        // variable amount of data follows
    };
    struct Response {
        RpcResponseCommon common;
        // variable amount of data follows
    };
};

/// RPC format used by #Remote.
struct RemoteRpc {
    static const RpcType type = RpcType(0xd2);
    struct Request {
        RpcRequestCommon common;
        uint32_t commandLength;
        // command string follows
    };
    struct Response {
        RpcResponseCommon common;
        uint8_t rc;
    };
};

/// See \ref TransportSmack.cc file header.
// This isn't a ServiceLocator at all, but that code is exactly
// what I want here, e.g., echo: size=1024, spinNs=1000000
typedef ServiceLocator TestDescription;

/// Interface for test commands.
class Test {
  protected:
    Test() {}
  public:
    virtual ~Test() {}
    virtual void start() = 0;
    virtual void wait() = 0;
    void startWait() {
        start();
        wait();
    }
    DISALLOW_COPY_AND_ASSIGN(Test);
};

/// Base class for normal test commands that send an RPC.
class CTest : public Test, public Client {
  protected:
    CTest(const TestDescription& desc, Transport::SessionRef server)
        : desc(desc)
        , server(server)
        , req()
        , resp()
        , asyncState()
    {}
    const TestDescription& desc;
    Transport::SessionRef server;
    Buffer req;
    Buffer resp;
    AsyncState asyncState;
};

typedef boost::shared_ptr<Test> TestRef;

/// Factory for Test instances.
template <typename T>
TestRef
makeTest(const TestDescription& desc, Transport::SessionRef server) {
    return TestRef(new T(desc, server));
}

/// Type of factories for #Test instances.
typedef TestRef (*TestFactory)(const TestDescription&,
                               Transport::SessionRef server);

/// Maps test names such as "echo" to test factories such as makeTest<Echo>.
boost::unordered_map<string, TestFactory> testFactories;

/// Implements echo test command.
struct Echo : public CTest {
  private:
    void init() {
        EchoRpc::Request& reqHdr(allocHeader<EchoRpc>(req));
        reqHdr.spinNs = spinNs;
        fillRandom(new(&req, APPEND) char[size], size);
    }
  public:
    Echo(const TestDescription& desc, Transport::SessionRef server)
        : CTest(desc, server)
        , size(desc.getOption("size", 16U))
        , spinNs(desc.getOption("spin", 0UL))
    {
        init();
    }

    // for EchoRange
    Echo(const TestDescription& desc, Transport::SessionRef server,
         uint32_t size)
        : CTest(desc, server)
        , size(size)
        , spinNs(desc.getOption("spin", 0UL))
    {
        init();
    }

    void start() {
        asyncState = send<RemoteRpc>(server, req, resp);
    }
    void wait() {
        const EchoRpc::Response& respHdr(recv<EchoRpc>(asyncState));
        checkStatus(HERE);
        if (size != resp.getTotalLength() - sizeof(respHdr)) {
            throw FatalError(HERE,
                             format("Server echo returned %lu bytes, "
                                    "but client sent %u bytes",
                                    resp.getTotalLength() - sizeof(respHdr),
                                    size));
        }
        if (Buffer::Iterator(req, sizeof(EchoRpc::Request), ~0U) !=
            Buffer::Iterator(resp, sizeof(EchoRpc::Response), ~0U)) {
            throw FatalError(HERE, "Server echo response differs from "
                                   "client echo request");
        }
        LOG(DEBUG, "Echoed %u bytes", size);
    }
    uint32_t size;
    uint32_t spinNs;
};

/// Implements echoRange test command.
struct EchoRange : public Test {
    EchoRange(const TestDescription& desc, Transport::SessionRef server)
        : desc(desc)
        , server(server)
        , async(desc.getOption("async", false))
        , startSize(desc.getOption("start", 0U))
        , endSize(desc.getOption("end", startSize))
        , echos()
    {
        for (uint32_t size = startSize; size <= endSize; ++size)
            echos.push_back(TestRef(new Echo(desc, server, size)));
    }
    void start() {
        if (async) {
            foreach (TestRef echo, echos)
                echo->start();
        }
    }
    void wait() {
        if (async) {
            foreach (TestRef echo, echos)
                echo->wait();
        } else {
            foreach (TestRef echo, echos) {
                echo->start();
                echo->wait();
            }
        }
    }
    const TestDescription& desc;
    Transport::SessionRef server;
    bool async;
    uint32_t startSize;
    uint32_t endSize;
    vector<TestRef> echos;
};

/// Implements echo test command.
struct Remote : public CTest {
    Remote(const TestDescription& desc, Transport::SessionRef server)
        : CTest(desc, server)
        , command(desc.getOption("command"))
    {
        RemoteRpc::Request& reqHdr(allocHeader<RemoteRpc>(req));
        reqHdr.commandLength = command.length() + 1;
        memcpy(new(&req, APPEND) char[reqHdr.commandLength],
               command.c_str(),
               reqHdr.commandLength);
    }
    void start() {
        asyncState = send<RemoteRpc>(server, req, resp);
    }
    void wait() {
        const RemoteRpc::Response& respHdr(recv<RemoteRpc>(asyncState));
        checkStatus(HERE);
        if (respHdr.rc != 0) {
            throw FatalError(HERE,
                             format("Remote response was %d to command %s",
                                    respHdr.rc, command.c_str()));
        }
        LOG(DEBUG, "Executed %s", command.c_str());
    }
    const string& command; // NOLINT
};

/// Implements do test command.
struct Do : public Test {
    Do(const TestDescription& desc, Transport::SessionRef server)
        : command(desc.getOption("command"))
        , descriptions()
        , asyncTests()
    {
    }
    explicit Do(const char* command)
        : command(command)
        , descriptions()
        , asyncTests()
    {
    }
    void start() {
        TestDescription::parseServiceLocators(command, &descriptions);
        foreach (const TestDescription& desc, descriptions) {
            uint32_t repeat = desc.getOption("repeat", 1U);
            if (desc.getProtocol() == "wait") {
                for (uint32_t i = 0; i < repeat; ++i) {
                    TestRef test = asyncTests.back();
                    asyncTests.pop_back();
                    test->wait();
                }
            } else {
                TestFactory testFactory = get(testFactories,
                                              desc.getProtocol());
                const string& serviceLocator(desc.getOption("server"));
                Transport::SessionRef server(
                    transportManager.getSession(serviceLocator.c_str()));
                bool async = desc.getOption("async", false);
                TestRef tests[repeat];
                for (uint32_t i = 0; i < repeat; ++i)
                    tests[i] = (*testFactory)(desc, server);
                for (uint32_t i = 0; i < repeat; ++i) {
                    tests[i]->start();
                    if (async)
                        asyncTests.push_back(tests[i]);
                    else
                        tests[i]->wait();
                }
            }
        }
    }
    void wait() {
        while (!asyncTests.empty()) {
            TestRef test = asyncTests.back();
            asyncTests.pop_back();
            test->wait();
        }
        LOG(DEBUG, "Executed %s", command.c_str());
    }
    const string command;
    vector<TestDescription> descriptions;
    vector<TestRef> asyncTests;
};

/// RPC server for server side of tests.
class TSServer : public Server {
  public:
    TSServer() {}
    void dispatch(RpcType type,
                  Transport::ServerRpc& rpc,
                  Responder& responder) {
        switch (type) {
            case EchoRpc::type:
                callHandler<EchoRpc, TSServer, &TSServer::echo>(rpc);
                break;
            case RemoteRpc::type:
                callHandler<RemoteRpc, TSServer, &TSServer::remote>(rpc);
                break;
            default:
                throw UnimplementedRequestError(HERE);
        }
    }
    void run() {
        while (true)
            handleRpc<TSServer>();
    }

  private:
    void echo(const EchoRpc::Request& reqHdr,
              EchoRpc::Response& respHdr,
              Transport::ServerRpc& rpc) {
        spin(reqHdr.spinNs);
        Buffer::Iterator iter(rpc.recvPayload, sizeof(reqHdr), ~0U);
        while (!iter.isDone()) {
            Buffer::Chunk::appendToBuffer(&rpc.replyPayload,
                                          iter.getData(),
                                          iter.getLength());
            // TODO(ongaro): This is unsafe if the Transport discards the
            // received buffer before it is done with the response buffer.
            // I don't think transports currently do this.
            iter.next();
        }
    }
    void remote(const RemoteRpc::Request& reqHdr,
              RemoteRpc::Response& respHdr,
              Transport::ServerRpc& rpc) {
        const char* command = getString(rpc.recvPayload, sizeof(reqHdr),
                                        reqHdr.commandLength);
        Do(command).startWait();
    }

    DISALLOW_COPY_AND_ASSIGN(TSServer);
};

} // anonymous namespace

int
main(int argc, char *argv[])
{
    testFactories = boost::assign::map_list_of
            ("remote", &makeTest<Remote>)
            ("echo", &makeTest<Echo>)
            ("echoRange", &makeTest<EchoRange>)
            ("do", &makeTest<Do>);
    try {
        bool isClient = false;
        string testStr("echo:");
        string localLocator;

        { // get config options
            OptionsDescription options("TransportSmack");
            options.add_options()
                ("client",
                 ProgramOptions::bool_switch(&isClient),
                 "Act only as a client")
                ("test,t",
                 ProgramOptions::value<string>(&testStr),
                 "Tests to run");
            OptionParser optionParser(options, argc, argv);
            localLocator = optionParser.options.getLocalLocator();
        }

        if (isClient) {
            LOG(NOTICE, "Running TransportSmack client, %s", testStr.c_str());
            // there's an implicit "do" command around testStr
            Do(testStr.c_str()).startWait();
            LOG(NOTICE, "Done");
        } else {
            LOG(NOTICE,
                "Running TransportSmack server, listening on %s",
                localLocator.c_str());
            transportManager.initialize(localLocator.c_str());
            TSServer().run();
        }

        return 0;
    } catch (RAMCloud::ClientException& e) {
        LOG(ERROR, "TransportSmack: %s", e.str().c_str());
        return 1;
    } catch (RAMCloud::Exception& e) {
        LOG(ERROR, "TransportSmack: %s", e.str().c_str());
        return 1;
    }
}
