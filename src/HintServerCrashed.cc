/* Copyright (c) 2011 Stanford University
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

#include <iostream>

#include "RamCloud.h"
#include "OptionParser.h"
#include "ServerId.h"

int
main(int argc, char* argv[])
{
    using namespace RAMCloud;

    vector<uint64_t> serverIds;
    OptionsDescription options("HintServerCrashed");
    options.add_options()
        ("down,d",
         ProgramOptions::value<vector<uint64_t>>(&serverIds),
         "Report the specified ServerId(s) as crashed, "
         "can be passed multiple times for multiple reports");

    OptionParser optionParser(options, argc, argv);
    RamCloud client(optionParser.options.getCoordinatorLocator().c_str());
    foreach (const auto& serverId, serverIds) {
        std::cout << "Hinting server crashed: " << *serverId << std::endl;
        client.coordinator.hintServerCrashed(ServerId(serverId));
    }
    return 0;
}
