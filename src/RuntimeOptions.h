/* Copyright (c) 2012 Stanford University
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

#ifndef RAMCLOUD_RUNTIMEOPTIONS_H
#define RAMCLOUD_RUNTIMEOPTIONS_H

#include <mutex>
#include <queue>
#include <unordered_map>
#include <string>
#include "Common.h"

namespace RAMCloud {

/**
 * Contains coordinator configuration options which can be modified while the
 * cluster is running. Allows the coordinator fast access to configuration
 * options without any kind of lookup while allowing clients to set
 * configuration options by a string name. This makes it easy to add
 * configuration options without adding rpc types. Since configuration
 * options may have different types the class employs type-specific parsers
 * to parse the configuration strings provided by clients.
 * All accesses are thread-safe.
 *
 * To add a new runtime option see RuntimeOptions().
 */
class RuntimeOptions {
    PUBLIC:
        RuntimeOptions();
        ~RuntimeOptions();

        void set(const char* option, const char* value);
        std::string get(const char* option);
        uint32_t popFailRecoveryMasters();
        void checkAndCrashCoordinator(const char *crashPoint);

    PRIVATE:
        /**
         * Interface for all configuration option parsers. Generally
         * parsers should subclass this and add a constructor which
         * grabs a reference to a specific field. parse() should have
         * the side effect of populating that field from the given
         * string argument.
         */
        struct Parseable {
            virtual void parse(const char* args) = 0;
            virtual std::string getValue() = 0;
            virtual ~Parseable() {}
        };

        void registerOption(const char* option, Parseable* parser);

        /**
         * Maps a field name to parser which can be used to set that
         * value given a string representation of the value. See
         * #Parseable for more information, or RuntimeOptions() for
         * details on how to add a field and/or parser.
         */
        std::unordered_map<string, Parseable*> parsers;

        typedef std::lock_guard<std::mutex> Lock;
        /// Protects all access to members to make use of fields thread-safe.
        std::mutex mutex;

        // - Options -

        /**
         * Describes how many recovery masters to fail for testing purposes
         * in upcoming recoveries. Each time a recovery is started the
         * head of the queue is popped and that many recovery masters are
         * instructed to kill themselves. For example, performing
         * set("failRecoveryMasters", "2 1") will cause 2 recovery masters
         * to fail on the next subsequent recovery and 1 to fail on the
         * recovery that follows. If the queue is empty then 0 recovery
         * masters are crashed.
         */
        std::queue<uint32_t> failRecoveryMasters;

        /**
         * Keeps track of the currently active crash point. Crashes the
         * coordinator the next time this crash point is reached.
         */
        std::string crashCoordinator;

    DISALLOW_COPY_AND_ASSIGN(RuntimeOptions);
};

} // namespace RAMCloud

#endif // RAMCLOUD_RUNTIMEOPTIONS_H
