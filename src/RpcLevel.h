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

#ifndef RAMCLOUD_RPCLEVEL_H
#define RAMCLOUD_RPCLEVEL_H

#include "Logger.h"
#include "WireFormat.h"

namespace RAMCloud {

/**
 * This class assigns an integer "level" to each RPC opcode, such that
 * if the implementation of RPC A invokes RPC B, then A's level is higher
 * than that of B. The class helps to prevent distributed deadlocks,
 * which could occur if all available threads in a set of nodes get tied
 * up serving higher level requests, leaving none to service lower
 * level requests.
 *
 * This class provides two functions. The first is to ensure that there
 * exists a level assignment (i.e., there are no circularities in the
 * call graph). It detects circularities at runtime and generates log
 * messages. The second function of this class is to provide information
 * that can be used to assign resources such as threads, so that there
 * are always resources available to service lower levels.
 *
 * This class provides only static methods and variables: it isn't
 * possible to construct an instance.
 */
class RpcLevel {
  public:
    /// Special argument for setCurrentOpcode.
    static const WireFormat::Opcode NO_RPC = WireFormat::Opcode(0);

    // The "level" associated with something that is not currently
    // executing an RPC.
    static const uint8_t NO_LEVEL = 255;

    /**
     * This method should be called before invoking any RPC: it verifies
     * that the RPC being invoked has a level lower than the RPC we're
     * processing (if any). If not, it logs a message so that someone
     * will update the call graph table.
     */
    static inline void
    checkCall(WireFormat::Opcode opcode)
    {
        if (levels[opcode] >= levels[currentOpcode]) {
            // If we get here, it means that the "callee" table in
            // genLevels.py is missing some information about caller-callee
            // relationships, so levels were not properly assigned. The
            // solution is to update the table in genLevels.py.
            RAMCLOUD_LOG(ERROR, "Unexpected RPC from %s to %s creates "
                    "potential for deadlock; must update 'callees' table in "
                    "scripts/genLevels.py",
                    WireFormat::opcodeSymbol(currentOpcode),
                    WireFormat::opcodeSymbol(opcode));
        }
    }

    /**
     * Returns the level associated with a particular opcode, which is the
     * depth of nested RPCs invoked by opcode. 0 means that opcode does not
     * invoke any other RPCs, and NO_LEVEL means that the opcode doesn't
     * have an RPC associated with it.
     */
    static inline int
    getLevel(WireFormat::Opcode opcode)
    {
#if TESTING
        return downCast<int>(levelsPtr[opcode]);
#else
        return downCast<int>(levels[opcode]);
#endif
    }

    static int maxLevel();

    /**
     * This method is invoked to indicate the RPC type currently being
     * served by this thread, so its level can be checked if the thread
     * invokes nested RPCs.
     *
     * \param opcode
     *      Operation that this thread is now serving, or NO_RPC if this
     *      thread is not currently acting on behalf of an incoming RPC
     *      (the NO_RPC value should be used, for example, by WorkerTimer
     *      threads, which may issue RPCs but do not service them).
     */
    static inline void
    setCurrentOpcode(WireFormat::Opcode opcode)
    {
        currentOpcode = opcode;
    }

  PRIVATE:
    RpcLevel();

    // Once maxLevel has been called, it saves its computed result here
    // so it doesn't have to be recomputed again (it won't change). This
    // value can be overridden by unit tests if necessary. < 0 means that
    // maxLevel hasn't been called yet.
    static int savedMaxLevel;

    // Thread-local variable that holds the most recent value passed to
    // setCurrentOpcode.
    static __thread WireFormat::Opcode currentOpcode;

    // Defines the level associated with each RPC opcode.
    static uint8_t levels[];

    // When unit tests are running, this points to the array of levels,
    // so it can be overridden. By default it points to levels.
    static uint8_t* levelsPtr;
};

} // end RAMCloud

#endif  // RAMCLOUD_RPCLEVEL_H
