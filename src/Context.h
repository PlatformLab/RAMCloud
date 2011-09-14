/* Copyright (c) 2011 Facebook
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

#ifndef RAMCLOUD_CONTEXT_H
#define RAMCLOUD_CONTEXT_H

#include "Common.h"

namespace RAMCloud {

// forward declarations
class Logger;
class Dispatch;
class MockContextMember;
class TransportManager;
class ServiceManager;

/**
 * Context is a container for global variables.
 *
 * Its main purpose is to allow multiple instances of these variables to
 * coexist in the same address space. This is useful for a variety of reasons,
 * for example:
 *  - Accessing RAMCloud from multiple threads in a multi-threaded client
 *    application without locking.
 *  - Running a simulator of RAMCloud in which multiple clients and servers
 *    share a single process.
 *  - Running unit tests in parallel in the same process.
 *
 * Context also defines an explicit order in which these variables are
 * constructed and destroyed. Without such an ordering, it's easy to run into
 * memory corruption problems (e.g., RAM-212).
 *
 * The current context is stored in thread-local state. Each entry points into
 * the RAMCloud library and the main functions of daemons and threads set
 * current the context using a Context::Guard. The remainder of RAMCloud code
 * uses Context::get() when it needs to access variables in the current
 * context.
 */
class Context {
  PUBLIC:
    class Guard; // forward declaration, see below for definition

    /**
     * Return the current context. This function is supposed to be very fast.
     * If no context has been set, your program will segfault.
     */
    static Context& get() {
#if TESTING
        return friendlyGet();
#else
        return *currentContext;
#endif
    }

    /**
     * Return true if this thread currently has a context set.
     * This can be used in rare cases where a function can operate without a
     * Context. For example, RAMCLOUD_LOG() calls this to fall back to logging
     * to stderr if no context is set.
     */
    static bool isSet() {
        return currentContext != NULL;
    }

    explicit Context(bool hasDedicatedDispatchThread);
    ~Context();

    // Rationale:
    // - These are pointers to the heap to work around circular dependencies in
    //   header files.
    // - They are exposed publicly rather than via accessor methods for
    //   convenience in caller code.
    // - They are not managed by smart pointers (such as std::unique_ptr)
    //   because they need to be constructed and destroyed while inside this
    //   context (since later members may depend on earlier ones). That's
    //   pretty awkward to achieve with smart pointers in the face of
    //   exceptions.

    Logger* logger;
    MockContextMember* mockContextMember1; ///< for testing purposes
    Dispatch* dispatch;
    MockContextMember* mockContextMember2; ///< for testing purposes
    TransportManager* transportManager;
    ServiceManager* serviceManager;

  PRIVATE:
    static Context& friendlyGet();
    static __thread Context* currentContext;
    void destroy();
    DISALLOW_COPY_AND_ASSIGN(Context);
};

/**
 * This class enters into a new context in its constructor and restores the
 * previous context in its destructor. It's used in main functions (for
 * processes and threads) and in entry points to the RAMCloud library.
 */
class Context::Guard {
  PUBLIC:

    /**
     * Enter into the given context.
     * \param newContext
     *      The new context which will be returned by Context::get() following
     *      this call.
     */
    explicit Guard(Context& newContext)
        : needsRestore(true)
        , oldContext(Context::currentContext)
    {
        Context::currentContext = &newContext;
    }

    /**
     * Destructor. If leave() hasn't yet been called, restore the context
     * that was active when the constructor ran (usually unset).
     */
    ~Guard() {
        if (needsRestore)
            Context::currentContext = oldContext;
    }

    /**
     * Restore the context that was active when the constructor ran (usually
     * unset).
     */
    void leave() {
        if (needsRestore) {
            Context::currentContext = oldContext;
            needsRestore = false;
        }
    }

  PRIVATE:

    /**
     * leave() sets this flag to false to indicate to the destructor that it
     * does not need to restore oldContext.
     */
    bool needsRestore;

    /**
     * The context that was active when the constructor was called.
     */
    Context* oldContext;

    DISALLOW_COPY_AND_ASSIGN(Guard);
};


} // end RAMCloud

#endif  // RAMCLOUD_DISPATCH_H
