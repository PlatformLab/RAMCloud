/* Copyright (c) 2011-2014 Stanford University
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
#ifndef RAMCLOUD_DISPATCHEXEC_H
#define RAMCLOUD_DISPATCHEXEC_H

#include "Common.h"
#include "Dispatch.h"
#include "Fence.h"
#include "Atomic.h"
#include "ShortMacros.h"

namespace RAMCloud {

/**
 * This class allows worker threads to arrange for actions to be executed in
 * the dispatch thread. It is meant to eliminate the need for other threads to
 * lock the dispatch thread in order to access the transports that it owns.
 */
class DispatchExec : public Dispatch::Poller {
    public:
        /**
         * Users of the DispatchExec mechanism should subclass this class and
         * write the work and any necessary cleanup into the invoke() function,
         * because the destructor is never called. 
         *
         * NOTE: Subclasses of this class must be smaller than the return value
         * of Lambda::getMaxSize().
         */
        class Lambda {
            public:
               /**
                * This method contains the code that is to be executed in the
                * dispatch thread. The code is defined by the worker thread
                * using a subclass of Lambda, and DispatchExec::addRequest is
                * invoked to arrange for the code to be executed in the
                * dispatch thread. 
                */
                virtual void invoke() = 0;

                /**
                 * This destructor is never invoked in practice because we use
                 * placement new to perform construction. This declaration only
                 * exists because g++ complains about a non-virtual public
                 * destructor without it.
                 */
                virtual ~Lambda() {}

                /**
                 * This method returns (at compile time) the largest number of
                 * bytes that a subclass of Lambda can contain. 
                 */
                static uint64_t getMaxSize() {
                    // This is a compile time check with no construction
                    // because LambdaBox is unevaluated.
                    return sizeof(LambdaBox) - sizeof(LambdaBox().data.full);
                }
        };

        /**
         * This object holds a Lambda and is cache line sized to minimize the
         * number of cache misses for each invocation.
         */
        union LambdaBox {
            struct {
                // Nonzero value means that this LambdaBox contains a Lambda
                // waiting for execution by the dispatch thread.
                // Zero value means that this LambdaBox is available for a new
                // Lambda.
                //
                // We make this 64 bits to align the Lambda on an 8-byte
                // boundary.
                //
                // We would have preferred to use Atomic, but C++ does not allow
                // the use of Atomic<uint64_t> in unions because Atomic has a
                // constructor. However, based on the load and store operations
                // inside Atomic.h, it appears that we will be safe as long as
                // we do not try to do arithmetic on this value.
                volatile uint64_t full;

                // This is storage space for constructing a Lambda that
                // describes the request.
                // The padding variable below determines the maximum size.
                char lambda[];
            } data;


            // Ensure that the block is a multiple of the cache line size
            char padding[CACHE_LINE_SIZE];

            // Syntactic sugar for callers to get the lambda for invocation out
            // of this LambdaBox.
            Lambda* getLambda() {
                return reinterpret_cast<Lambda*>(&data.lambda);
            }
        };

        explicit DispatchExec(Dispatch* dispatch);
        virtual int poll();

        /**
         * Worker threads invoke this method to schedule work for execution in
         * the dispatch thread. It does placement new of a Lambda that
         * describes the work.
         *
         *  \tparam T
         *       A subclass of Lambda.
         *  \param args
         *       The arguments for the constructor of the subclass of Lambda,
         *       which describes the work to be done in the dispatch thread.
         */
        template<typename T, typename... Args> void addRequest(Args&&... args) {
            static_assert(std::is_base_of<Lambda, T>::value,
                    "T is not a subclass of Lambda");
            std::lock_guard<SpinLock> guard(lock);

            // This check is done at compile-time because all parts of the
            // expression are constant.
            const uint64_t maxSize = Lambda::getMaxSize();
            if (sizeof(T) > maxSize) {
                DIE("The data in the Lambda exceeds %lu bytes, aborting...",
                        maxSize);
            }

            // If there is no space at the current index, it means that the
            // Dispatch thread has fallen behind, so spin until there is space
            // at the current index
            while (requests[addIndex].data.full == 1) {
                RAMCLOUD_CLOG(
                    NOTICE,
                    "Request queue for dispatch thread full, worker blocked..");
            }

            new(&requests[addIndex].data.lambda)
                T(static_cast<Args&&>(args)...);

            // Make sure the object above has fully initialized before marking
            // the LambdaBox as full
            Fence::sfence();
            requests[addIndex].data.full = 1;
            addIndex++;
            if (addIndex == NUM_WORKER_REQUESTS)
                addIndex = 0;

            // It is most likely that the next LambdaBox is already empty, so
            // we should prefetch it now to save time on the next invocation.
            // (It was moved to the Dispatch thread's cache when it was
            // executed).
            prefetch(&requests[addIndex], sizeof(LambdaBox));

        }

    PRIVATE:
        // The maximum number of requests that the dispatch thread can lag
        // behind before blocking worker threads.
        static const uint16_t NUM_WORKER_REQUESTS = 100;

        // This is a circular buffer which contains the requests that are being
        // enqueued by the worker threads.
        LambdaBox* requests;

        // The index within requests of the next Lambda that the dispatch
        // thread will execute.
        uint16_t removeIndex;

        // This pad ensures that all state above here is on a different
        // cache line than all state below, to minimize false sharing.
        // (Only workers access the information below this line.)
        char pad[CACHE_LINE_SIZE];

        // Prevent multiple worker threads from simultaneously adding entries
        // to the same position.
        SpinLock lock;

        // The index in requests at which the worker will place the next Lambda
        // for the dispatch thread to execute.
        uint16_t addIndex;

        DISALLOW_COPY_AND_ASSIGN(DispatchExec);
};

}
#endif
