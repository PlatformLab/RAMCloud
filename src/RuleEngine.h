#ifndef RAMCLOUD_RULEENGINE_H
#define RAMCLOUD_RULEENGINE_H

#include <algorithm>
#include <deque>

#include "Minimal.h"

namespace RAMCloud {

// DQ0: Do we want a rule engine or task queue here?
// ANS: Depends on how thick this abstraction ends up. If it really doesn't do
// much other than calling applyRules on each task. I would say call it TaskQueue.

// DQ1: Should the rule engine be responsible for managing the memory of the tasks?
// ANS: Not clear yet; but sometimes I would like to allocate task memory using Tub
// inside the DCFT module class, then it would be impossible for this class to destroy
// a task correctly because it's not aware of the surrounding Tub.

// DQ2: Are tasks opaque to the rule engine?
// ANS: Probably, in order to make the code general.

// DQ3: How would this class handle state update for the tasks?
// ANS: No, the tasks are opaque to this class. It's the responsibility of DCFT modules.

// DQ4: What happens when an inactive task become active again?
// ANS: See DQ3. This class can't do anything. The DCFT module should (re)schedule that task.

class RuleEngine {
  public:

    /**
     * FIXME: 1. provide modularity for rules & states. 2. what is a goal?
     * 3. active (= scheduled) vs inactive. 4. Opaque to rule engine.
     * 5. updates handled outside rule engine + subclass provides event handler
     * which should be called by the DCFT module. 6. since event handlers update
     * states and the rule engine read/update states => they must be run in the
     * same thread. 7. Task can spawn tasks? 8. Task storage can be handled
     * inside or outside the rule engine? 9. Who is responsible for scheduling,
     * descheduling, and potentially re-scheduling a task? DCFT module, rule
     * engine, and DCFT module again. 10. If a task's storage is managed by the
     * rule engine, then the module should never retain a pointer to this task.
     * As a result, the state of that task can only be updated by its own rules
     * and not the module (e.g., the task has a bunch of outstanding RPCs, it
     * can check their status in its #applyRules).
     */
    class Task {
      public:
        /**
         * Constructor for Task.
         *
         * \param[optional] ruleEngine
         *      The rule engine to schedule the task on. NULL means we will only
         *      construct the task and not schedule it.
         */
        explicit Task(RuleEngine* ruleEngine = NULL)
            : completed(false)
            , ownedByRuleEngine(false)
            , ruleEngine(NULL)
        {
            if (ruleEngine) {
                ruleEngine->schedule(this);
            }
        }

        /**
         * Destructor for Task.
         */
        virtual ~Task()
        {
            if (isScheduled()) {
                // If the task is still scheduled, take it off from the rule
                // engine's task queue.
                TaskQueue* taskQueue = &ruleEngine->taskQueue;
                TaskQueue::iterator it = std::find(taskQueue->begin(),
                        taskQueue->end(), this);
                ruleEngine->taskQueue.erase(it);
                ruleEngine = NULL;
            }
        }

        /**
         * Execute all rules of this task whose conditions are satisfied.
         * It should only be invoked by #RuleEngine::performTasks in the
         * normal case.
         */
        virtual void applyRules() = 0;

        /**
         * Return true if the goal of this task has been achieved.
         */
        bool isCompleted() { return completed; }

        /**
         * Return true if the task is currently scheduled on a rule engine.
         */
        bool isScheduled() { return ruleEngine != NULL; }

      PROTECTED:
        void taskCompleted()
        {
            assert(!completed);
            completed = true;
        };

        /// True if the goal of this task has been achieved.
        bool completed;

        /// True if the storage of this task is allocated by #new and managed
        /// by #ruleEngine, which means the rule engine is responsible for
        /// deleting this task upon descheduling.
        bool ownedByRuleEngine;

        /// The rule engine this task is scheduled on. NULL means the task
        /// is currently unscheduled.
        RuleEngine* ruleEngine;

      PRIVATE:
        /// Required by #RuleEngine::schedule.
        friend class RuleEngine;

        DISALLOW_COPY_AND_ASSIGN(Task)
    };

    // FIXME: these methods are kind of small; shall we put them in the header

    /**
     * Create and schedule a task whose storage is managed by the rule engine.
     *
     * Note: there is no need to provide a variant of this method that creates
     * the task using outside storage because the caller might as well call the
     * constructor of Task with an explicit pointer to this rule engine.
     *
     * \tparam T
     *      Concrete type of the task.
     * \tparam Args
     *      Types of the arguments to pass to T's constructor.
     * \param args
     *      Arguments to pass to T's constructor.
     *
     * \return
     *      A pointer to the task just created.
     */
    template<typename T, typename... Args>
    T*
    createAndSchedule(Args&&... args)
    {
        // TODO: avoid new? use custom allocator?
        T* task = new T(args...);
        task->ownedByRuleEngine = true;
        // If the task has not been scheduled by the constructor, do it here.
        if (!task->isScheduled()) {
            schedule(task);
        }
        return task;
    };

    /**
     * Make progress on scheduled tasks by calling #Task::applyRules on them and
     * deschedule tasks when they are completed.
     */
    void performTasks()
    {
        for (TaskQueue::iterator it = taskQueue.begin();
                it != taskQueue.end();) {
            Task* task = *it;
            task->applyRules();
            if (task->isCompleted()) {
                it = taskQueue.erase(it);
                task->ruleEngine = NULL;
                if (task->ownedByRuleEngine) {
                    delete task;
                }
            } else {
                it++;
            }
        }
    }

    /**
     * Schedule a task so its #applyRules method can be called inside
     * #performTasks.
     *
     * \param task
     *      Task to schedule. The caller must ensure the task is valid before
     *      the it's descheduled from the rule engine.
     */
    void
    schedule(Task* task)
    {
        assert(!task->isScheduled());
        task->ruleEngine = this;
        taskQueue.push_back(task);
    }


    explicit RuleEngine()
        : taskQueue()
    {}

    /// Destructor
    ~RuleEngine()
    {
        // Deschedule all tasks so they can be destructed correctly.
        for (Task* task : taskQueue) {
            task->ruleEngine = NULL;
            if (task->ownedByRuleEngine) {
                delete task;
            }
        }
    }

    /**
     * Return the number of tasks whose goals have not been achieved.
     */
    int
    outstandingTasks()
    {
        return downCast<int>(taskQueue.size());
    }

  PRIVATE:
    // TODO: use boost intrusive list?
    using TaskQueue = std::deque<Task*>;

    /// Holds all scheduled (i.e. active) tasks.
    TaskQueue taskQueue;
};

}

#endif