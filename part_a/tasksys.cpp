#include "tasksys.h"
#include <thread>
#include <stdio.h>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads(num_threads), task_ptr(0) {
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::spawnWorker(IRunnable* runnable, int num_total_tasks, int thread_id) {
    // for(int i = thread_id; i < num_total_tasks; i += this->num_threads + 1) {
    //    runnable->runTask(i, num_total_tasks);
    //}
    int task_to_run = -1;
    while(true) {
        this->task_ptr_mutex.lock();

        if(this->task_ptr >= num_total_tasks) {
            this->task_ptr_mutex.unlock();
            break;
        } else {
            task_to_run = this->task_ptr;
            this->task_ptr++;
            this->task_ptr_mutex.unlock();
        }

        runnable->runTask(task_to_run, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // Spawn threads
    this->task_ptr = 0;
    std::thread workers[this->num_threads];

    for(int i = 0; i < this->num_threads; i++) {
        workers[i] = std::thread(&TaskSystemParallelSpawn::spawnWorker, this, runnable, num_total_tasks, i);
    }

    // Join threads
    for(int i = 0; i < this->num_threads; i++) {
        workers[i].join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads(num_threads), completed_threads(0), task_ptr(0), runnable(nullptr), num_total_tasks(0) {
    this->thread_pool = new std::thread[num_threads];
    this->thread_states = new enum ThreadState[num_threads];

    for(int i = 0; i < num_threads; i++) {
        this->thread_states[i] = WAITING;
        this->thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spawnWorker, this, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    for(int i = 0; i < this->num_threads; i++) {
        this->thread_states[i] = STOPPED;
        this->thread_pool[i].join();
    }
    
    delete [] thread_states;
    delete [] thread_pool;
}

void TaskSystemParallelThreadPoolSpinning::spawnWorker(int thread_id) {
    // All threads start as WAITING
    // When run called, all change to RUNNING
    //
    // If there is work task_ptr < num_total_task, thread claims next one
    // Otherwise, if thread is RUNNING then change to WAITING and increment completed
    // If thread was already WAITING just continue WAITING
 
    while(true) {
        int task_to_run = this->task_ptr.fetch_add(1);
        
        if(task_to_run < this->num_total_tasks) {
            this->runnable->runTask(task_to_run, this->num_total_tasks);
        } else {
            this->task_ptr_mutex.lock();
        
            if(this->thread_states[thread_id] == RUNNING) {
                this->completed_threads++;
                this->thread_states[thread_id] = WAITING;
            }

            this->task_ptr_mutex.unlock();

            while(true) {
                this->task_ptr_mutex.lock();    
                if(this->thread_states[thread_id] == STOPPED) {
                    this->task_ptr_mutex.unlock();
                    return;
                } else if(this->thread_states[thread_id] == RUNNING) {
                    this->task_ptr_mutex.unlock();
                    break;
                }
                this->task_ptr_mutex.unlock();
            }
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // Reset thread running state
    this->task_ptr_mutex.lock();
    this->completed_threads = 0;
    this->task_ptr.store(0);
    this->num_total_tasks = num_total_tasks;
    this->runnable = runnable;

    for(int i = 0; i < this->num_threads; i++) {
        this->thread_states[i] = RUNNING;
    }

    this->task_ptr_mutex.unlock();

    // Wait for all threads to finish
    while(true) {
        this->task_ptr_mutex.lock();
        
        // Reset state if they're done
        if(this->completed_threads == this->num_threads) {
            this->completed_threads = 0;
            this->task_ptr.store(0);
            this->num_total_tasks = 0;

            for(int i = 0; i < this->num_threads; i++) {
                this->thread_states[i] = WAITING;
            }

            break;
        }

        this->task_ptr_mutex.unlock();
    }

    this->task_ptr_mutex.unlock();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
