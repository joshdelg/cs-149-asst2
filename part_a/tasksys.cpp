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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads(num_threads), completed_threads(0), task_ptr(0), num_total_tasks(0), bulk_task_id(0), runnable(nullptr) {
    this->thread_pool = new std::thread[num_threads];

    for(int i = 0; i < num_threads; i++) {
        this->thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spawnWorker, this, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    this->state_mutex.lock();
    this->bulk_task_id = -1;
    this->state_mutex.unlock();

    for(int i = 0; i < this->num_threads; i++) {
        this->thread_pool[i].join();
    }
    
    delete [] thread_pool;
}

void TaskSystemParallelThreadPoolSpinning::spawnWorker(int thread_id) {
    while(true) {
        this->state_mutex.lock();
        int task_to_run = this->task_ptr++;
        
        int current_bulk_task = this->bulk_task_id;
        this->state_mutex.unlock();

        if(task_to_run < this->num_total_tasks) {
            this->runnable->runTask(task_to_run, this->num_total_tasks);
        } else {
            this->state_mutex.lock();
            this->completed_threads += 1;
            this->state_mutex.unlock();

            while(true) {
                this->state_mutex.lock();
                int new_bulk_task = this->bulk_task_id;
                this->state_mutex.unlock();

                if(new_bulk_task == -1) {
                    return;
                }
                if(new_bulk_task != current_bulk_task) {
                    break;
                }
            }
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) { 
    this->runnable = runnable; 
    this->completed_threads = 0;
    this->num_total_tasks = num_total_tasks;
    this->task_ptr = 0;


    this->state_mutex.lock(); 
    this->bulk_task_id += 1; 
    this->state_mutex.unlock();


    // Wait for all threads to finish
    while(true) {
        this->state_mutex.lock();
        int completed = this->completed_threads;
        this->state_mutex.unlock();

        if(completed == this->num_threads) break;
    }
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
