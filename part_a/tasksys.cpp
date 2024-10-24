#include "tasksys.h"
#include "CycleTimer.h"
#include <thread>
#include <mutex>
#include <stdio.h>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

#define print false

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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), num_threads(num_threads), task_ptr(0), completed_tasks(0), runnable(nullptr), num_total_tasks(0) {
    this->thread_pool = new std::thread[num_threads];

    for(int i = 0; i < num_threads; i++) {
        this->thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::spawnWorker, this, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    this->thread_mutex.lock();
    this->task_ptr = -1;
    this->thread_mutex.unlock();

    for(int i = 0; i < this->num_threads; i++) {
        this->thread_pool[i].join();
    }
    
    delete [] thread_pool;
}

void TaskSystemParallelThreadPoolSpinning::spawnWorker(int thread_id) {
    bool shouldRun = false;
    
    while(true) {
        this->thread_mutex.lock();
        
        // If thread ran task on last iteration, mark it as finished now
        if(shouldRun) {
            this->completed_tasks++;
            shouldRun = false;
        }

        // Find the next task, run if in range, abort if -1, nothing otherwise
        int task_to_run = this->task_ptr;
        if(task_to_run == -1) {
            this->thread_mutex.unlock();
            return;
        } else if(this->task_ptr < this->num_total_tasks) {
            shouldRun = true;
            this->task_ptr++;
        }

        this->thread_mutex.unlock();

        if(shouldRun) {
            this->runnable->runTask(task_to_run, this->num_total_tasks);
        }
    } 
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    this->runnable = runnable;
    
    // Reset shared thread state 
    this->thread_mutex.lock();

    this->task_ptr = 0;
    this->completed_tasks = 0;
    this->num_total_tasks = num_total_tasks;

    this->thread_mutex.unlock();

    // Wait for all threads to finish
    while(true) {
        this->thread_mutex.lock();
       	bool shouldAbort = this->completed_tasks == this->num_total_tasks;
        this->thread_mutex.unlock();

        if(shouldAbort) return;
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), num_threads(num_threads), num_total_tasks(0), task_ptr(0), done(false), completed_tasks(0), runnable(nullptr) {
    this->thread_pool = new std::thread[num_threads];

    for(int i = 0; i < num_threads; i++) {
        this->thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::spawnWorker, this, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    this->done = true; 
    this->work_cv.notify_all();

    for(int i = 0; i < this->num_threads; i++) {
        this->thread_pool[i].join();
    }
   
    delete [] thread_pool;
}

void TaskSystemParallelThreadPoolSleeping::spawnWorker(int thread_id) {
    // Wait until notified there is work, loop while claiming
    // Whenever finishes a task, check if == num_total_tasks
    // If so, notify

    while(!this->done) {
        std::unique_lock<std::mutex> lock(this->thread_mutex);
        
        // if(this->task_ptr >= this->num_total_tasks) {
        this->work_cv.wait(lock, [this] { return this->task_ptr < this->num_total_tasks || this->done; });
        // }

        int task_to_run = this->task_ptr;
        this->task_ptr++;
        lock.unlock();
        
        if(task_to_run < this->num_total_tasks) {
            this->runnable->runTask(task_to_run, this->num_total_tasks);
        
            int completed = this->completed_tasks.fetch_add(1);
            if(completed + 1 >= this->num_total_tasks) {
                this->done_cv.notify_one();
            }
        }
    }
  
  
    /*while(true) {
        std::unique_lock<std::mutex> lock(this->thread_mutex);
        bool shouldRun = false;
        int task_to_run = this->task_ptr;
        int total_tasks = this->num_total_tasks;
        // printf("Thread %d claiming task %d\n", thread_id, task_to_run);

        if(task_to_run == -1) {
            // lock.unlock();
            return;
        }

        if(task_to_run < total_tasks) {
            // printf("Thread %d task %d valid, incrementing taskptr\n", thread_id, task_to_run);
            this->task_ptr++;
            shouldRun = true;
        } else {
            // printf("Thread %d sleeping\n", thread_id);
            this->work_cv.wait(lock);
            // printf("Thread %d woke up\n", thread_id);
        }

        if(shouldRun) {
            lock.unlock();
            this->runnable->runTask(task_to_run, total_tasks);
            lock.lock();
            int completed = ++(this->completed_tasks);
            // printf("Thread %d task %d done, completed now %d\n", thread_id, task_to_run, completed);

            if(completed == total_tasks) {
                // printf("Thread %d notifying done\n", thread_id);
                this->done_cv.notify_one();
            }
        }
    }*/

    /*while(true) {
        std::unique_lock<std::mutex> lock(this->thread_mutex);

        int task_to_run = this->task_ptr;
        
        
        if(task_to_run == -1) return;
        
        if(task_to_run < this->num_total_tasks) {
            if(print) printf("Thread %d fetched valid task %d\n", thread_id, task_to_run);
            this->task_ptr++;
            lock.unlock();
            
            this->runnable->runTask(task_to_run, this->num_total_tasks);
        } else {
            if(print) printf("Thread %d fetched invalid task %d, sleeping\n", thread_id, task_to_run);
            this->completed_tasks++;
            if(print) printf("THread %d completed now %d\n", thread_id, this->completed_tasks);
            if(this->completed_tasks == this->num_threads) {
                lock.unlock();
                this->done_cv.notify_all();
                lock.lock();
            }

            this->work_cv.wait(lock);
        }
    }*/
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // Call notify_all
    // Main thread waits until notified that completed_tasks = num_total_tasks 
    // TODO: Probably unnecessary
    
    std::unique_lock<std::mutex> lock(this->thread_mutex);
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    this->task_ptr = 0;
    this->completed_tasks = 0;
    // lock.unlock();
    //printf("About to notify all\n");
    this->work_cv.notify_all();
    
   // lock.lock();
    this->done_cv.wait(lock);
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
