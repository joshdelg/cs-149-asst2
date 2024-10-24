#include "tasksys.h"


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads), task_ptr(0), done(false), num_threads(num_threads) {
   this->thread_pool = new std::thread[num_threads];

   for(int i = 0; i < num_threads; i++) {
       thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::spawnWorker, this, i);
   }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    this->done = true;

    for(int i = 0; i < this->num_threads; i++) {
        thread_pool[i].join();
    }

    delete [] thread_pool;
    
}

void TaskSystemParallelThreadPoolSleeping::enqueueReadyBulkTasks() {
    // Linear scan through all bulk tasks
    // If the task is not already complete:
    // Check if all their dependencies have now completed
    // If so, enqueue to ready tasks
    // Also, check if all tasks in bulk tasks are done, if so, wake up sync
    
    bool allBulkTasksCompleted = true;
    for(BulkTask& bulk_task : this->bulk_tasks) {
        
        if(bulk_task.completed_tasks != bulk_task.num_total_tasks) {
            allBulkTasksCompleted = false;

            bool allDepsCompleted = true;
            for(TaskID dep : bulk_task.deps) {
                if(this->bulk_tasks[dep].completed_tasks != this->bulk_tasks[dep].num_total_tasks) {
                    allDepsCompleted = false;
                }
            }

            if(allDepsCompleted) {
                this->ready_tasks.push(bulk_task.bulk_id);
            }
        }
    }
    
    if(allBulkTasksCompleted) {
        this->sync_cv.notify_all();
    }
}

void TaskSystemParallelThreadPoolSleeping::spawnWorker(int thread_id) {
    while(!this->done) {
        std::unique_lock<std::mutex> lock(this->thread_mutex);
        
        // First, check if the task is empty and sleep immediately
        if(this->ready_tasks.empty()) {
            this->work_cv.wait(lock);    
        }
        
        TaskID next_bulk_task_id = this->ready_tasks.front();
        BulkTask& next_bulk_task = this->bulk_tasks[next_bulk_task_id];

        int task_to_run = this->task_ptr;
        
        if(task_to_run < next_bulk_task.num_total_tasks) {
            this->task_ptr++;
            // Run the task
            lock.unlock();
            next_bulk_task.runnable->runTask(task_to_run, next_bulk_task.num_total_tasks); 
            lock.lock();

            // Increment completed and check if task done
            next_bulk_task.completed_tasks++;
            if(next_bulk_task.completed_tasks == next_bulk_task.num_total_tasks) {
                // Do logic for checking for other ready bulks
                this->enqueueReadyBulkTasks();
            }
        } else if(task_to_run == next_bulk_task.num_total_tasks) {
            // Check if we're done with tasks on this bulk launch, pop and reset tasks to 0
            this->ready_tasks.pop();
            this->task_ptr = 0;
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> nodeps;
    
    this->runAsyncWithDeps(runnable, num_total_tasks, nodeps);
    this->sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
    // Append bulk task struct to vector
    std::unique_lock<std::mutex> lock(this->thread_mutex);

    TaskID new_task_id = (this->bulk_tasks.size() == 0) ? 0 : this->bulk_tasks.back().bulk_id + 1;
    this->bulk_tasks.emplace_back(new_task_id, num_total_tasks, runnable, deps);

    this->work_cv.notify_all();
    
    return new_task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    // Check if all tasks in queue are done, otherwise, wait
    std::unique_lock<std::mutex> lock(this->thread_mutex);
    
    bool allBulkTasksDone = true;
    for(BulkTask& bulk_task : this->bulk_tasks) {
        if(bulk_task.num_total_tasks != bulk_task.completed_tasks) {
            allBulkTasksDone = false;
            break;
        }
    }

    if(allBulkTasksDone) return;

    this->sync_cv.wait(lock);
}
