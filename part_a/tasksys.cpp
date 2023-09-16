#include "tasksys.h"

#include <iostream>

#include "CycleTimer.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() { return "Serial"; }

TaskSystemSerial::TaskSystemSerial(int num_threads)
    : ITaskSystem(num_threads) {}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable,
                                          int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
  return 0;
}

void TaskSystemSerial::sync() { return; }

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
  return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
  //
  // TODO: CS149 students will modify the implementation of this
  // method in Part A.  The implementation provided below runs all
  // tasks sequentially on the calling thread.
  //
  auto task = [this, runnable, num_total_tasks](int thread_id) {
    for (int i = thread_id; i < num_total_tasks; i += num_threads_) {
      runnable->runTask(i, num_total_tasks);
    }
  };

  std::vector<std::thread> thread_pool;
  for (int thread_id = 0; thread_id < num_threads_; ++thread_id) {
    thread_pool.emplace_back(std::thread(task, thread_id));
  }

  for (auto& thread : thread_pool) {
    thread.join();
  }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
  return 0;
}

void TaskSystemParallelSpawn::sync() { return; }

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
  return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads), running_(true) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  auto thread_loop = [this]() {
    while (running_.load(std::memory_order_acquire)) {
      mu_.lock();
      if (task_queue_.empty()) {
        mu_.unlock();
        continue;
      }

      Task task = task_queue_.front();
      task_queue_.pop_front();
      mu_.unlock();

      task.runnable->runTask(task.task_id, task.num_total_tasks);
      if (pending_completions_.fetch_sub(1, std::memory_order_release) == 1) {
        // this is the last remaining task.
        cv_.notify_one();
      }
    }
  };

  for (int _ = 0; _ < num_threads; ++_) {
    thread_pool_.emplace_back(std::thread(thread_loop));
  }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
  running_.store(false, std::memory_order_release);
  for (auto& thread : thread_pool_) {
    thread.join();
  }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable,
                                               int num_total_tasks) {
  std::unique_lock<std::mutex> l(mu_);
  for (int i = 0; i < num_threads_; ++i) {
    for (int task_id = i; task_id < num_total_tasks; task_id += num_threads_) {
      task_queue_.emplace_back(Task{runnable, task_id, num_total_tasks});
    }
  }

  pending_completions_.store(num_total_tasks, std::memory_order_release);
  cv_.wait(l, [this]() {
    return !running_.load(std::memory_order_acquire) ||
           pending_completions_.load(std::memory_order_acquire) == 0;
  });
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() { return; }

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
  return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads), running_(true) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  auto thread_loop = [this](int tid) {
    while (running_.load(std::memory_order_acquire)) {
      {
        std::unique_lock<std::mutex> l(mu_);
        cv_.wait(l, [this, tid]() {
          return !running_.load(std::memory_order_acquire) ||
                 task_queues_[tid].size() > 0;
        });
      }

      if (!running_.load()) {
        return;
      }

      int num_tasks = task_queues_[tid].size();
      while (true) {
        std::unique_lock<std::mutex> l(mu_);
        if (task_queues_[tid].empty()) {
          break;
        }

        Task task = task_queues_[tid].front();
        task_queues_[tid].pop_front();
        l.unlock();

        task.runnable->runTask(task.task_id, task.num_total_tasks);
        pending_completions_.fetch_sub(1, std::memory_order_release);
      }

      // We are now done. attempt to steal work.
      while (true) {
        std::unique_lock<std::mutex> l(mu_);
        bool did_steal = false;
        Task task;

        // Find a task.
        for (int other_tid = 0; other_tid < num_threads_; ++other_tid) {
          if (other_tid == tid || task_queues_[other_tid].empty()) {
            continue;
          }

          did_steal = true;
          task = task_queues_[other_tid].back();
          task_queues_[other_tid].pop_back();
          break;
        }

        if (!did_steal) {
          break;
        }

        l.unlock();
        task.runnable->runTask(task.task_id, task.num_total_tasks);
        pending_completions_.fetch_sub(1, std::memory_order_release);
      }

      if (pending_completions_.load(std::memory_order_acquire) == 0) {
        cv_.notify_all();
      }
    }
  };

  for (int tid = 0; tid < num_threads; ++tid) {
    task_queues_.emplace_back(std::deque<Task>());
    thread_pool_.emplace_back(std::thread(thread_loop, tid));
  }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  //
  // TODO: CS149 student implementations may decide to perform cleanup
  // operations (such as thread pool shutdown construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  running_.store(false, std::memory_order_release);
  cv_.notify_all();
  for (auto& thread : thread_pool_) {
    thread.join();
  }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable,
                                               int num_total_tasks) {
  std::unique_lock<std::mutex> l(mu_);
  for (int tid = 0; tid < num_threads_; ++tid) {
    for (int task_id = tid; task_id < num_total_tasks;
         task_id += num_threads_) {
      task_queues_[tid].emplace_back(Task{runnable, task_id, num_total_tasks});
    }
  }

  pending_completions_.store(num_total_tasks, std::memory_order_release);
  l.unlock();
  cv_.notify_all();

  l.lock();
  cv_.wait(l, [this]() {
    return !running_.load(std::memory_order_acquire) ||
           pending_completions_.load(std::memory_order_acquire) == 0;
  });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
  //
  // TODO: CS149 students will implement this method in Part B.
  //

  return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //

  return;
}
