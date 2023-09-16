#include "tasksys.h"

#include <iostream>

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
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

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
    : ITaskSystem(num_threads) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemParallelSpawn::sync() {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(
    int num_threads)
    : ITaskSystem(num_threads) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable,
                                               int num_total_tasks) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
  for (int i = 0; i < num_total_tasks; i++) {
    runnable->runTask(i, num_total_tasks);
  }

  return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
  // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn
  // in Part B.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(
    int num_threads)
    : ITaskSystem(num_threads), next_batch_id_(0), running_(true) {
  //
  // TODO: CS149 student implementations may decide to perform setup
  // operations (such as thread pool construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //

  auto loop = [this](int thread_id) {
    while (running_) {
      std::unique_lock<std::mutex> l(mu_);
      cv_.wait(l, [this]() { return !running_ || !pending_tasks_.empty(); });

      if (!running_) {
        return;
      }

      Task task = pending_tasks_.front();
      pending_tasks_.pop_front();
      l.unlock();

      task.runnable->runTask(task.id, task.num_total_tasks);

      l.lock();
      Batch* batch = pending_batches_[task.batch_id].get();
      batch->num_pending_tasks--;
      if (batch->num_pending_tasks == 0) {
        // This was the last task in the batch.
        for (Batch* dependent : dep_graph_[task.batch_id]) {
          dependent->num_pending_deps--;
          if (dependent->num_pending_deps == 0) {
            // This dependent is ready. We can drain its tasks into
            // `pending_tasks_`.
            for (int i = 0; i < dependent->num_total_tasks; ++i) {
              pending_tasks_.emplace_back(Task{i, dependent->id,
                                               dependent->num_total_tasks,
                                               dependent->runnable});
            }
          }
        }

        cv_.notify_all();

        // Erase necessary keys.
        pending_batches_.erase(pending_batches_.find(task.batch_id));
        dep_graph_.erase(dep_graph_.find(task.batch_id));
      }
    }
  };

  for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
    thread_pool_.emplace_back(std::thread(loop, thread_id));
  }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
  //
  // TODO: CS149 student implementations may decide to perform cleanup
  // operations (such as thread pool shutdown construction) here.
  // Implementations are free to add new class member variables
  // (requiring changes to tasksys.h).
  //
  mu_.lock();
  running_ = false;
  mu_.unlock();

  cv_.notify_all();
  for (std::thread& thread : thread_pool_) {
    thread.join();
  }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable,
                                               int num_total_tasks) {
  runAsyncWithDeps(runnable, num_total_tasks, {});
  sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
  int batch_id = next_batch_id_;
  ++next_batch_id_;

  std::unique_lock<std::mutex> l(mu_);
  std::vector<TaskID> pending_deps;
  for (const TaskID& dep : deps) {
    if (pending_batches_.find(dep) == pending_batches_.end()) {
      continue;
    }

    pending_deps.emplace_back(dep);
  }

  std::unique_ptr<Batch> batch(new Batch);
  batch->id = batch_id;
  batch->num_pending_deps = pending_deps.size();
  batch->num_pending_tasks = num_total_tasks;
  batch->num_total_tasks = num_total_tasks;
  batch->runnable = runnable;

  if (pending_deps.empty()) {
    // Immediately place work into pending queue.
    for (int i = 0; i < num_total_tasks; ++i) {
      pending_tasks_.emplace_back(Task{i, batch_id, num_total_tasks, runnable});
    }

    cv_.notify_all();
  } else {
    // Dependencies still exist. Add current batch to dependency graph.
    for (const TaskID dep : pending_deps) {
      dep_graph_[dep].emplace_back(batch.get());
    }
  }

  pending_batches_[batch_id] = std::move(batch);
  return batch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
  //
  // TODO: CS149 students will modify the implementation of this method in Part
  // B.
  //
  if (pending_tasks_.size() == 0 && pending_batches_.size() == 0) return;

  std::unique_lock<std::mutex> l(mu_);
  cv_.wait(l, [this]() {
    return pending_tasks_.size() == 0 && pending_batches_.size() == 0;
  });

  return;
}
