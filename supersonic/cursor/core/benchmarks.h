// Copyright 2010 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef SUPERSONIC_CURSOR_CORE_BENCHMARKS_H_
#define SUPERSONIC_CURSOR_CORE_BENCHMARKS_H_

#include <memory>
#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/integral_types.h"
#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/timer.h"
#include "supersonic/utils/walltime.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/cursor/base/operation.h"

namespace supersonic {

class BufferAllocator;
class Cursor;

class OperationBenchmarkListener {
 public:
  // Virtual desctuctor.
  virtual ~OperationBenchmarkListener() {}

  // Called when benchmark is started. current_time is in microseconds.
  virtual void OnBenchmarkStarted(uint64 current_time) = 0;

  // Called when Operation has finished creating the cursor.
  virtual void OnCreateFinished(uint64 current_time) = 0;

  // Reports benchmark result. All times are in microseconds:
  virtual void OnBenchmarkFinished(uint64 current_time,
                                   uint64 num_rows_generated,
                                   uint64 processing_time,
                                   uint64 paused_time,
                                   uint64 user_time,
                                   uint64 system_time) = 0;
};

// Wrapper around an Operation measuring its performance. Takes ownership of
// the original operation.
class BenchmarkedOperation : public Operation {
 public:
  BenchmarkedOperation(Operation* operation,
                       OperationBenchmarkListener* benchmark_listener)
      : operation_(operation),
        benchmark_listener_(benchmark_listener) {
    CHECK_NOTNULL(operation);
    CHECK_NOTNULL(benchmark_listener);
  }

  virtual ~BenchmarkedOperation() {}

  // Returns wrapped cursor.
  virtual FailureOrOwned<Cursor> CreateCursor() const;

  virtual void SetBufferAllocator(BufferAllocator* buffer_allocator,
                                  bool cascade_to_children) {
    operation_->SetBufferAllocator(buffer_allocator, cascade_to_children);
  }

  virtual void SetBufferAllocatorWhereUnset(BufferAllocator* buffer_allocator,
                                            bool cascade_to_children) {
    operation_->SetBufferAllocatorWhereUnset(buffer_allocator,
                                             cascade_to_children);
  }

  virtual void AppendDebugDescription(string* const target) const {
    target->append("Benchmarked(");
    operation_->AppendDebugDescription(target);
    target->append(")");
  }

 private:
  // Original operation being benchmarked.
  const std::unique_ptr<Operation> operation_;
  OperationBenchmarkListener* const benchmark_listener_;

  DISALLOW_COPY_AND_ASSIGN(BenchmarkedOperation);
};

template <bool pausing_enabled = true>
class Timer {
 public:
  Timer() : state_(NOT_STARTED) { }

  ~Timer() {
    DCHECK(!IsRunning());
  }

  void Start() {
    DCHECK_EQ(NOT_STARTED, state_);
    state_ = RUNNING;
    start_time_ = GetSystemTime();
    user_timer_.Start();
    system_timer_.Start();
    timer_.Restart();
  }

  void Pause() {
    if (pausing_enabled) {
      DCHECK(IsRunning());
      timer_.Stop();
      state_ = PAUSED;
    }
  }

  void Resume() {
    if (pausing_enabled) {
      DCHECK_EQ(PAUSED, state_);
      timer_.Start();
      state_ = RUNNING;
    }
  }

  void Stop() {
    CHECK(IsRunning());
    timer_.Stop();
    stop_time_ = GetSystemTime();
    user_timer_.Stop();
    system_timer_.Stop();
    state_ = STOPPED;
  }

  int64 start_time() const { return start_time_; }
  int64 stop_time() const { return stop_time_; }

  int64 elapsed_time() const { return stop_time() - start_time(); }
  int64 active_wall_time() const { return timer_.GetInUsec(); }
  int64 paused_time() const { return elapsed_time() - active_wall_time(); }

  int64 active_user_time() const { return user_timer_.Get() * 1E6; }
  int64 active_system_time() const { return system_timer_.Get() * 1E6; }

  int64 GetSystemTime() const {
    return GetCurrentTimeMicros();
  }

 private:
  enum State {
    NOT_STARTED,
    RUNNING,
    PAUSED,
    STOPPED
  };

  bool IsRunning() const { return state_ == RUNNING || state_ == PAUSED; }

  State state_;
  // Times are in microseconds since the Epoch.
  int64 start_time_;
  int64 stop_time_;
  WallTimer timer_;


  // Measure real and system CPU usage.
  UserTimer user_timer_;
  SystemTimer system_timer_;

  DISALLOW_COPY_AND_ASSIGN(Timer);
};

}  // namespace supersonic

#endif  // SUPERSONIC_CURSOR_CORE_BENCHMARKS_H_
