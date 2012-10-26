// Copyright 2012 Google Inc.  All Rights Reserved
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
// Author: tomasz.kaftal@gmail.com (Tomasz Kaftal)
//
// An extension of spy listener for benchmarking use.

#ifndef SUPERSONIC_BENCHMARK_INFRASTRUCTURE_BENCHMARK_LISTENER_H_
#define SUPERSONIC_BENCHMARK_INFRASTRUCTURE_BENCHMARK_LISTENER_H_

#include "supersonic/cursor/core/spy.h"

namespace supersonic {

// Simple spy listener class which introduces methods for obtaining several
// benchmarking values.
class BenchmarkListener : public SpyListener {
 public:
  virtual ~BenchmarkListener() {}

  // Method called before the listened-to cursor's next() call.
  virtual void BeforeNext(const string& id, rowcount_t max_row_count) = 0;

  // Method called after the listened-to cursor's next() call.
  virtual void AfterNext(const string& id,
                         rowcount_t max_row_count,
                         const ResultView& result_view,
                         int64 time_nanos) = 0;

  // Returns the number of calls to the cursor's Next() function.
  virtual int64 NextCalls() const = 0;

  // Returns the number of processed rows.
  virtual int64 RowsProcessed() const = 0;

  // Returns the total time spent processing the data in milliseconds.
  virtual int64 TotalTimeUsec() const = 0;

  // Returns the number of milliseconds spent processing the first call
  // to Next().
  virtual int64 FirstNextTimeUsec() const = 0;

  // Produces a string description of benchmarking results.
  virtual string GetResults() const = 0;

  // This version subtracts results for the 'subnetwork' from the collected
  // results. This way you can measure performance of some part of the whole
  // process. For example you can skip the cost of scan (so disk-reading related
  // cost.
  virtual string GetResults(const BenchmarkListener& subtract) const = 0;
};

BenchmarkListener* CreateBenchmarkListener();

}  // namespace supersonic

#endif  // SUPERSONIC_BENCHMARK_INFRASTRUCTURE_BENCHMARK_LISTENER_H_
