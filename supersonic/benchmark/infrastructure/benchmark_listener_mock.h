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
// Mock benchmark listener object for use in testing.

#ifndef SUPERSONIC_BENCHMARK_INFRASTRUCTURE_BENCHMARK_LISTENER_MOCK_H_
#define SUPERSONIC_BENCHMARK_INFRASTRUCTURE_BENCHMARK_LISTENER_MOCK_H_

#include "supersonic/benchmark/infrastructure/benchmark_listener.h"
#include "supersonic/utils/macros.h"

#include "gmock/gmock.h"

namespace supersonic {

// Simple mock for benchmark listener.
class MockBenchmarkListener : public BenchmarkListener {
 public:
  MOCK_METHOD2(BeforeNext, void(const string& id, rowcount_t max_row_count));
  MOCK_METHOD4(AfterNext, void(const string& id,
                               rowcount_t max_row_count,
                               const ResultView& result_view,
                               int64 time_usec));
  MOCK_CONST_METHOD0(NextCalls, int64());
  MOCK_CONST_METHOD0(RowsProcessed, int64());
  MOCK_CONST_METHOD0(TotalTimeUsec, int64());
  MOCK_CONST_METHOD0(FirstNextTimeUsec, int64());
  MOCK_CONST_METHOD0(GetResults, string());
  MOCK_CONST_METHOD1(GetResults, string(const BenchmarkListener& subtract));
};

}  // namespace supersonic

#endif  // SUPERSONIC_BENCHMARK_INFRASTRUCTURE_BENCHMARK_LISTENER_MOCK_H_
