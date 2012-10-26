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
// Mock cursor statistics object for use in testing.

#ifndef SUPERSONIC_BENCHMARK_INFRASTRUCTURE_CURSOR_STATISTICS_MOCK_H_
#define SUPERSONIC_BENCHMARK_INFRASTRUCTURE_CURSOR_STATISTICS_MOCK_H_

#include "supersonic/benchmark/infrastructure/cursor_statistics.h"
#include "supersonic/utils/macros.h"

#include "gmock/gmock.h"

namespace supersonic {

// Simple mock for CursorStatistics.
class MockCursorStatistics : public CursorStatistics {
 public:
  // Does not take ownership of the entry.
  explicit MockCursorStatistics(CursorWithBenchmarkListener* entry)
      : CursorStatistics(entry, NULL, BenchmarkData::BENCHMARKED) {}

  MOCK_METHOD0(GatherData, void());
};

}  // namespace supersonic

#endif  // SUPERSONIC_BENCHMARK_INFRASTRUCTURE_CURSOR_STATISTICS_MOCK_H_
