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
// Utility function facilitating operation benchmarking.

#ifndef SUPERSONIC_BENCHMARK_EXAMPLES_COMMON_UTILS_H_
#define SUPERSONIC_BENCHMARK_EXAMPLES_COMMON_UTILS_H_

#include "supersonic/benchmark/manager/benchmark_manager.h"
#include "supersonic/utils/walltime.h"

namespace supersonic {

class Operation;

// Runs a benchmark on the argument operation using the visualisation options.
// The function will create a cursor and drain it using the maximum block size.
// Will log the result of the operation iff log_result is set to true.
//
// Takes ownership of operation.
void BenchmarkOperation(Operation* operation,
                        const string& benchmark_name,
                        GraphVisualisationOptions options,
                        rowcount_t max_block_size,
                        bool log_result);

// Utility function returning the number of days since epoch from an input date
// string. The string must be in year-month-day format.
int32 EpochDaysFromStringDate(const string& date_string);

}  // namespace supersonic

#endif  // SUPERSONIC_BENCHMARK_EXAMPLES_COMMON_UTILS_H_
