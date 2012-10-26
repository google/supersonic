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
// This file contains an enum listing types of computations performed by cursors
// with regard to their benchmarking.

#ifndef SUPERSONIC_BENCHMARK_BASE_BENCHMARK_TYPES_H_
#define SUPERSONIC_BENCHMARK_BASE_BENCHMARK_TYPES_H_

namespace supersonic {

// A BenchmarkType enum describes what kind of computation a given cursor
// represents and how it should be benchmarked.
//
// LEAF - Cursor reads data from an external data source; it does not have a
// child cursor. We will compute the number of rows extracted, the total
// throughput in bytes and rows per second and the total time spent on the
// operation.
//
// PASS_ALL - Cursor reads data from other children cursor(s). The rows are all
// passed together, that is the column schema and values may vary but the count
// remains the same. The same characteristics as for LEAF cursors are
// calculated.
//
// PASS_SOME - Cursor reads data from children cursor(s), but the calculation
// may affect the row count. Besides the values described above, a percentage
// row return rate is calculated. The throughput might also take into account
// the filtering nature of the operation and have slightly different semantics
// than throughput for non-filtering cursors.
//
// PREPROCESS - Some cursors need to perform initial preprocessing on the first
// call to Next() like, for instance, loading the whole data to memory in order
// to be able to calculate aggregation functions or sort the rows. For such
// cursors we are also interested in measuring the time spent on such
// preparation.
//
// MAY_PREPROCESS - Similar benchmarking type to PREPROCESS used when different
// preprocessing outcomes may occur, for instance due to insufficient resources.
// For such cursors we also provide information on the number of rows returned
// compared to the requested row count, which gives insight into what happened
// during preprocessing.
//
// JOIN - Supersonic's hash join pulls data in from two children cursors. One of
// them is used to create a lookup index - we time this creation much like we do
// with PREPROCESS cursors. Once the index has been set up, we turn to measuring
// lookup performance by inspecting the cursor containing the streamed data.
//
// PARALLEL - Parallel cursors use multiple threads to run their children's
// computations. For such cursors the computation time has a different meaning,
// we are also interested in measuring the speed-up gained by using multiple
// cores.
//
// TRANSPARENT - This cursor is just a utility wrapper, it does not make sense
// to time its performance.
//
// UNKNOWN - No information on cursor's computation type.
enum BenchmarkType {
  LEAF,
  PASS_ALL,
  PASS_SOME,
  PREPROCESS,
  MAY_PREPROCESS,
  JOIN,
  PARALLEL,
  TRANSPARENT,
  UNKNOWN
};

}  // namespace supersonic
#endif  // SUPERSONIC_BENCHMARK_BASE_BENCHMARK_TYPES_H_
