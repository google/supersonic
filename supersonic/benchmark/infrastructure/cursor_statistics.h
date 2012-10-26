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
// This file contains the class which takes care of calculating statistics for
// cursors based on the results provided by benchmark listeners encompassing
// a given cursor.

#ifndef SUPERSONIC_BENCHMARK_INFRASTRUCTURE_CURSOR_STATISTICS_H_
#define SUPERSONIC_BENCHMARK_INFRASTRUCTURE_CURSOR_STATISTICS_H_

#include "supersonic/benchmark/infrastructure/benchmark_transformer.h"
#include "supersonic/benchmark/infrastructure/benchmark_listener.h"
#include "supersonic/benchmark/proto/benchmark.pb.h"
#include "supersonic/utils/macros.h"

namespace supersonic {

// Cursor statistics abstract class describing the functionalities of
// benchmarking trees. Implementations provide statistics tailored for specific
// cursors.
class CursorStatistics {
 public:
  virtual ~CursorStatistics() {}

  // Abstract method for gathering data from listeners. Depending on the
  // statistics type it will use a different logic to aggregate the information
  // measured by benchmark listeners.
  //
  // Typical use case is to call this function after the cursors have completed
  // their computations and the listeners are done with measuring.
  virtual void GatherData() = 0;

  // Cursor benchmark getter. If called before GatherData(), a reference to a
  // partially initialised CursorBenchmarkPack object will be returned. It is
  // okay to do it, if we only want to check for cursor name or type, but
  // referring to actual statistics may lead to unspecified behaviour.
  const BenchmarkData& GetBenchmarkData() const {
    return benchmark_data_;
  }

  // Initialises the CursorStatistics object used to describe the root cursor
  // of the computation by setting the timing_reference_ to this.
  void InitRoot() {
    timing_reference_ = this;
  }

 protected:
  // Constructs the statistics from input and output CursorWithBenchmarkListener
  // objects. Only pointers to benchmark listeners are stored but ownership
  // is not taken.
  //
  // The timing_reference statistics object will be used to calculate relative
  // computation times. Typical use case is to set it to the cursor statistics
  // object for the root cursor, so that the relative times show the percentage
  // of the total computation time spent in a given cursor. Pass a NULL value
  // to disable relative time calculation. Ownership of the timing_reference is
  // not taken.
  //
  // For the root CursorStatistics object use the InitRoot() method to set
  // itself as the timing reference.
  //
  // The cursor_type argument is stored to differentiate between cursors that
  // should, should not be benchmarked, and also unrecognised ones for which
  // special action should be taken.
  CursorStatistics(
      const vector<CursorWithBenchmarkListener*>& input,
      CursorWithBenchmarkListener* output,
      const CursorStatistics* timing_reference,
      BenchmarkData::CursorType cursor_type);

  // Constructs the (leaf) cursor statistics object using the output
  // CursorWithBenchmarkListener. Utility constructor which works just as the
  // previous one, except it handles cases with no cursor input.
  explicit CursorStatistics(
      CursorWithBenchmarkListener* output,
      const CursorStatistics* timing_reference,
      BenchmarkData::CursorType cursor_type);

  // Initialisation method which sets up the timing_reference_
  // and computation_time_ initial values and stores the pointer to the output
  // listener. Also, if output is not a NULL pointer, it will initialise
  // benchmark_data_'s cursor type and name, using the string representation
  // of the cursor's id for the latter.
  void Init(CursorWithBenchmarkListener* output,
            const CursorStatistics* timing_reference,
            BenchmarkData::CursorType cursor_type);

  // Utility calculation functions which compute the total input and output
  // operation times.
  int64 GetTotalInputTime() const;

  int64 GetTotalOutputTime() const;

  // Functions returning total input and output times spent on the first call to
  // Next().
  int64 GetFirstNextInputTime() const;

  int64 GetFirstNextOutputTime() const;

  // Utility functions for calculating the sum and the maximum of the row counts
  // among input sources.
  int64 GetInputRowCountSum() const;

  int64 GetInputRowCountMax() const;

  // Utility function which adds benchmark values for row count, number
  // of calls to the Next() function, relative and absolute processing time
  // and throughput. If sequential is set to true, the processing time will be
  // computed as a simple difference between output and input times. Otherwise
  // the output time will be treated as the processing time.
  //
  // The function also initialises the computation_time_ field with the total
  // output operation time. The value is then used by cursor statistics objects
  // corresponding to descendant nodes to calculate the relative computation
  // time.
  void GatherCommonData(bool sequential);

  // Stored values.

  // Input and output spy listeners. Ownership is not taken.
  vector<BenchmarkListener*> input_listeners_;
  BenchmarkListener* output_listener_;

  // A pack of statistics for the benchmarked cursor.
  BenchmarkData benchmark_data_;

  // The following statistics object is a reference for calculating relative
  // processing time. If NULL relative time will not be computed.
  const CursorStatistics* timing_reference_;

  // Field storing the total time the node spent on processing. It is populated
  // by the GatherData() function and is intended to be used to calculate the
  // relative computation times of the node's descendants.
  int64 computation_time_;

 private:
  DISALLOW_COPY_AND_ASSIGN(CursorStatistics);
};

// Creator functions for cursor statistics. For all input and output
// CursorWithBenchmarkListener objects only pointers to benchmark listeners are
// stored, but ownership is not taken (of either the listener or the
// CursorWithBenchmarkListener entry). The functions correspond to
// the BenchmarkType values defined in benchmark/base/benchmark_types .
//
// The input vector should consist of a list of entries containing pointers
// to the input data cursors and pointers to the benchmark listeners used
// to time them. Similar arguments are passed as output entries. The function
// creating a CursorStatistics object for the Join-type cursors accepts
// left and right hand side of the join separately for convenience.
//
// LeafStats is used for cursors with no children, which take data from
// external, non-cursor sources such as files or views. Statistic computation is
// simple - the values measured by the output listener are stored.
CursorStatistics* LeafStats(CursorWithBenchmarkListener* output,
                            const CursorStatistics* timing_reference);

// PassAllStats is used for cursors with possibly multiple inputs (but always
// at least one) which pass all rows to the output cursor.
CursorStatistics* PassAllStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference);

// PassSomeStats is used for cursors which may drop some input rows -
// characteristics such as the return rate are also calculated for them.
CursorStatistics* PassSomeStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference);

// PreprocessStats describes those cursors where there is a distinct
// preprocessing stage after the first call to Next(). Preprocessing time is
// calculated.
CursorStatistics* PreprocessStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference);

// MayPreprocessStats performs calculations similar to PreprocessStats but it is
// used for cursors where the preprocessing stage may have varying outcomes, for
// instance due to insufficient resources.
CursorStatistics* MayPreprocessStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference);

// JoinStats objects are used specifically for the HashJoin cursor and allow for
// timing index build and row look-up and matching.
CursorStatistics* JoinStats(
    CursorWithBenchmarkListener* lhs_input,
    CursorWithBenchmarkListener* rhs_input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference);

// ParallelStats adopts different computation time semantics which works when
// children cursors are being computed in parallel.
CursorStatistics* ParallelStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference);

// TransparentStats creates a statistics object which does not time the cursor.
// A proper flag is set in the benchmark_pack_ field so that a visualisation
// tool could handle the corresponding node differently.
CursorStatistics* TransparentStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output);

// UnknownStats handles the case of an unrecognised cursor. An UNKNOWN flag will
// be set in the benchmark_pack_ field.
CursorStatistics* UnknownStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output);
}  // namespace supersonic

#endif  // SUPERSONIC_BENCHMARK_INFRASTRUCTURE_CURSOR_STATISTICS_H_
