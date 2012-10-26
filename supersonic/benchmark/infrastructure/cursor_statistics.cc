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
// The file contains the implementations of the CursorStatistics object for
// various types of cursors.

#include "supersonic/benchmark/infrastructure/benchmark_listener.h"
#include "supersonic/benchmark/infrastructure/cursor_statistics.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/proto/cursors.pb.h"

#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/container_literal.h"

namespace supersonic {

namespace {

inline double CalculateRate(
    int64 numerator,
    int64 denominator,
    bool to_percentage) {
  double rate = static_cast<double>(numerator) / denominator;
  rate *= to_percentage ? 100.0 : 1.0;
  return rate;
}

// Cursor statistics' implementations.
//
// ------------------------- LeafStatistics -------------------------
class LeafStatistics : public CursorStatistics {
 public:
  explicit LeafStatistics(CursorWithBenchmarkListener* output,
                          const CursorStatistics* timing_reference)
      : CursorStatistics(output,
                         timing_reference,
                         BenchmarkData::BENCHMARKED) {}

  virtual void GatherData();
};

void LeafStatistics::GatherData() {
  GatherCommonData(true);
}

// ------------------------- PassAllStatistics -------------------------
class PassAllStatistics : public CursorStatistics {
 public:
  PassAllStatistics(
      const vector<CursorWithBenchmarkListener*>& input,
      CursorWithBenchmarkListener* output,
      const CursorStatistics* timing_reference)
      : CursorStatistics(input,
                         output,
                         timing_reference,
                         BenchmarkData::BENCHMARKED) {}

  virtual void GatherData();
};

void PassAllStatistics::GatherData() {
  GatherCommonData(true);
}

// ------------------------- PassSomeStatistics -------------------------
class PassSomeStatistics : public CursorStatistics {
 public:
  PassSomeStatistics(
      const vector<CursorWithBenchmarkListener*>& input,
      CursorWithBenchmarkListener* output,
      const CursorStatistics* timing_reference)
      : CursorStatistics(input,
                         output,
                         timing_reference,
                         BenchmarkData::BENCHMARKED) {}

  virtual void GatherData();
};

void PassSomeStatistics::GatherData() {
  GatherCommonData(true);

  int64 time_difference = GetTotalOutputTime() - GetTotalInputTime();
  int64 rows_out = output_listener_->RowsProcessed();
  int64 max_rows_in  = GetInputRowCountMax();
  int64 total_rows_in  = GetInputRowCountSum();

  // The input to PassSome nodes is assumed to have a "horizontal" shape, that
  // is tuples of input tables are merged together. When calculating the
  // return rate we get a percentage of rows passed up the tree to the maximum
  // row count among the input sources.
  benchmark_data_.set_return_rate(
      CalculateRate(rows_out, max_rows_in, true));

  // Additional throughput-like characteristic which describes the number of
  // rows handled per second. For highly selective cursors this will provide a
  // better description of how fast rows are processed.
  benchmark_data_.set_row_processing_rate(
      CalculateRate(total_rows_in, time_difference, false));
}

// ------------------------- PreprocessStatistics -------------------------
class PreprocessStatistics : public CursorStatistics {
 public:
  PreprocessStatistics(
      const vector<CursorWithBenchmarkListener*>& input,
      CursorWithBenchmarkListener* output,
      const CursorStatistics* timing_reference)
      : CursorStatistics(input,
                         output,
                         timing_reference,
                         BenchmarkData::BENCHMARKED) {}

  virtual void GatherData();
};

void PreprocessStatistics::GatherData() {
  GatherCommonData(true);

  // The first call to Next() on a Preprocess cursor will poll the children
  // for all the data available. The preprocessing time is measured as the
  // difference between the said first Next() call by the output node and the
  // total time spent waiting for the inputs.
  benchmark_data_.set_preprocessing_time(
        GetFirstNextOutputTime() - GetTotalInputTime());
}

// ------------------------- MayPreprocessStatistics -------------------------
class MayPreprocessStatistics : public CursorStatistics {
 public:
  MayPreprocessStatistics(
      const vector<CursorWithBenchmarkListener*>& input,
      CursorWithBenchmarkListener* output,
      const CursorStatistics* timing_reference)
      : CursorStatistics(input,
                         output,
                         timing_reference,
                         BenchmarkData::BENCHMARKED) {}

  virtual void GatherData();
};

void MayPreprocessStatistics::GatherData() {
  GatherCommonData(true);

  // TODO(tkaftal): Implement a call time storing listener and use it to
  // provide more detailed information on preprocessing times. The information
  // will contain the number, rows returned and time spent on some of child's
  // Next() calls which were the result of the first call to Next() in parent.
  // By "some" I mean a sensible value, which will prevent stuffing the
  // listener with a possibly enormous amout of information. The first guess
  // is to store the information about a small number of first and last calls.
  benchmark_data_.set_preprocessing_time(
      GetFirstNextOutputTime() - GetTotalInputTime());
}

// ------------------------- JoinStatistics -------------------------
class JoinStatistics : public CursorStatistics {
 public:
  JoinStatistics(
      const vector<CursorWithBenchmarkListener*>& input,
      CursorWithBenchmarkListener* output,
      const CursorStatistics* timing_reference)
      : CursorStatistics(input,
                         output,
                         timing_reference,
                         BenchmarkData::BENCHMARKED) {}

  // TODO(tkaftal): See if this can be done in a nicer way without blowing up
  // the cursor transformer infrastructure.
  virtual BenchmarkListener* GetLHS() {
    return input_listeners_[0];
  }

  virtual BenchmarkListener* GetRHS() {
    return input_listeners_[1];
  }

  virtual void GatherData();
};

void JoinStatistics::GatherData() {
  GatherCommonData(true);

  int64 total_left_time = GetLHS()->TotalTimeUsec();
  int64 total_right_time = GetRHS()->TotalTimeUsec();

  int64 first_left_time = GetLHS()->FirstNextTimeUsec();

  // The index build build time computed below is not exact - it also takes
  // into account the time spent on matching the first streamed batch with the
  // index. The inaccuracy should be slim to none in most conceivable cases.
  //
  // TODO(tkaftal): Investigate the possibility of subtracting an estimate of
  // the matching time using a properly weighted (on row counts) fraction of
  // the total matching time which is also computed...
  //
  // TODO(tkaftal): ...or, once listeners with history have been implemented,
  // use them to get the exact first call to lhs to be able to exclude the
  // time spent on matching the data it fetched.
  int64 index_build_time =
      GetFirstNextOutputTime() - total_right_time - first_left_time;

  // The matching time describes how long it took to match the streamed input
  // with the index.
  //
  // TODO(tkaftal): As of now the time of matching the first result batch is not
  // counted, which may result in negative times if there is only one call to
  // HashJoin's Next(). A possible fix could be to ensure that HashJoin always
  // returned an empty view on the first call to Next(), since as of now, in the
  // case of a single call matching and index build times are indiscernible.

  int64 matching_time =
      GetTotalOutputTime() - GetFirstNextOutputTime() - total_left_time +
      first_left_time;  // adding - it was deleted twice

  if (matching_time < 0) {
    LOG(WARNING)
        << "Cannot compute matching time as index setup and matching"
           "cannot be distinguished. There have been "
         << benchmark_data_.next_calls() << " call(s) to Next().";
    matching_time = -1;
  }

  benchmark_data_.set_index_set_up_time(index_build_time);
  benchmark_data_.set_matching_time(matching_time);
}

// ------------------------- ParallelStatistics  -------------------------
class ParallelStatistics : public CursorStatistics {
 public:
  ParallelStatistics(
      const vector<CursorWithBenchmarkListener*>& input,
      CursorWithBenchmarkListener* output,
      const CursorStatistics* timing_reference)
      : CursorStatistics(input,
                         output,
                         timing_reference,
                         BenchmarkData::BENCHMARKED) {}

  virtual void GatherData();
};

void ParallelStatistics::GatherData() {
  GatherCommonData(false);

  double speedup = static_cast<double>(GetTotalInputTime())
      / GetTotalOutputTime();

  benchmark_data_.set_speed_up(speedup);
}

// ------------------------- NoStatistics  -------------------------
class NoStatistics : public CursorStatistics {
 public:
  NoStatistics(
      const vector<CursorWithBenchmarkListener*>& input,
      CursorWithBenchmarkListener* output,
      BenchmarkData::CursorType cursor_type)
    : CursorStatistics(input, output, NULL, cursor_type) {}

  virtual void GatherData() {}
};

}  // namespace

CursorStatistics::CursorStatistics(
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference,
    BenchmarkData::CursorType cursor_type) {
  Init(output, timing_reference, cursor_type);
}

typedef vector<CursorWithBenchmarkListener*>::const_iterator
    const_entry_iterator;

CursorStatistics::CursorStatistics(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference,
    BenchmarkData::CursorType cursor_type) {
  Init(output, timing_reference, cursor_type);
  for (const_entry_iterator entry = input.begin(); entry != input.end();
       ++entry) {
    input_listeners_.push_back((*entry)->listener());
  }
}

void CursorStatistics::Init(
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference,
    BenchmarkData::CursorType cursor_type) {
  output_listener_ = output->listener();
  timing_reference_ = timing_reference;
  computation_time_ = 0;

  if (output->cursor() != NULL) {
    benchmark_data_.set_cursor_name(
        CursorId_Name(output->cursor()->GetCursorId()));
    benchmark_data_.set_cursor_type(cursor_type);
  }
}

void CursorStatistics::GatherCommonData(bool sequential) {
  // Set up total computation time.
  computation_time_ = GetTotalOutputTime();
  benchmark_data_.set_total_subtree_time(computation_time_);

  BenchmarkListener* above = output_listener_;
  int64 rows_processed = above->RowsProcessed();

  int64 processing_time = sequential
                          ? GetTotalOutputTime() - GetTotalInputTime()
                          : GetTotalOutputTime();

  benchmark_data_.set_processing_time(processing_time);
  if (timing_reference_ != NULL) {
    benchmark_data_.set_relative_time(
        CalculateRate(processing_time,
                      timing_reference_->computation_time_,
                      true));
  }
  // TODO(tkaftal): add throughput in bytes calculation.
  benchmark_data_.set_throughput(
      CalculateRate(rows_processed, processing_time, false));

  benchmark_data_.set_rows_processed(rows_processed);
  benchmark_data_.set_next_calls(above->NextCalls());
}

int64 CursorStatistics::GetTotalInputTime() const {
  int64 time_sum = 0;
  for (size_t i = 0; i < input_listeners_.size(); ++i) {
    time_sum += input_listeners_[i]->TotalTimeUsec();
  }
  return time_sum;
}

int64 CursorStatistics::GetTotalOutputTime() const {
  return output_listener_->TotalTimeUsec();
}

int64 CursorStatistics::GetFirstNextInputTime() const {
  int64 time_sum = 0;
  for (size_t i = 0; i < input_listeners_.size(); ++i) {
    time_sum += input_listeners_[i]->FirstNextTimeUsec();
  }
  return time_sum;
}

int64 CursorStatistics::GetFirstNextOutputTime() const {
  return output_listener_->FirstNextTimeUsec();
}

int64 CursorStatistics::GetInputRowCountSum() const {
  int64 total_rows_in = 0;
  for (size_t i = 0; i < input_listeners_.size(); ++i) {
    total_rows_in += input_listeners_[i]->RowsProcessed();
  }
  return total_rows_in;
}

int64 CursorStatistics::GetInputRowCountMax() const {
  int64 rows_max = 0;
  for (size_t i = 0; i < input_listeners_.size(); ++i) {
    rows_max = std::max(rows_max, input_listeners_[i]->RowsProcessed());
  }
  return rows_max;
}

// Implementations of creator functions.
CursorStatistics* LeafStats(CursorWithBenchmarkListener* output,
                            const CursorStatistics* timing_reference) {
  return new LeafStatistics(output, timing_reference);
}

CursorStatistics* PassAllStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference) {
  CHECK(!input.empty());
  return new PassAllStatistics(input, output, timing_reference);
}

CursorStatistics* PassSomeStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference) {
  CHECK(!input.empty());
  return new PassSomeStatistics(input, output, timing_reference);
}

CursorStatistics* PreprocessStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference) {
  CHECK(!input.empty());
  return new PreprocessStatistics(input, output, timing_reference);
}

CursorStatistics* MayPreprocessStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference) {
  CHECK(!input.empty());
  return new MayPreprocessStatistics(input, output, timing_reference);
}

CursorStatistics* JoinStats(
    CursorWithBenchmarkListener* lhs_input,
    CursorWithBenchmarkListener* rhs_input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference) {
  return new JoinStatistics(
      util::gtl::Container(lhs_input, rhs_input),
      output,
      timing_reference);
}

CursorStatistics* ParallelStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output,
    const CursorStatistics* timing_reference) {
  CHECK(!input.empty());
  return new ParallelStatistics(input, output, timing_reference);
}

CursorStatistics* TransparentStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output) {
  return new NoStatistics(input,
                          output,
                          BenchmarkData::NOT_BENCHMARKED);
}

CursorStatistics* UnknownStats(
    const vector<CursorWithBenchmarkListener*>& input,
    CursorWithBenchmarkListener* output) {
  return new NoStatistics(input,
                          output,
                          BenchmarkData::UNRECOGNISED);
}

}  // namespace supersonic
