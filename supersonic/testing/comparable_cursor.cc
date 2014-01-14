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

#include "supersonic/testing/comparable_cursor.h"

#include <sstream>
#include <string>
namespace supersonic {using std::string; }
#include <vector>
using std::vector;

#include "supersonic/utils/integral_types.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/comparable_tuple_schema.h"
#include "supersonic/testing/comparable_view.h"
#include "supersonic/testing/row.h"
#include "supersonic/cursor/core/spy.h"

namespace supersonic {

class StatsListener : public SpyListener {
 public:
  StatsListener(bool include_rows_in_representation)
      : include_rows_in_representation_(include_rows_in_representation),
        next_call_count_(0),
        row_count_(0) {}

  size_t next_calls() const { return next_call_count_; }
  size_t rows_read() const { return row_count_; }
  const string& last_result() const { return last_result_; }

  virtual void BeforeNext(const string& id, rowcount_t max_row_count) {}

  virtual void AfterNext(const string& id,
                         rowcount_t max_row_count,
                         const ResultView& result,
                         int64 time_cycles) {
    ++next_call_count_;
    if (result.has_data()) {
      if (include_rows_in_representation_) {
        rows_representation_.push_back(StreamableToString(
            ComparableView(result.view(), false, true)));
      }
      row_count_ += result.view().row_count();
    }
    std::stringstream os(std::stringstream::out);
    os << result;
    last_result_ = os.str();
  }

  void AppendRowRepresentationsToStream(std::ostream *s) {
    for (size_t i = 0; i < rows_representation_.size(); i++)
      *s << rows_representation_[i];
  }

 private:
  bool include_rows_in_representation_;
  vector<string> rows_representation_;
  size_t next_call_count_;
  size_t row_count_;
  string last_result_;
  DISALLOW_COPY_AND_ASSIGN(StatsListener);
};

ComparableCursor::ComparableCursor(Cursor* cursor)
    : spy_(new StatsListener(true)),
      iterator_(BoundSpy("", spy_.get(), cursor)),
      row_id_(0) {}


ComparableCursor::ComparableCursor(Cursor* cursor,
                                   bool include_rows_in_representation)
    : spy_(new StatsListener(include_rows_in_representation)),
      iterator_(BoundSpy("", spy_.get(), cursor)),
      row_id_(0) {}

const ResultView ComparableCursor::NextRow() {
  if (!iterator_.Next(1, false)) {
    PROPAGATE_ON_FAILURE(iterator_);
    return ResultView::EOS();
  }
  ++row_id_;
  return ResultView::Success(&iterator_.view());
}

ComparableCursor::~ComparableCursor() {}

void ComparableCursor::AppendToStream(std::ostream *s) const {
  *s << "Cursor"
     << "; rows read: " << spy_->rows_read()
     << (iterator_.is_eos() ? " (all)" : "")
     << "; calls to Next(): " << spy_->next_calls();
  *s << "; schema: " << ComparableTupleSchema(iterator_.schema()) << std::endl;
  spy_->AppendRowRepresentationsToStream(s);
  // Include last result in the representation only if there has been one.
  if (spy_->next_calls() > 0)
    *s << "last result: " << spy_->last_result();
}

std::ostream& operator<<(std::ostream& s, const ResultView& result) {
  if (result.is_failure()) {
    s << "exception " << ReturnCode_Name(result.exception().return_code())
      <<  ": " << result.exception().PrintStackTrace();
  } else if (result.is_eos()) {
    s << "end of stream";
  } else {
    s << "data (" << result.view().row_count() << " rows)";
  }
  return s;
}

testing::AssertionResult ComparableCursor::operator==(
    ComparableCursor& other) {  // NOLINT
  // Compare schemas.
  const testing::AssertionResult schemas_equal =
      ComparableTupleSchema(iterator_.schema()).Compare(
          ComparableTupleSchema(other.iterator_.schema()));
  if (!schemas_equal) {
    return testing::AssertionFailure()
        << "schema mismatch: " << schemas_equal.message();
  }

  // Compare returned data row by row.
  for ( ;; ) {
    const ResultView result = NextRow();
    const ResultView other_result = other.NextRow();
    // Compare result codes (exception & its details / eos / has_data).
    const testing::AssertionResult results_equal =
        CompareResultViews(result, other_result);
    if (!results_equal) {
      return testing::AssertionFailure()
          << "result code mismatch in row #" << row_id_ << ": "
          << results_equal.message();
    }

    if (!result.has_data())
      return testing::AssertionSuccess();

    // result.has_data() && other_result.has_data(). Compare rows.
    const testing::AssertionResult rows_equal = (
        Row(result.view(), 0) == Row(other_result.view(), 0));
    if (!rows_equal) {
      return testing::AssertionFailure()
          << "mismatch in row #" << row_id_ << ": "
          << rows_equal.message();
    }
  }
}

string DescribeResultViewState(const ResultView& rv) {
  return
      rv.is_failure()
        ? ("exception(" + ReturnCode_Name(rv.exception().return_code()) + ")")
        : rv.is_eos()
          ? "eos"
          : "data";
}

testing::AssertionResult CompareResultViews(
    const ResultView& a, const ResultView& b) {
  string a_state = DescribeResultViewState(a);
  string b_state = DescribeResultViewState(b);
  return (a_state == b_state)
      ? testing::AssertionSuccess()
      : (testing::AssertionFailure() << a_state << " vs " << b_state);
}

testing::AssertionResult CursorsEqual(
    const char* a_str, const char* b_str,
    Cursor* a, Cursor* b) {
  VLOG(2) << "CursorsEqual: Trying to check cursors:";
  VLOG(2) << a->schema().GetHumanReadableSpecification();
  VLOG(2) << b->schema().GetHumanReadableSpecification();
  ComparableCursor a_comparable(a);
  ComparableCursor b_comparable(b);
  testing::AssertionResult result = (a_comparable == b_comparable);
  if (!result) {
    result
        << "\nin CursorsEqual(" << a_str << ", " << b_str << ")\n"
        << "\n" << a_str << " = " << a_comparable << "\n"
        << "\n" << b_str << " = " << b_comparable;
  }
  return result;
}

}  // namespace supersonic
