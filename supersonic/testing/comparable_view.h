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

// Utility class for exhaustive comparison of two views.

#ifndef SUPERSONIC_TESTING_COMPARABLE_VIEW_H_
#define SUPERSONIC_TESTING_COMPARABLE_VIEW_H_

#include <stddef.h>
#include <iosfwd>
#include <ostream>

#include "supersonic/utils/macros.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/testing/streamable.h"
#include "supersonic/cursor/infrastructure/view_printer.h"
#include "gtest/gtest.h"

namespace supersonic {

// TODO(user): Investigate feasibility of replacing this and related
// classes with free-standing template operators, so that arbitrary Views
// could be compared and printed w/ EXPECT_EQ.
class ComparableView : public Streamable {
 public:
  // include_header_in_representation = true
  // include_rows_in_representation = true
  explicit ComparableView(const View& view);

  ComparableView(const View& view,
                 bool include_header_in_representation,
                 bool include_rows_in_representation);

  virtual void AppendToStream(std::ostream* s) const;

  void AppendRowToStream(size_t row_id, std::ostream* s) const;

  // Evaluates as bool. Includes detailed explanation if not equal.
  testing::AssertionResult operator==(const ComparableView& other) const;

  // Default error message is enough in case an inequality assertion is not met.
  bool operator!=(const ComparableView& other) const {
    return !static_cast<bool>(*this == other);
  }

  const View& view() const { return view_; }

 private:
  View view_;
  ViewPrinter view_printer_;
  DISALLOW_COPY_AND_ASSIGN(ComparableView);
};

// Predicate formatter for use with EXPECT_PRED_FORMAT2.
testing::AssertionResult ViewsEqual(
    const char* a_str, const char* b_str,
    const View& a, const View& b);

// Predicate formatter for use with EXPECT_PRED_FORMAT3.
// Checks that the data not only is the same, but actually is stored in exactly
// the same place (so, it was not copied). Not the same for the is_null column,
// due to short circuit we cannot expect the is_null column to be identical.
testing::AssertionResult ColumnsEqual(
    const char* a_str, const char* b_str, const char* c_str,
    const Column& a, const Column& b, rowcount_t c);

// A check that variable-size types are deep copied.
bool VariableSizeColumnIsACopy(const Column& original,
                               const Column& copy,
                               size_t row_count);

}  // namespace supersonic

#endif  // SUPERSONIC_TESTING_COMPARABLE_VIEW_H_
