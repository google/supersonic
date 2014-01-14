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

// Utility class for exhaustive comparison of two rows at artibtrary row ids
// from any View, eg.
//
// Prints out verbose string representation of the two rows.
// EXPECT_EQ(Row(block_->view(), 0), Row(block_->view(), 1));
//
// Prints out detailed reason for inequality.
// EXPECT_TRUE(Row(block_->view(), 3) == Row(view_, 0));
//
// Combines the above messages.
// EXPECT_ROWS_EQUAL(Row(block_->view(), 3), Row(view_, 0));
//
// TODO(user): reconcile with internal/row.h. Perhaps all we need is a
// set of template operators, so that arbitrary ConstRows, per internal/row.h,
// can be compared and printed.

#ifndef SUPERSONIC_TESTING_ROW_H_
#define SUPERSONIC_TESTING_ROW_H_

#include <stddef.h>

#include <iosfwd>
#include <ostream>

#include "supersonic/utils/macros.h"
#include "supersonic/testing/comparable_view.h"
#include "supersonic/testing/streamable.h"
#include "gtest/gtest.h"

namespace supersonic {

class View;

class Row : public Streamable {
 public:
  Row(const View& view, size_t row_id);

  // Evaluates as bool. Includes detailed explanation if not equal.
  testing::AssertionResult operator==(const Row& other) const;

  // Default error message is enough in case an inequality assertion is not met.
  bool operator!=(const Row& other) const {
    return !static_cast<bool>(*this == other);
  }

  virtual void AppendToStream(std::ostream* s) const;

 private:
  // A view over a single row at row_id from view/block.
  ComparableView comparable_view_;

  DISALLOW_COPY_AND_ASSIGN(Row);
};

// Predicate formatter for use with EXPECT_PRED_FORMAT2.
testing::AssertionResult RowsEqual(
    const char* a_str, const char* b_str,
    const Row& a, const Row& b);

}  // namespace supersonic

#endif  // SUPERSONIC_TESTING_ROW_H_
