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

// Utility class for exhaustive comparison of two cursors:

#ifndef SUPERSONIC_TESTING_COMPARABLE_CURSOR_H_
#define SUPERSONIC_TESTING_COMPARABLE_CURSOR_H_

#include <stddef.h>

#include <iosfwd>
#include <memory>
#include <ostream>

#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/testing/streamable.h"
#include "gtest/gtest.h"

namespace supersonic {

class StatsListener;

class ComparableCursor : public Streamable {
 public:
  // include_rows_in_representation = true
  // Takes ownership of cursor.
  explicit ComparableCursor(Cursor* cursor);
  virtual ~ComparableCursor();

  // Takes ownership of cursor.
  ComparableCursor(Cursor* cursor, bool include_rows_in_representation);

  // Returns a View over next row from the last view returned by wrapped cursor.
  // Calls Next() internally if necessary.
  const ResultView NextRow();

  // Evaluates as bool. Includes detailed explanation if not equal.
  // Note that operator== is not const and consumes both cursors!
  testing::AssertionResult operator==(ComparableCursor& other);  // NOLINT

  // Default error message is enough in case an inequality assertion is not met.
  bool operator!=(ComparableCursor& other) {  // NOLINT
    return !static_cast<bool>(*this == other);
  }

  virtual void AppendToStream(std::ostream* s) const;

 private:
  std::unique_ptr<StatsListener> spy_;
  CursorIterator iterator_;
  size_t row_id_;
  DISALLOW_COPY_AND_ASSIGN(ComparableCursor);
};

std::ostream& operator<<(std::ostream& s, const ResultView& result);

testing::AssertionResult CompareResultViews(
    const ResultView& a, const ResultView& b);

// Predicate formatter for use with EXPECT_PRED_FORMAT2.
testing::AssertionResult CursorsEqual(
    const char* a_str, const char* b_str,
    Cursor* a, Cursor* b);

}  // namespace supersonic

#endif  // SUPERSONIC_TESTING_COMPARABLE_CURSOR_H_
