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

// Utility class for exhaustive comparison of two schemas:

#ifndef SUPERSONIC_TESTING_COMPARABLE_TUPLE_SCHEMA_H_
#define SUPERSONIC_TESTING_COMPARABLE_TUPLE_SCHEMA_H_

#include <iosfwd>
#include <ostream>

#include "supersonic/utils/macros.h"
#include "supersonic/testing/streamable.h"
#include "supersonic/cursor/infrastructure/view_printer.h"
#include "gtest/gtest.h"

namespace supersonic {

class TupleSchema;

class ComparableTupleSchema : public Streamable {
 public:
  explicit ComparableTupleSchema(const TupleSchema& schema)
      : schema_(schema),
        view_printer_(true, true) {}

  virtual void AppendToStream(std::ostream* s) const;

  // Evaluates as bool. Includes detailed explanation if not equal.
  // Uses CompareStrict method.
  testing::AssertionResult operator==(const ComparableTupleSchema& other) const;

  // Default error message is enough in case an inequality assertion is not met.
  bool operator!=(const ComparableTupleSchema& other) const {
    return !static_cast<bool>(*this == other);
  }

  // Compares schemas against number of columns and their types.
  testing::AssertionResult Compare(const ComparableTupleSchema& other) const;

  // Additionally to what Compare checks, it compares also column names
  // and nullability of columns.
  testing::AssertionResult CompareStrict(
      const ComparableTupleSchema& other) const;

 protected:
  const TupleSchema& schema_;
  ViewPrinter view_printer_;

  DISALLOW_COPY_AND_ASSIGN(ComparableTupleSchema);
};

// Predicate formatter for use with EXPECT_PRED_FORMAT2.
testing::AssertionResult TupleSchemasEqual(
    const char* a_str, const char* b_str,
    const TupleSchema& a, const TupleSchema& b);

// Predicate formatter for use with EXPECT_PRED_FORMAT2.
testing::AssertionResult TupleSchemasStrictEqual(
    const char* a_str, const char* b_str,
    const TupleSchema& a, const TupleSchema& b);

}  // namespace supersonic


#endif  // SUPERSONIC_TESTING_COMPARABLE_TUPLE_SCHEMA_H_
