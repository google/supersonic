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

#include "supersonic/testing/comparable_view.h"

#include <math.h>

#include <vector>
using std::vector;

#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/types_infrastructure.h"
#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/testing/comparable_tuple_schema.h"
#include "supersonic/utils/strings/stringpiece.h"

namespace supersonic {

ComparableView::ComparableView(const View& view)
    : view_(view),
      view_printer_() {}

ComparableView::ComparableView(
    const View& view,
    bool include_header_in_representation,
    bool include_rows_in_representation)
    : view_(view),
      view_printer_(include_header_in_representation,
                    include_rows_in_representation) {}

void ComparableView::AppendToStream(std::ostream *s) const {
  view_printer_.AppendViewToStream(view_, s);
}

void ComparableView::AppendRowToStream(size_t row_id, std::ostream* s) const {
  view_printer_.AppendRowToStream(view_, row_id, s);
}

testing::AssertionResult ComparableView::operator==(
    const ComparableView& other) const {
  // Compare schemas.
  const testing::AssertionResult schemas_equal =
      ComparableTupleSchema(view_.schema())
          .Compare(ComparableTupleSchema(other.view_.schema()));
  if (!schemas_equal) {
    return testing::AssertionFailure()
        << "schema mismatch: " << schemas_equal.message();
  }

  // Compare row counts.
  if (view_.row_count() != other.view_.row_count()) {
    return testing::AssertionFailure()
        << "row count mismatch: "
        << view_.row_count() << " vs " << other.view_.row_count();
  }

  // Pairwise compare rows.
  vector<EqualityComparator> column_comparators;
  for (size_t c = 0; c < view_.schema().attribute_count(); c++) {
    column_comparators.push_back(
        GetEqualsComparator(
            view_.column(c).type_info().type(),
            other.view_.column(c).type_info().type(),
            false, false));
  }

  for (size_t row_id = 0; row_id < view_.row_count(); row_id++) {
    for (size_t c = 0; c < view_.schema().attribute_count(); c++) {
      const Column& my_column = view_.column(c);
      const Column& other_column = other.view_.column(c);
      VariantConstPointer value = my_column.is_null() != NULL &&
                                  my_column.is_null()[row_id]
          ? NULL
          : my_column.data_plus_offset(row_id);
      VariantConstPointer other_value = other_column.is_null() != NULL &&
                                        other_column.is_null()[row_id]
          ? NULL
          : other_column.data_plus_offset(row_id);
      if (!column_comparators[c](value, other_value)) {
        // We want NaN values for doubles to compare to true in testing
        // (while we still want the standard NaN =/= NaN in production code).
        if (my_column.type_info().type() == DOUBLE &&
            isnan(*value.as<DOUBLE>()) && isnan(*value.as<DOUBLE>()))
          continue;
        if (my_column.type_info().type() == FLOAT &&
            isnan(*value.as<FLOAT>()) && isnan(*value.as<FLOAT>()))
          continue;
        return testing::AssertionFailure()
            << "value mismatch in row " << row_id << ", column " << c;
      }
    }
  }

  return testing::AssertionSuccess();
}

testing::AssertionResult ViewsEqual(
    const char* a_str, const char* b_str,
    const View& a, const View& b) {
  ComparableView a_comparable(a);
  ComparableView b_comparable(b);
  testing::AssertionResult result = (a_comparable == b_comparable);
  if (!result) {
    result
        << "\nin ViewsEqual(" << a_str << ", " << b_str << ")\n"
        << "\n" << a_str << " = " << a_comparable
        << "\n" << b_str << " = " << b_comparable;
  }
  return result;
}

testing::AssertionResult RawDataEqual(const void* a, const void* b) {
  if (a != b) {
    return testing::AssertionFailure()
        << "The data is in different places in the two columns: "
        << a << " vs. " << b;
  }
  return testing::AssertionSuccess();
}

testing::AssertionResult DecorateForColumnsEqual(
    testing::AssertionResult result,
    const ComparableView& a, const ComparableView& b, rowcount_t c,
    const char* a_str, const char* b_str, const char* c_str) {
  result
      << "\nin ColumnsEqual(" << a_str << ", " << b_str << ", "
      << c_str << ")\n"
      << "\n" << a_str << " = " << a
      << "\n" << b_str << " = " << b
      << "\n" << c_str << " = " << c;
  return result;
}

testing::AssertionResult ColumnsEqual(
    const char* a_str, const char* b_str, const char* c_str,
    const Column& a, const Column& b, rowcount_t c) {
  ComparableView a_comparable(View(a, c));
  ComparableView b_comparable(View(b, c));
  testing::AssertionResult result = (a_comparable == b_comparable);
  if (!result)
    return DecorateForColumnsEqual(result, a_comparable, b_comparable, c,
                                   a_str, b_str, c_str);
  testing::AssertionResult data_result
      = RawDataEqual(a.data().raw(), b.data().raw());
  if (!data_result)
    return DecorateForColumnsEqual(data_result, a_comparable, b_comparable, c,
                                   a_str, b_str, c_str);
  return testing::AssertionSuccess();
}

bool VariableSizeColumnIsACopy(const Column& original,
                               const Column& copy,
                               size_t row_count) {
  const StringPiece* original_sp = original.typed_data<STRING>();
  const StringPiece* copy_sp = copy.typed_data<STRING>();
  for (size_t i = 0; i < row_count; i++) {
    const bool is_copy = (*original_sp == *copy_sp) &&
                         (original_sp->data() != copy_sp->data());
    if (!is_copy)
      return false;
  }
  return true;
}

}  // namespace supersonic
